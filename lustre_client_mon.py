#!/usr/bin/env python3

import subprocess
import sys
import time
import re
import signal
import threading
import queue
import shutil
from datetime import datetime

# ============================================================
# Configuration
# ============================================================

DEFAULT_INTERVAL = 2.0
DEFAULT_WORKERS = 20
SSH_TIMEOUT = 8

# Interactive sort keys
SORT_KEYS = {
    "1": ("node",          "NODE"),
    "5": ("md_ops",        "MD OPS/s"),
    "6": ("md_read",       "MD READ/s"),
    "7": ("md_write",      "MD WRITE/s"),
    "8": ("md_lock",       "MD LOCK/s"),
    "9": ("read_iops",      "READ IOPS"),
    "0": ("write_iops",     "WRITE IOPS"),
    "a": ("read_mb",        "READ MB/s"),
    "b": ("write_mb",       "WRITE MB/s"),
}

# ============================================================
# Global state
# ============================================================

running = True
sort_key = "md_ops"
sort_reverse = True

previous = {}


# ============================================================
# Signal handling
# ============================================================

def signal_handler(sig, frame):
    global running
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ============================================================
# Formatting
# ============================================================

def fmt_num(v):
    if v is None:
        return "N/A"

    if abs(v) >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"

    if abs(v) >= 1_000:
        return f"{v / 1_000:.2f}K"

    return f"{v:.2f}"


def fmt_fixed(v):
    if v is None:
        return "N/A"
    return f"{v:.2f}"


# ============================================================
# Get node list
# ============================================================

def get_nodes():
    cmd = ["scontrol", "show", "node"]

    try:
        out = subprocess.check_output(
            cmd,
            text=True,
            stderr=subprocess.DEVNULL
        )
    except Exception as e:
        print(f"ERROR: cannot run scontrol: {e}")
        sys.exit(1)

    nodes = []

    for line in out.splitlines():
        m = re.search(r"\bNodeName=(\S+)", line)
        if m:
            name = m.group(1)

            # Expand ranges such as node[01-04] if scontrol returned them.
            if "[" in name:
                try:
                    expanded = subprocess.check_output(
                        ["scontrol", "-o", "show", "hostnames", name],
                        text=True,
                        stderr=subprocess.DEVNULL
                    )
                    nodes.extend(
                        x.strip()
                        for x in expanded.splitlines()
                        if x.strip()
                    )
                except Exception:
                    nodes.append(name)
            else:
                nodes.append(name)

    return sorted(set(nodes))


# ============================================================
# Remote lctl collection
# ============================================================

REMOTE_SCRIPT = r'''
echo "===MDSTATS==="
lctl get_param 'mdc.*.md_stats' 2>/dev/null

echo "===RPCSTATS==="
lctl get_param 'osc.*.rpc_stats' 2>/dev/null
'''


def ssh_collect(node):
    """
    Collect raw Lustre client statistics from one node.
    """

    cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=5",
        "-o", "StrictHostKeyChecking=no",
        node,
        "bash", "-s"
    ]

    try:
        p = subprocess.run(
            cmd,
            input=REMOTE_SCRIPT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=SSH_TIMEOUT
        )

        if p.returncode != 0:
            return node, None

        return node, p.stdout

    except Exception:
        return node, None


# ============================================================
# Parsing MDS stats
# ============================================================

MD_OPERATIONS = [
    "close",
    "create",
    "enqueue",
    "getattr",
    "intent_lock",
    "link",
    "rename",
    "setattr",
    "fsync",
    "read_page",
    "unlink",
    "setxattr",
    "getxattr",
    "intent_getattr_async",
    "revalidate_lock",
]


def parse_md_stats(text):
    """
    Returns aggregate md_stats counters.

    IMPORTANT:
    These are cumulative counters from all MDT connections visible
    through the client's MDCs.
    """

    counters = {x: 0 for x in MD_OPERATIONS}

    if not text:
        return counters

    in_md = False

    for line in text.splitlines():

        if line.startswith("===MDSTATS==="):
            in_md = True
            continue

        if line.startswith("===RPCSTATS==="):
            break

        if not in_md:
            continue

        m = re.match(
            r"^\s*(\S+)\s+(\d+(?:\.\d+)?)\s+samples",
            line
        )

        if not m:
            continue

        op = m.group(1)

        if op in counters:
            counters[op] += float(m.group(2))

    return counters


# ============================================================
# Parse OSC rpc stats
# ============================================================

def parse_rpc_stats(text):
    """
    Parse aggregate client OSC RPC statistics.

    read_iops/write_iops here mean completed READ/WRITE RPCs/sec
    from the client perspective.

    read_mb/write_mb are estimated from:
        pages per RPC * 4096
    """

    if not text:
        return {
            "read_rpcs": 0,
            "write_rpcs": 0,
            "read_bytes": 0,
            "write_bytes": 0,
        }

    read_rpcs = 0
    write_rpcs = 0
    read_bytes = 0
    write_bytes = 0

    current = None

    for line in text.splitlines():

        if line.startswith("===RPCSTATS==="):
            continue

        if ".rpc_stats=" in line:
            current = "rpc"
            continue

        if line.startswith("read RPCs in flight:"):
            continue

        # Detect the two RPC histogram columns.
        if line.strip().startswith("pages per rpc"):
            current = "pages"
            continue

        if line.strip().startswith("rpcs in flight"):
            current = "inflight"
            continue

        if line.strip().startswith("offset"):
            current = "offset"
            continue

        if current == "pages":
            m = re.match(
                r"^\s*(\d+):\s+(\d+)\s+\d+\s+\d+\s+\|\s+(\d+)\s+\d+\s+\d+",
                line
            )

            if m:
                pages = int(m.group(1))
                rrpcs = int(m.group(2))
                wrpcs = int(m.group(3))

                read_rpcs += rrpcs
                write_rpcs += wrpcs

                read_bytes += rrpcs * pages * 4096
                write_bytes += wrpcs * pages * 4096

    return {
        "read_rpcs": read_rpcs,
        "write_rpcs": write_rpcs,
        "read_bytes": read_bytes,
        "write_bytes": write_bytes,
    }


# ============================================================
# Convert cumulative counters -> rates
# ============================================================

def rate(old, new, elapsed):
    if old is None:
        return 0.0

    if elapsed <= 0:
        return 0.0

    delta = new - old

    # Counter reset/reconnect.
    if delta < 0:
        return 0.0

    return delta / elapsed


# ============================================================
# One node
# ============================================================

def collect_node(node):
    node, raw = ssh_collect(node)

    if raw is None:
        return {
            "node": node,
            "online": False,
        }

    md = parse_md_stats(raw)
    rpc = parse_rpc_stats(raw)

    now = time.monotonic()

    old = previous.get(node)

    result = {
        "node": node,
        "online": True,
    }

    # --------------------------------------------------------
    # Metadata
    # --------------------------------------------------------

    md_ops = sum(md.values())

    # "MD READ" and "MD WRITE" are deliberately explicit
    # classifications rather than pretending Lustre md_stats
    # contains an open/read/write syscall breakdown.
    #
    # Read-like metadata operations:
    #   getattr, read_page, getxattr,
    #   intent_getattr_async, revalidate_lock
    #
    # Write-like metadata operations:
    #   close, create, link, rename, setattr,
    #   fsync, unlink, setxattr
    #
    # intent_lock/enqueue are kept out of these two categories.
    md_read = (
        md["getattr"] +
        md["read_page"] +
        md["getxattr"] +
        md["intent_getattr_async"] +
        md["revalidate_lock"]
    )

    md_write = (
        md["close"] +
        md["create"] +
        md["link"] +
        md["rename"] +
        md["setattr"] +
        md["fsync"] +
        md["unlink"] +
        md["setxattr"]
    )

    md_lock = (
        md["intent_lock"] +
        md["enqueue"]
    )

    # --------------------------------------------------------
    # Store cumulative counters
    # --------------------------------------------------------

    cumulative = {
        "md_ops": md_ops,
        "md_read": md_read,
        "md_write": md_write,
        "md_lock": md_lock,
        "read_rpcs": rpc["read_rpcs"],
        "write_rpcs": rpc["write_rpcs"],
        "read_bytes": rpc["read_bytes"],
        "write_bytes": rpc["write_bytes"],
    }

    # First sample.
    if old is None:
        previous[node] = {
            "time": now,
            "data": cumulative,
        }

        result.update({
            "md_ops": 0.0,
            "md_read": 0.0,
            "md_write": 0.0,
            "md_lock": 0.0,
            "read_iops": 0.0,
            "write_iops": 0.0,
            "read_mb": 0.0,
            "write_mb": 0.0,
        })

        return result

    elapsed = now - old["time"]

    if elapsed <= 0:
        elapsed = DEFAULT_INTERVAL

    result["md_ops"] = rate(
        old["data"]["md_ops"],
        cumulative["md_ops"],
        elapsed
    )

    result["md_read"] = rate(
        old["data"]["md_read"],
        cumulative["md_read"],
        elapsed
    )

    result["md_write"] = rate(
        old["data"]["md_write"],
        cumulative["md_write"],
        elapsed
    )

    result["md_lock"] = rate(
        old["data"]["md_lock"],
        cumulative["md_lock"],
        elapsed
    )

    result["read_iops"] = rate(
        old["data"]["read_rpcs"],
        cumulative["read_rpcs"],
        elapsed
    )

    result["write_iops"] = rate(
        old["data"]["write_rpcs"],
        cumulative["write_rpcs"],
        elapsed
    )

    result["read_mb"] = rate(
        old["data"]["read_bytes"],
        cumulative["read_bytes"],
        elapsed
    ) / 1024 / 1024

    result["write_mb"] = rate(
        old["data"]["write_bytes"],
        cumulative["write_bytes"],
        elapsed
    ) / 1024 / 1024

    previous[node] = {
        "time": now,
        "data": cumulative,
    }

    return result


# ============================================================
# Parallel collection
# ============================================================

def collect_all(nodes, workers):
    results = []

    q = queue.Queue()

    for node in nodes:
        q.put(node)

    lock = threading.Lock()

    def worker():
        while True:
            try:
                node = q.get_nowait()
            except queue.Empty:
                return

            try:
                result = collect_node(node)

                with lock:
                    results.append(result)

            finally:
                q.task_done()

    threads = []

    nworkers = min(workers, max(1, len(nodes)))

    for _ in range(nworkers):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        threads.append(t)

    q.join()

    return results


# ============================================================
# TOTAL
# ============================================================

def make_total(results):
    total = {
        "node": "TOTAL",
        "online": True,
    }

    fields = [
        "md_ops",
        "md_read",
        "md_write",
        "md_lock",
        "read_iops",
        "write_iops",
        "read_mb",
        "write_mb",
    ]

    for field in fields:
        total[field] = sum(
            r.get(field, 0.0)
            for r in results
            if r.get("online", False)
        )

    return total


# ============================================================
# Display
# ============================================================

def clear_screen():
    # ANSI clear + home cursor.
    print("\033[2J\033[H", end="")


def print_table(results, interval):

    clear_screen()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    sort_name = SORT_KEYS.get(
        next(
            (k for k, v in SORT_KEYS.items()
             if v[0] == sort_key),
            "?"
        ),
        ("", sort_key)
    )[1]

    print(
        f"LUSTRE CLUSTER TOP                              {now}"
    )

    print("=" * 128)

    print(
        f"Refresh: {interval:.1f}s    "
        f"Sort: {sort_name} "
        f"({'DESC' if sort_reverse else 'ASC'})"
    )

    print(
        "Keys: 5=MD OPS  6=MD READ  7=MD WRITE  8=MD LOCK  "
        "9=READ IOPS  0=WRITE IOPS  a=READ MB/s  b=WRITE MB/s  "
        "1=NODE  r=reverse"
    )

    print("=" * 128)

    header = (
        f"{'NODE':<24}"
        f"{'MD OPS/s':>12}"
        f"{'MD READ/s':>12}"
        f"{'MD WRITE/s':>13}"
        f"{'MD LOCK/s':>12}"
        f"{'READ IOPS':>12}"
        f"{'WRITE IOPS':>13}"
        f"{'READ MB/s':>12}"
        f"{'WRITE MB/s':>13}"
    )

    print(header)
    print("-" * 128)

    # --------------------------------------------------------
    # Sort ONLY HERE.
    #
    # Collection has already completed.
    # Therefore sorting cannot affect MDS collection.
    # --------------------------------------------------------

    def sort_value(r):
        if sort_key == "node":
            return r.get("node", "")

        value = r.get(sort_key, 0.0)

        if value is None:
            return -1

        return value

    sorted_results = sorted(
        results,
        key=sort_value,
        reverse=sort_reverse
    )

    # --------------------------------------------------------
    # Total is calculated independently and is always last.
    # --------------------------------------------------------

    total = make_total(results)

    for r in sorted_results:

        if not r.get("online", False):
            print(
                f"{r['node']:<24}"
                f"{'OFFLINE':>12}"
            )
            continue

        print(
            f"{r['node']:<24}"
            f"{fmt_num(r['md_ops']):>12}"
            f"{fmt_num(r['md_read']):>12}"
            f"{fmt_num(r['md_write']):>13}"
            f"{fmt_num(r['md_lock']):>12}"
            f"{fmt_num(r['read_iops']):>12}"
            f"{fmt_num(r['write_iops']):>13}"
            f"{fmt_num(r['read_mb']):>12}"
            f"{fmt_num(r['write_mb']):>13}"
        )

    print("-" * 128)

    print(
        f"{'TOTAL':<24}"
        f"{fmt_num(total['md_ops']):>12}"
        f"{fmt_num(total['md_read']):>12}"
        f"{fmt_num(total['md_write']):>13}"
        f"{fmt_num(total['md_lock']):>12}"
        f"{fmt_num(total['read_iops']):>12}"
        f"{fmt_num(total['write_iops']):>13}"
        f"{fmt_num(total['read_mb']):>12}"
        f"{fmt_num(total['write_mb']):>13}"
    )

    print("=" * 128)

    print(
        "q/Ctrl-C: exit | "
        "5 MD OPS | 6 MD READ | 7 MD WRITE | 8 MD LOCK | "
        "9 READ IOPS | 0 WRITE IOPS | a READ MB/s | b WRITE MB/s | "
        "1 NODE | r reverse"
    )


# ============================================================
# Keyboard input
# ============================================================

def keyboard_thread():
    global running
    global sort_key
    global sort_reverse

    # Put terminal into character-at-a-time mode.
    if not sys.stdin.isatty():
        return

    old_settings = None

    try:
        import termios
        import tty

        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)

        tty.setcbreak(fd)

        while running:
            ch = sys.stdin.read(1)

            if not ch:
                continue

            if ch in ("q", "Q"):
                running = False
                break

            if ch == "r":
                sort_reverse = not sort_reverse
                continue

            if ch in SORT_KEYS:
                sort_key = SORT_KEYS[ch][0]

                # Node name is naturally ascending.
                if sort_key == "node":
                    sort_reverse = False
                else:
                    sort_reverse = True

    except Exception:
        pass

    finally:
        if old_settings is not None:
            try:
                termios.tcsetattr(
                    fd,
                    termios.TCSADRAIN,
                    old_settings
                )
            except Exception:
                pass


# ============================================================
# Main
# ============================================================

def main():

    global running

    interval = DEFAULT_INTERVAL
    workers = DEFAULT_WORKERS

    if len(sys.argv) >= 2:
        try:
            interval = float(sys.argv[1])
            if interval < 0.5:
                interval = 0.5
        except ValueError:
            pass

    if len(sys.argv) >= 3:
        try:
            workers = int(sys.argv[2])
            workers = max(1, workers)
        except ValueError:
            pass

    nodes = get_nodes()

    if not nodes:
        print("No Slurm nodes found.")
        sys.exit(1)

    print(f"Found {len(nodes)} nodes.")
    print(f"Workers: {workers}")
    print("Collecting initial counters...")

    # --------------------------------------------------------
    # First sample establishes the cumulative-counter baseline.
    # --------------------------------------------------------

    collect_all(nodes, workers)

    time.sleep(interval)

    # --------------------------------------------------------
    # Keyboard listener.
    # --------------------------------------------------------

    kt = threading.Thread(
        target=keyboard_thread,
        daemon=True
    )

    kt.start()

    # --------------------------------------------------------
    # Main monitoring loop.
    # --------------------------------------------------------

    while running:

        cycle_start = time.monotonic()

        results = collect_all(nodes, workers)

        if not running:
            break

        print_table(results, interval)

        elapsed = time.monotonic() - cycle_start

        sleep_time = max(0.05, interval - elapsed)

        end = time.monotonic() + sleep_time

        while running and time.monotonic() < end:
            time.sleep(0.05)

    # --------------------------------------------------------
    # Clean exit.
    # --------------------------------------------------------

    clear_screen()
    print("Lustre cluster top exited.")


if __name__ == "__main__":
    main()
