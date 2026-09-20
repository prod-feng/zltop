
# zltop
This purpose of this program is to provide a lighed weighted tool to monitor the Lustre storage's performance.
It only uses ssh, lctl command, python's curses, multiprocess modules.
Lustre seems has very rough manual for it's procfs system, many of them are not clear what they really are. We can still get 
some useful performance metrics from it.


```
This program does not support multiple OSTs on an OSS, or multiple MDTs either. Some of the performance metrics are missing, which need more works.


Nothing is guaranteed here. Use at your own risk.


Python 2.7, Centos 7, Lustre 2.12.5.

This program collects Lustre performance information using:

    ssh
    /usr/sbin/lctl get_param

It is designed around Lustre output such as:

OSS:

obdfilter.cls04010-OST0000.kbytesavail=116865851864
obdfilter.cls04010-OST0000.kbytesfree=120802806588
obdfilter.cls04010-OST0000.kbytestotal=390365606128

obdfilter.cls04010-OST0000.filesfree=298919263
obdfilter.cls04010-OST0000.filestotal=384429952

obdfilter.cls04010-OST0000.stats=
snapshot_time             1789936196.719749479 secs.nsecs
read_bytes                2741015563 samples [bytes] 1 4194304 2212208235646983
write_bytes               865416709 samples [bytes] 1 4194304 1956767105813338
getattr                   641716717 samples [reqs]
setattr                   987954196 samples [reqs]
punch                     24337679 samples [reqs]
sync                      331147012 samples [reqs]
destroy                   368805874 samples [reqs]
create                    78604 samples [reqs]
statfs                    1859226 samples [reqs]
get_info                  3 samples [reqs]
set_info                  14394 samples [reqs]
quotactl                  18254 samples [reqs]

MDT:


osd-ldiskfs.cls04010-MDT0000.kbytesavail=3910410976
osd-ldiskfs.cls04010-MDT0000.kbytesfree=3995811364
osd-ldiskfs.cls04010-MDT0000.kbytestotal=4231249880

osd-ldiskfs.cls04010-MDT0000.filesfree=4028998808
osd-ldiskfs.cls04010-MDT0000.filestotal=4270030848

mdt.cls04010-MDT0000.md_stats=
snapshot_time             1789936384.884839506 secs.nsecs
open                      6342332055 samples [reqs] 1 1 6342332055
close                     5592079279 samples [reqs] 1 1 5592079279
mknod                     743404900 samples [reqs]
link                      7170912 samples [reqs]
unlink                    132112140 samples [reqs]
mkdir                     12904897 samples [reqs]
rmdir                     7378217 samples [reqs]
rename                    20683631 samples [reqs]
getattr                   9450681049 samples [reqs]
setattr                   1518871255 samples [reqs]
getxattr                  818091495 samples [reqs]
setxattr                  358381 samples [reqs]
statfs                    3301329 samples [reqs]
sync                      667112089 samples [reqs]
samedir_rename            17838741 samples [reqs]
crossdir_rename           2844890 samples [reqs]

Keys:

    q       Quit
    r       Refresh immediately

Command examples:

    python2.7 lustre_monitor.py

    python2.7 lustre_monitor.py \
        --oss ookami04 \
        --oss ookami05 \
        --mds ookami02

    python2.7 lustre_monitor.py --once

    python2.7 lustre_monitor.py \
        --oss ookami04 \
        --mds ookami02 \
        --once

```


<img width="894" height="897" alt="Screenshot 2026-09-20 at 5 33 14 PM" src="https://github.com/user-attachments/assets/53addf6f-9302-408e-a14c-351a50965d71" />
