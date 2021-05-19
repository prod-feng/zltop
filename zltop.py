#!/usr/bin/python
'''
This purpose of this program is to provide a lighed weighted tool to monitor the Lustre storage's performance.
It only uses ssh, lctl command, python's curses, multiprocess modules.
Lustre seems has very rough manual for it's procfs system, many of them are not clear what they really are. We can still get 
some useful performance metrics from it.

This program does not support multiple OSTs on an OSS, or multiple MDTs either. Some of the performance metrics are missing, which need more works.


Nothing is guaranteed here. Use at your own risk.

Feel free to use it and modify it.

---05/19/2021
'''

import pprint
import subprocess
from multiprocessing import Process, Manager
import time
import copy 
debug=1
#
# Change the MDT and OST servers info here to run
# Make sure these servers can be ssh accessed via key(not password).
# No need to be root to run this program.
#
ost_hosts=['oss1', 'oss2']
mdt_hosts= ['mds1']
# Define the MDT and OST info we need.
ost_param=['obdfilter.*-OST*.kbyte*', 'obdfilter.*-OST*.file*', 'obdfilter.*-OST*.stats']
mdt_param=['osd*.*MDT*.kbyte*', 'osd*.*MDT*.files*', 'mdt.*MDT*.md_stats']

#for MDT
mdtdata={}
mdtdata_prev={}

#mdtrates={}

#for OST
mydata={}
mydata_prev={}

myrates={}
myrates_prev={}
rwrates={}

#uuu= {'test': 0}

myquit=0


import curses

import threading

def plot_sum(w,ftot,nfus,fuse,ffre,stot,nsus,suse,sfre):

#   
 
    # plot the sumarry info of the storage
    w.addstr(0, 0, "Storage system status:")
    w.addstr(1, 0, "{:>11} {:>12} {:>6}  {:>11} {:>5} {:>7} {:>11} {:>5}".format('Inodes:',ftot,'total', nfus, 'used', fuse, ffre, 'free'))
    w.addstr(2, 0, "{:>11} {:>12} {:>6}  {:>11} {:>5} {:>7} {:>11} {:>5}".format('Space:', stot,'total', nsus, 'used', suse, sfre, 'free'))

def show(w):
#
# start the keyboard mornitoring thread first
  mon=threading.Thread(target=keypress)
  mon.start()

# Now start the whole process
  while True:
#
#   Check if "q" key is pressed. If so, terminate the program now.
    if (myquit>0): 
       mon.join()
       break
#
#   start to process the OST/MDT info
    data_process()

    ftot = str(format(float(mysum['filestotal'])/(1000*1000),'.2f')) +'M'
    ffre = str(format(float(mysum['filesfree'])/(1000*1000),'.2f')) +'M'

    nfus = (float(mysum['filestotal']) - float(mysum['filesfree']))
    fuse = '('+str(format(100.*(float(nfus)/float(mysum['filestotal'])) , '2.1f') ) + '%)'
    nfus = str(format(float(nfus/(1000*1000)),'.2f')) +'M'

    stot = str(format(float(mysum['kbytestotal'])/(1024*1024*1024),'.2f')) +'T'
    sfre = str(format(float(mysum['kbytesfree']) /(1024*1024*1024),'.2f')) +'T'

    nsus = (float(mysum['kbytestotal']) - float(mysum['kbytesfree']))
    suse = '('+str(format(100.*(float(nsus))/float(mysum['kbytestotal']) , '2.1f') ) + '%)'
    nsus = str(format(float(nsus/(1024*1024*1024)),'.2f')) +'T'
#   plot basic info
    plot_sum(w,ftot,nfus,fuse,ffre,stot,nsus,suse,sfre)
 
    myratesum={}
    mdtrates={}
    mdtval={} #list to store the iops info from MDT, easy to handle in coding
    for host in mdt_hosts:
        mdtrates[host]= dict(zip(mdt_dataps,mdt_dataps_init))
    mdtval={}
        
    for host in mdt_hosts:
        mdtrates[host].pop('snapshot_time')

    iops={}
    iops_sum=0
    for host in mdt_hosts:
       iops[host]=0
       for (mdata, value) in mdtrates[host].items():
           tdif =float(mdtdata[host]['snapshot_time']) - float(mdtdata_prev[host]['snapshot_time'])
           if (tdif <= 0):
               tdif = 1
           mdtrates[host][mdata] = ((float(mdtdata[host][mdata]) - float(mdtdata_prev[host][mdata]))/tdif)
           iops[host] = iops[host] + mdtrates[host][mdata]
       iops_sum = iops_sum + iops[host]



    iops_sum = int(iops_sum +0.5)

    for (mdata, value) in mdtrates[host].items():
      mdtval[mdata] = 0
      for host in mdt_hosts:
         mdtval[mdata] = mdtval[mdata] + mdtrates[host][mdata]
      mdtval[mdata] = int( mdtval[mdata] +0.5)


    for host in ost_hosts:
        rwrates[host]= dict(zip(ost_dataps,ost_dataps_init))


    for odata in ['read_bytes', 'write_bytes']:
        myratesum[odata]=0.0
        for host in ost_hosts:
           tdif =float(myrates[host]['snapshot_time']) - float(myrates_prev[host]['snapshot_time'])
           if (tdif <= 0):
               tdif = 1
           rwrates[host][odata] = ((float(myrates[host][odata]) - float(myrates_prev[host][odata]))/tdif)
           myratesum[odata]=myratesum[odata] + rwrates[host][odata]


    read =  str(format(float(myratesum['read_bytes'])/(1024*1024), '.3f')) + 'M'  #MB/s
    write = str(format(float(myratesum['write_bytes'])/(1024*1024), '.3f')) + 'M'


    w.addstr(3, 0, "{:>11} {:>12} {:>6} {:>12} {:>5} {:>12} {:>6}".format('Bytes/s:',read, 'read',write, 'write',format(int(iops_sum)), 'IOPS'))
#
# MDT md_stats info
    w.addstr(4, 0, "{:>11} {:>7} {:>7} {:>7} {:>7} {:>8} {:>7} {:>7} {:>7}".format('Mops/s:',mdtval['open'],'open', mdtval['close'],'close',mdtval['getattr'], 'getattr',mdtval['setattr'], 'setattr'))
    w.addstr(5, 0, "{:>19} {:>7} {:>7} {:>7}  {:>7} {:>7} {:>7} {:>7}".format(mdtval['link'],'link',mdtval['unlink'], 'unlink',mdtval['mkdir'], 'mkdir',mdtval['rmdir'], 'rmdir'))
    w.addstr(6, 0, "{:>19} {:>7} {:>7} {:>7}  {:>7} {:>7}".format(mdtval['statfs'],'statfs',mdtval['rename'], 'rename', mdtval['getxattr'],'getxattr'))
#
    args=                     "{:>3}  {:>10} {:>3}  {:>3}  {:>9}  {:>9}    {:>6}   {:>6} {:>6}  {:>6}  {:>3}  {:>3}    {:>3}"
    w.addstr(7, 0, args.format('OST', 'OSS', 'Exp', 'CR', 'rMBs', 'wMBs', 'IOPS', 'LOCK', 'LGR', 'LCR', 'cpu', 'mem', 'spc'))
 

    ih=8
    for host in ost_hosts:
       w.addstr(ih, 0, args.format('', host,   ' ' ,  'cr ', 
                                                    str(format(float(rwrates[host]['read_bytes'])/(1024*1024), '5.3f')), 
                                                    str(format(float(rwrates[host]['write_bytes'])/(1024*1024), '5.3f')),
                                                                                   ' ',   '   ',  '  ',  '  ',  '  ',  '  ',  '  '))
       ih = ih + 1

    w.addstr(30,0,"\npress 'q' to exit...")

    w.refresh()

#   update the old params
    for (key, value) in mdtdata.items():
       mdtdata_prev[key] =mdtdata[key]
    for (key, value) in mydata.items():
       mydata_prev[key] =mydata[key]
    for (key, value) in myrates.items():
       myrates_prev[key] =myrates[key]
    time.sleep(3)

# parallel process to monitor the keyboard, so we can exit the program
def keypress():
    global myquit

    stdscr = curses.initscr()
    curses.cbreak()
    stdscr.keypad(1)

    key = ''
    while key != ord('q'):
      key = stdscr.getch()
      time.sleep(0.01)
    myquit=1
    curses.endwin()


# the info we now want for the collected MDT server
mdt_data= [
 'filesfree',
 'filestotal',
 'kbytesavail',
 'kbytestotal'
]

mdt_data_init = [

'0',
'0',
'0',
'0'
]

# the info we now want for the collected OST servers
ost_data= [
 'filesfree',
 'filestotal',
 'kbytesavail',
 'kbytestotal',
 'kbytesfree',
 'snapshot_time',
 'write_bytes',
 'read_bytes'
]

ost_data_init = [

'0',
'0',
'0',
'0',
'0',
'0',
'0',
'0'
]

# the index of the splited ost stats info. The order is critical here.
ost_data_format = [

'0',
'0',
'0',
'0',
'0',
'0',
'-1', #last splited item
'-1'  #
]

# data structure for processing info like read/s, iops/s, etc
ost_dataps= [
 'snapshot_time',
 'write_bytes',
 'read_bytes'
]

ost_dataps_init = [

'0',
'0',
'0'
]

ost_dataps_format = [
'0',
'-1', #last splited item
'-1'  #
]

# MDT md_stats
mdt_dataps=[
'snapshot_time',#             1621376949.986194443 secs.nsecs
'open',#  0                     14180345 samples [reqs]
'close',# 1                     10131059 samples [reqs] 1 1 10131059
'mknod',# 2                     4194246 samples [reqs] 1 1 4194246
'link',#  3                     132483 samples [reqs]
'unlink',#4                     1833846 samples [reqs]
'mkdir',# 5                     353169 samples [reqs]
'rmdir',# 6                     184489 samples [reqs]
'rename',#7                     4026210 samples [reqs]
'getattr',#8                    27264206 samples [reqs]
'setattr',#9                    16016986 samples [reqs]
'getxattr',#10                   1661148 samples [reqs]
'setxattr',#11                   47579 samples [reqs]
'statfs',#  12                  1087815 samples [reqs]
'sync',#    13                   1879 samples [reqs]
'samedir_rename',# 14            4020755 samples [reqs]
'crossdir_rename'# 15           5455 samples [reqs]
]


mdt_dataps_init = [
'0',
'0',
'0',
'0',
'0',
'0',
'0',
'0',
'0',
'0',
'0',
'0',
'0',
'0',
'0',
'0',
'0'
]

mysum= dict(zip(ost_data,ost_data_init))
ostformat =dict(zip(ost_data,ost_data_format))


def my_split(sep, sz):
    return lambda s: s.strip().split(sep, sz)

def get_mdt_info(output):
    """
    Now the info need to be process for MDT is the sam OST. We may need it in the future.
    """
    size=2 # split the output to size+1 items of each line. We only need the first two items for key and value 

    raw_data = map(lambda s: s.strip().split(None, size), output[1:])
    if(debug > 10) : print(raw_data)
    dict1={}
    ret1 = [dict1.update(dict({r[0]: r[1]})) for r in raw_data]

    return dict1

def get_ost_info(output, role):
    """
    Function to process the info fetched from OST( and MDT now) servers.
    """
    # Process the first 5 parameters from OST and MDT.
    # The order of the Popen args, ost_param and mdt param is very important here. Make sure kbyte* and files are in the first two.
    #5 params of *.*.kbyte*', '*.*.files*',
    #MDT: mdt.*MDT*.md_stats
    #OST: obdfilter.*-OST*.stats
    proc_ost = my_split('=', 1)
    raw_data1 = map(proc_ost,  output[:5])
    if(debug > 10) : print(raw_data1)
    dict1={}
    ret1 = [ dict1.update(dict({r[0].split('.')[2]: r[1]})) for r in raw_data1 ]
    if(debug > 10) : print(dict1)

    # Now to process the stats info of MDT and OST
    ind2=1
    if (role == 'MDT'): ind2=10
    proc_ost = my_split(None, ind2)
    raw_data2 = map(proc_ost,  output[6:])
    if(debug > 10) : print('PPP', raw_data2)
    ret2 = [dict1.update(dict({r[0]: r[1]})) for r in raw_data2]

    return dict1

def subpcmd(host,output, ost_param):
    # Prepare to open a subprocess to fetch the MDT and OST info.
    param=[arg for arg in ost_param]
    cmd=['ssh', host, '/usr/sbin/lctl', 'get_param'] + param
    output[host]=subprocess.Popen(cmd, stdout=subprocess.PIPE).stdout.readlines()


def request(output):
#
# Initialize subprocess dict
      sub_p={}
# Now process to get MDT info
      for host in mdt_hosts:
        sub_p[host] = Process(target=subpcmd, args=(host,output,mdt_param))
        sub_p[host].start()

# Now process to get OST info
      for host in ost_hosts:
        sub_p[host] = Process(target=subpcmd, args=(host,output,ost_param))
        sub_p[host].start()

# Wait untill all processes done
      for host in mdt_hosts+ost_hosts:
        sub_p[host].join()

#
def data_process():
  with Manager() as manager:
# Initiliaze output dict, which is a global shared space, can excange between parent and children processes.
#
    if( True ):
      output=manager.dict()

      request(output)
#
      for host in mdt_hosts:
        mdtdata[host] = get_ost_info(output[host], 'MDT')


      for host in ost_hosts:
       #process the data returned back from OST servers
         mydata[host]=get_ost_info(output[host], 'OST')

#     sum the info from all OSTs.
      for odata in ost_data:
          mysum[odata] = str(float(0))
#
      for odata in ost_data:
        for host in ost_hosts:
#          if(debug > 0): print(odata, '   ', mydata[host][odata], ' :: ', ostformat[odata])
          mysum[odata] = str(float(mysum[odata])+ float(mydata[host][odata].split()[int(ostformat[odata])]))


      # Initialize the summary dict
      for host in ost_hosts:
          myrates[host]= dict(zip(ost_dataps,ost_dataps_init))
      ostpsformat =dict(zip(ost_dataps,ost_dataps_format))

#     get the rates info from all OSTs.
      for odata in ost_dataps:
        for host in ost_hosts:
#           if(debug > 0): print(odata, '   ', mydata[host][odata], ' :: ', ostpsformat[odata])
           myrates[host][odata] = str(float(mydata[host][odata].split()[int(ostformat[odata])]))

if __name__ == "__main__":
#
#     Run first round to initialize all the info
      data_process()
#
      mdtdata_prev= {key: value for key, value in mdtdata.items()}
      mydata_prev= {key: value for key, value in mydata.items()}
      myrates_prev = {key: value for key, value in myrates.items()}
#     Start the main process now
      curses.wrapper(show)

