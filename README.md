# zltop
This purpose of this program is to provide a lighed weighted tool to monitor the Lustre storage's performance.
It only uses ssh, lctl command, python's curses, multiprocess modules.
Lustre seems has very rough manual for it's procfs system, many of them are not clear what they really are. We can still get 
some useful performance metrics from it.
Ss
This program does not support multiple OSTs on an OSS, or multiple MDTs either. Some of the performance metrics are missing, which need more works.


Nothing is guaranteed here. Use at your own risk.
