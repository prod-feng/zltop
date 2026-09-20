
# zltop
This purpose of this program is to provide a lighed weighted tool to monitor the Lustre storage's performance.
It only uses ssh, lctl command, python's curses, multiprocess modules.
Lustre seems has very rough manual for it's procfs system, many of them are not clear what they really are. We can still get 
some useful performance metrics from it.
Ss
This program does not support multiple OSTs on an OSS, or multiple MDTs either. Some of the performance metrics are missing, which need more works.


Nothing is guaranteed here. Use at your own risk.


Python 2.7, Centos 7, Lustre 2.12.5.

<img width="894" height="897" alt="Screenshot 2026-09-20 at 5 33 14 PM" src="https://github.com/user-attachments/assets/53addf6f-9302-408e-a14c-351a50965d71" />
