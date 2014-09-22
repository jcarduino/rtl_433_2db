rtl_433_2db
===========

Example Python script running rtl_433 as subprocess and sends output to mysql
It asumes a working rtl_433 in the same directory.
Needs:
      mysql connector
      Python 2.7 (tested, might work on others)
      
Please use as example only and customze as you wish. It is ment as an example how to handle output, running it as subprocess.

Todo: working now on some error detection when database connection fails.
The code is based on my fork of rtl_433, so it needs customising to your devices.


