import time
import subprocess
from datetime import datetime

cmd = ['python', 'main.py', '-m', 'super']
minute = 2

while 1:
    timestr = datetime.now().strftime('%m-%d-%Y, %H:%M:%S')
    write_time_cmd = ['echo', timestr]

    subprocess.run(write_time_cmd)
    subprocess.run(cmd)
    time.sleep(1000 * 60 * minute)
