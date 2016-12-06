# encoding=utf-8
from multiprocessing.connection import Listener
from multiprocessing.connection import Client
import os
import sys
import subprocess
import signal

import platform

import psutil


def UsePlatform():
    sysstr = platform.system()
    if(sysstr == "Windows"):
        print ("Call Windows tasks")
    elif(sysstr == "Linux"):
        print ("Call Linux tasks")
    else:
        print ("Other System tasks")


def checkListener():
    pid = ''
    for proc in psutil.process_iter():
        try:
            pinfo = proc.as_dict(attrs=['pid', 'name', 'cmdline'])
        except psutil.NoSuchProcess:
            pass
        else:
            if pinfo['name'] == __file__ or \
               pinfo['cmdline'].find(__file__) >= 0
            pid = str(pinfo['pid'])
            # print(pinfo)
    return pid

UsePlatform()

'''
def havaListener(kill=False):
    p = subprocess.Popen(['ps', '-A'], stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, shell=False)
    stdout, stderr = p.communicate()
    pid = -1
    for line in stdout.splitlines():
        if 'xyscheduler.py' in line:
            pidtmp = int(line.split(None, 1)[0])
            if os.getpid() != pidtmp:
                pid = pidtmp
    if (pid != -1 and kill):
        os.kill(pid, signal.SIGKILL)
    return pid
'''


def startListener():
    address = ('localhost', 6000)     # family is deduced to be 'AF_INET'
    listener = Listener(address, authkey='secret password')

    while True:
        conn = listener.accept()
        print 'connection accepted from', listener.last_accepted
        data = conn.recv()
        try:
            if data == 'list':
                print 'list job'
                # jobs = sched.get_jobs()
                # conn.send_bytes(str(jobs))
            elif data == 'stop':
                # sched.shutdown()
                print 'stop '
                conn.send_bytes('shutdown')
            elif data == 'start':
                # sched = Scheduler(daemonic=True)
                # sched.start()
                # getJobToSche(sched)
                conn.send_bytes('start scheduler')
            elif data == 'stoplisten':
                break

        except Exception, e:
            print e
        finally:
            conn.close()
    listener.close()


def setMSGtoListener(msg):
    address = ('localhost', 6000)
    conn = Client(address, authkey='secret password')
    conn.send(msg)
    print conn.recv_bytes()
    conn.close()


def getJobToSche(sched):
    if sched.get_jobs():
        sched.unschedule_func(jobctl.runJob)
    res = jobctl.getResultBySQL(
        "SELECT jbid,year,month,day,week,dayofweek,hour,minute,second FROM jobcontrol.schecfg")
    for (jbid, year, month, day, week, dayofweek, hour, minute, second) in res:
        print jbid, year, month, day, week, dayofweek, hour, minute, second
        pjbid = jbid
        if year and year.strip():
            pyear = year
        else:
            pyear = None
        if month and month.strip():
            pmonth = month
        else:
            pmonth = None
        if day and day.strip():
            pday = day
        else:
            pday = None
        if week and week.strip():
            pweek = week
        else:
            pweek = None
        if dayofweek and dayofweek.strip():
            pdayofweek = dayofweek
        else:
            pdayofweek = None
        if hour and hour.strip():
            phour = hour
        else:
            phour = None
        if minute and minute.strip():
            pminute = minute
        else:
            pminute = None
        if second and second.strip():
            psecond = second
        else:
            psecond = None
        '''
        funnm="y%sm%sd%sw%sdow%sh%sm%ss%s"%(str(pyear),str(pmonth),str(pday),str(pweek),str(pdayofweek),
        str(phour),str(pminute),str(psecond))

        exec("%s=jobctl.runjob"%(funnm))
        '''
        sched.add_cron_job(jobctl.runJob, year=pyear, month=pmonth, day=pday, week=pweek,
                           day_of_week=pdayofweek, hour=phour, minute=pminute, second=psecond, args=[pjbid])
    # sched.shutdown()
# Start the scheduler

    # Schedule job_function to be called every two hours
#sched.add_interval_job(job_function, seconds=5)
# while True:
   # sched.print_jobs()
   # time.sleep(3)


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ['list', 'start', 'stop', 'listen', 'stoplisten']:
        print 'you need input jobcontrol list'
        print '                          start'
        print '                          stop'
        print '                          listen'
        print '                          stoplisten'
        sys.exit(0)
    try:
        if sys.argv[1] == 'list':
            setMSGtoListener('list')
        elif sys.argv[1] == 'listen':
            if havaListener() == -1:
                pid = os.fork()
                if pid == 0:  # 子进程
                    startListener()
                else:  # 父进程
                    pass
            else:
                print "listerner have already start"
        elif sys.argv[1] == 'stop':
            setMSGtoListener('stop')
        elif sys.argv[1] == 'start':
            setMSGtoListener('start')
        elif sys.argv[1] == 'stoplisten':
            havaListener(kill=True)

        '''
            while True:
                time.sleep(5)
                setMSGtoListener("list")'''
    except OSError, e:
        pass


if __name__ == '__main__':
    main()
    print havaListener()
