from multiprocessing import Process
import os
import time


def run_proc(name):
    while True:
        time.sleep(3)
        print 'Run child process %s (%s)...' % (name, os.getpid())

if __name__ == '__main__':
    print 'Parent process %s.' % os.getpid()
    processes = list()
    for i in range(1):
        p = Process(target=run_proc, args=('test',))
        print 'Process will start.'
        p.daemon = False
        p.start()
        processes.append(p)

    print 'wudi'
    while True:
        if raw_input() == 'q':
            break
        for p in processes:
            print str(p.pid) + str(p.is_alive())
    print 'primary process is end'
