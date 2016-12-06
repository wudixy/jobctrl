import multiprocessing
import time


def func(msg):
    for i in xrange(6):
        print msg
        time.sleep(5)
    return "done " + msg


if __name__ == "__main__":
    pool = multiprocessing.Pool(processes=4)
    result = []
    result.append(pool.apply_async(func, ('msg', )))
    # pool.close()
    #pool.join()
    while True:
        for res in result:
            print res.get()
        print "Sub-process(es) done."
        time.sleep(2)
