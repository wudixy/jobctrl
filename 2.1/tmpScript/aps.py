
from datetime import datetime

import time

import os

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler


def tick():

    print('Tick! The time is: %s' % datetime.now())


if __name__ == '__main__':

    scheduler = BackgroundScheduler()

    # scheduler = BlockingScheduler(timezone='MST')

    # scheduler.add_job(tick, 'interval', seconds=3)
    scheduler.add_job(tick, 'cron', id='my_cron_job2', second='*/5')

    scheduler.start()

    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:

        # This is here to simulate application activity (which keeps the main
        # thread alive).

        while True:

            time.sleep(2)

    except (KeyboardInterrupt, SystemExit):

        # Not strictly necessary if daemonic mode is enabled but should be done
        # if possible

        scheduler.shutdown()
