# -*- coding: utf-8 -*-

'''作业调度管理模块

根据配置文件，管理JCI

Notes:

1.定时调度
2.远程调度
3.将JCI扩展为远程模式
4.目前程序仅在windows测试，如迁移至linux，可能需要完善其中涉及路径的代码

Example:
  >>>
'''


# @Date    : 2016-11-24 15:35:18
# @Author  : wudi (wudi@xiyuetech.com)
# @Link    : https://github.com/wudixy/jci/tree/master/2.1
# @Version : 2.1

import os
# import sys
import json
import logging
import logging.handlers
from multiprocessing.connection import Listener
from multiprocessing.connection import Client
# from multiprocessing import Process
import threading

import psutil
from apscheduler.schedulers.background import BackgroundScheduler

from jci import JobCtlInterface


class jobCtlMan():

    LLMAP = {"DEBUG": logging.DEBUG,
             "INFO": logging.INFO,
             "WARNING": logging.WARNING,
             "ERROR": logging.ERROR}

    __SCHEDULER_PARAM_LIST = {"YEAR": None,
                              "MONTH": None,
                              "DAY": None,
                              "WEEK": None,
                              "DAY_OF_WEEK": None,
                              "HOUR": None,
                              "MINUTE": None,
                              "SECOND": None,
                              "START_DATE": None,
                              "END_DATE": None}

    def __init__(self, cfgfile='jcm.cfg'):
        # read config file
        if os.path.exists(cfgfile):
            f = open(cfgfile)
            js = f.read()
            try:
                basecfg = json.loads(js)
                self._jcmport = basecfg['PORT']
                self._ip = basecfg['IP']
                self._logleve = self.LLMAP[basecfg['LOGLEVEL']]
                if 'SCHEDULER' in basecfg.keys():
                    self._schedcfg = basecfg['SCHEDULER']
                else:
                    self._schedcfg = []
            except Exception, e:
                print str(e)
                # set default
                self._jcmport = 6000
                self._ip = "127.0.0.1"
                self._logleve = "INFO"
            finally:
                f.close()

        self.log = logging.getLogger('jcm')
        ch = logging.handlers.RotatingFileHandler('jcm.log')
        logformat = '%(asctime)s [%(levelname)s] [line:%(lineno)d] %(funcName) s.%(message) s'
        formatter = logging.Formatter(logformat)
        ch.setFormatter(formatter)
        self.log.addHandler(ch)
        self.log.setLevel(logging.DEBUG)
        self.log.datefmt = '%Y-%m-%dT%H:%M:%S'

        self._scheduler = None

        # 初始化日志设置
        # logformat = '%(asctime)s [%(levelname)s] [line:%(lineno)d] %(funcName) s.%(message) s'
        # logging.basicConfig(level=self._logleve, format=logformat,
        #                    datefmt='%Y-%m-%dT%H:%M:%S', filename='jcm.log')
        # define a Handler which writes INFO messages or higher to the
        # sys.stderr
        '''
        console = logging.StreamHandler()
        console.setLevel(self._logleve)
        # set a format which is simpler for console use
        formatter = logging.Formatter(logformat)
        # tell the handler to use this format
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)
        '''
        # self.log = logging.getLogger()

    def __del__(self):
        if self._scheduler:
            self._scheduler.shutdown()
        # pass

    def schedulerTask(self, cfg):
        # scheduler.add_job(tick, 'interval', seconds=3)
        if 'SNAME' in cfg.keys():
            if 'TASKCFG' not in cfg.keys():
                self.log.warning('task:%s taskcfg not found')
                return False
            if not os.path.exists(cfg['TASKCFG']):
                self.log.warning('task:%s taskcfg not exists')
                return False
            plist = self.__SCHEDULER_PARAM_LIST
            for key in self.__SCHEDULER_PARAM_LIST.keys():
                if key in cfg.keys():
                    if cfg[key]:
                        plist[key] = cfg[key]
            jci = JobCtlInterface()
            jci.readConfig(cfg['TASKCFG'])
            self._scheduler.add_job(jci.runTask, 'cron', id=cfg['SNAME'],
                                    args=[False, False],
                                    year=plist['YEAR'],
                                    month=plist['MONTH'],
                                    day=plist['DAY'],
                                    week=plist['WEEK'],
                                    day_of_week=plist['DAY_OF_WEEK'],
                                    hour=plist['HOUR'],
                                    minute=plist['MINUTE'],
                                    second=plist['SECOND'],
                                    start_date=plist['START_DATE'],
                                    end_date=plist['END_DATE']
                                    )
            self.log.info('task:%s add to scheduler' % (cfg['SNAME'],))
        else:
            self.log.warning('not found SNAME')

    def schedulerBycfg(self):
        for cfg in self._schedcfg:
            self.schedulerTask(cfg)

    def removeSche(self):
        for cfg in self._schedcfg:
            self._scheduler.remove_job(cfg['SNAME'])

    def checkSelfRun(self):
        pid = ''
        for proc in psutil.process_iter():
            try:
                pinfo = proc.as_dict(attrs=['pid', 'name', 'cmdline'])
            except psutil.NoSuchProcess:
                pass
            else:
                cmdl = pinfo['cmdline']
                if pinfo['name'] == __file__ and os.getpid() != pinfo['pid']:
                    pid = str(pinfo['pid'])
                    print pinfo
                if cmdl and __file__ in cmdl and os.getpid() != pinfo['pid']:
                    pid = str(pinfo['pid'])
                    print pinfo
        return pid

    def startListener(self):
        address = (self._ip, self._jcmport)     # family is deduced to be 'AF_INET'
        listener = Listener(address, authkey='secret password')
        while True:
            conn = listener.accept()
            self.log.debug('connection accepted from' + str(listener.last_accepted))
            data = conn.recv()
            try:
                if data == 'ping':
                    # jobs = sched.get_jobs()
                    conn.send_bytes('success!')
                elif data == 'shutdown':
                    # sched.shutdown()
                    conn.send_bytes('shutdown')
                    self.log.info('stop listen by %s' % (str(listener.last_accepted)))
                    conn.send_bytes('listener is stopped')
                    break
                elif data == 'startsche':
                    if self._scheduler:
                        msg = 'already running'
                    else:
                        self._scheduler = BackgroundScheduler()
                        self._scheduler.daemonic = True
                        # scheduler = BlockingScheduler(timezone='MST')
                        self.schedulerBycfg()
                        self._scheduler.start()
                        msg = 'scheduler start'
                    # sched = Scheduler(daemonic=True)
                    # sched.start()
                    # getJobToSche(sched)
                    conn.send_bytes(msg)
                elif data == 'stopsche':
                    if self._scheduler:
                        self.removeSche()
                        conn.send_bytes('remove scheduler')
                    else:
                        conn.send_bytes('scheduler not running')
                elif data == 'listsche':
                    if self._scheduler:
                        jobs = self._scheduler.get_jobs()
                        ls = []
                        for jb in jobs:
                            ls.append({'nrt': str(jb.next_run_time), 'nm': jb.id})

                        # self._scheduler.print_jobs()
                        conn.send_bytes(str(ls))
                    else:
                        conn.send_bytes('no scheduler')
                else:
                    conn.send_bytes('Unknown command')
            except Exception, e:
                self.log.error(str(e))
            finally:
                conn.close()
        listener.close()

    def listByThread(self):
        self._listener = threading.Thread(target=self.startListener, args=(), name='listen')
        self._listener.setDaemon(True)
        self._listener.start()

    def sentMsg(self, msg):
        s = ''
        address = ('localhost', self._jcmport)
        conn = Client(address, authkey='secret password')
        conn.send(msg)
        try:
            s = conn.recv_bytes()
        except Exception:
            pass
        conn.close()
        return s


CMD_LIST = ['-startlisten', '-stoplisten', '-ping', '-listsche']
DEBUG_MEMU_LIST = ['-gettaskstatus', '-getjobstatus',
                   '-resetdb', '-showconfig', '-runjob',
                   '-test', '-sendtestmail',
                   '-runtask', '-rerun', '-checkconfig']


def main():
    # 如果只有一个参数，并且参数为-help，则打印帮助
    promt = 'Please input parameter:\noption%s\nor input [quit] to exit\n' % (
            CMD_LIST, )
    promt = ' '.join(promt.split())
    jcm = jobCtlMan('jcm.cfg')

    while True:
        parmas = raw_input(promt + '\n')
        if parmas == '-startlisten':
            pid = jcm.checkSelfRun()
            if pid:
                print 'jcm listener is running,pid is %s' % (pid,)
                return
            else:
                # p = Process(target=jcm.startListener)
                # print 'Listener will start.'
                # p.daemon = True
                # p.start()
                # processes.append(p)
                jcm.listByThread()
                # jcm.startListener()
                # break
        elif parmas == '-ping':
            print jcm.sentMsg('ping')
        elif parmas == '-stoplisten':
            pid = jcm.checkSelfRun()
            if pid:
                print jcm.sentMsg('stoplisten')
            else:
                print 'listener already stopped'
        elif parmas == 'quit':
            break
        elif parmas == '-listsche':
            print jcm.sentMsg('listsche')
        else:
            print jcm.sentMsg(parmas)


if __name__ == '__main__':
    main()
