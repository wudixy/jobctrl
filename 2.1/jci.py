# -*- coding: utf-8 -*-

'''作业调度模块

根据配置文件(JSON格式)，并行调度作业执行

Notes:

1.需要安装ORACLE客户端
2.目前支持的作业类型为
    a. Attunity Replicate Task
    b. ORACLE STORE PROCEDURE
    c. ORACLE SELECT SQL STATEMEN
    d. SHELL OR BIN
3.程序所在目录不能包含中文
4.目前程序仅在windows测试，如迁移至linux，可能需要完善其中涉及路径的代码

Example:
  >>> jobcontrol [configfile] -runtask
'''


# @Date    : 2016-11-07 23:35:18
# @Author  : wudi (wudi@xiyuetech.com)
# @Link    : https://github.com/wudixy/jci/tree/master/2.1
# @Version : 2.1

# base module
import json
import os
import smtplib
from email.mime.text import MIMEText
import logging
import logging.handlers
import time
import subprocess
import sqlite3
import threading
import sys
import string
import uuid
# import pdb

# Third party module
import cx_Oracle


# MAPPING OF WAIT TYPE JOB
WAITSTATUS = {"WAITING": 0, "FINDFLAG": 1, "TIMEOUT": 2}

TASKSTATUS = {"ALLFINISH": 0, "HAVEERROR": 1}


class RunJob:
    '''运行指定类型作业模块'''
    JOBTYPE = {'WAIT_DB_FLAG': 'WAIT FLAG BY SQL',
               'SELECTDBSQL': 'EXECUTE SELECT SQL STATEMENT',
               'RUN_AR_TASK': 'RUN ATTUNITY REPLICATE TASK',
               'GET_AR_TASK_STATUS': ' GET ATTUNITY REPLICATE TASK STATUS',
               'WAIT_AR_TASK':
               'WAIT ATTUNITY REPLICATE TASK STOPPED AND FULL LOAD COMPLETE',
               'DB_PROC': 'EXECUTE DATABASE STORE PROCEDURE',
               'SHELL': 'EXECUTE SHELL OR BIN FILE',
               'GET_JOB_STATE': 'GET JOB RUNNING STATUS AND RETURN MSG'}

    # Logleve mapping
    LOGLEVE = {"DEBUG": logging.DEBUG,
               "INFO": logging.INFO,
               "WARNING": logging.WARNING,
               "ERROR": logging.ERROR}

    # ATTUNITY REPLICATE COMMAND SUCCEEDED STRING
    AR_SUCESS_TEXT = 'Succeeded'

    def __init__(self, logfile='runjob.log'):
        pass

    def __del__(self):
        pass

    def __formatARTime(self, tmstr):
        a = str(tmstr)
        a = a[:len(a) - 6]
        a = string.atol(a)
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(a))

    def initLog(self, logname, level):
        """INIT LOG SETTING,初始化日志设置"""
        self.log = logging.getLogger('jci')
        ch = logging.handlers.RotatingFileHandler(logname)
        logformat = '%(asctime)s [%(levelname)s] [line:%(lineno)d] %(funcName) s.%(message) s'
        formatter = logging.Formatter(logformat)
        ch.setFormatter(formatter)
        self.log.addHandler(ch)
        self.log.setLevel(level)
        self.log.datefmt = '%Y-%m-%dT%H:%M:%S'

        '''
        logformat = '%(asctime)s [%(levelname)s] [line:%(lineno)d] %(funcName) s.%(message) s'
        logging.basicConfig(level=level, format=logformat,
                            datefmt='%Y-%m-%dT%H:%M:%S', filename=logname)
        if showConsole:
            console = logging.StreamHandler()
            console.setLevel(level)
            # set a format which is simpler for console use
            formatter = logging.Formatter(logformat)
            # tell the handler to use this format
            console.setFormatter(formatter)
            logging.getLogger('jci').addHandler(console)
        self.log = logging.getLogger('jci')
        # return logging.getLogger()
        '''

    def getTime(self):
        """get now time by yyyy-mm-dd hh:mm:ss format"""
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    def send_mail(self, mail_user, mail_pass, mail_host, to_list, sub, content):
        '''sent mail ,if sent success ,return True'''
        content = content + '\n' + 'make mail time:' + self.getTime()
        # me="hello"+"<"+mail_user+"@"+mail_postfix+">"
        me = mail_user
        msg = MIMEText(content, _subtype='plain', _charset='gb2312')
        msg['Subject'] = sub
        msg['From'] = me
        msg['To'] = ";".join(to_list)
        try:
            server = smtplib.SMTP()
            server.connect(mail_host)
            server.login(mail_user, mail_pass)
            server.sendmail(me, to_list, msg.as_string())
            server.close()
            return True
        except Exception, e:
            tp = sys.getfilesystemencoding()
            self.log.error(str(e).decode('utf-8').encode(tp))
            return False

    def runAttunityTask(self, pdir, ddir, taskname, operation, flags):
        """ start Attunity Replicate TASK"""
        try:
            CDCPRO = '"%s\\repctl" -d "%s"' % (pdir, ddir)
            p = subprocess.Popen(CDCPRO, stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE)
            startar = 'connect\nexecute task=%s operation=%d flags=%d\nquit\n'
            p.stdin.write(startar % (taskname, operation, flags))
            p.stdin.flush()
            # p.stdout.flush()
            r = p.stdout.read()
            # 因为同时提交了三个命令，最后返回成功可能仅仅是quit成功
            # 需要检查其中是否有命令失败
            if r.find('failed') >= 0:
                rcd = 'failed'
                self.log.error(r)
            else:
                rcd = r.rstrip()[-9:]
            return {'st': 'normal', 'rcd': rcd}
            # p.wait()
        except Exception, e:
            self.log.error(str(e))
            return {'st': 'exception', 'rcd': ''}

    def getReplicateTaskStatus(self, pdir, ddir, taskname):
        """get Attunity Replicate Task Status"""
        r = {'state': 'OTHER',
             'full_load_completed': 'false',
             'start_time': '1970-01-01 00:00:00',
             'stop_time': '1970-01-01 00:00:00'}

        # Get ATTUNITY REPLICATE TASK status command
        GETTASKSTATUSCMD = 'connect\ngettaskstatus %s\nquit\n'
        try:
            CDCPRO = '"%s\\repctl" -d "%s"' % (pdir, ddir)
            p = subprocess.Popen(CDCPRO, stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE)
            p.stdin.write(GETTASKSTATUSCMD % (taskname))
            b = ''
            start = False
            p.stdin.flush()

            # 去掉AR返回值中的头部和尾部，只保留JSON部分
            while 1:
                si = p.stdout.readline()
                if not si:
                    break
                if (si[0] == '{'):
                    start = True
                if start:
                    b = b + si
                    if (si[0] == '}'):
                        break
            xx = json.loads(b)

            r = {'state': xx['task_status']["state"],
                 "full_load_completed": str(xx['task_status']['full_load_completed']),
                 'start_time': self.__formatARTime(xx['task_status']['start_time'])
                 }
            # task正在运行时不会有stop_time这个key
            # 'stop_time': self.__formatARTime(xx['task_status']['stop_time']),
        except Exception, e:
            self.log.error(str(e))
        return r

    def waitAttunityTask(self, pdir, ddir, taskname, waitsec, timeoutcnt):
        """wait Attunity Replicate task FINISH,
        return wait status[find or timeout]"""
        status = WAITSTATUS['TIMEOUT']  # 0 waiting,1 findflag,2 timeout
        for i in range(timeoutcnt):
            st = self.getReplicateTaskStatus(pdir, ddir, taskname)
            if st['state'] == 'STOPPED' or st['state'] == 'OTHER':
                break
            time.sleep(waitsec)
        if st['state'] == 'STOPPED' and \
           str(st['full_load_completed']) == 'True':
            status = WAITSTATUS['FINDFLAG']
        return {'st': 'normal', 'rcd': status}

    def select_Oracle_sql(self, tns, use, pwd, execute):
        """执行ORACLE的sql语句"""
        try:
            db = cx_Oracle.connect(use, pwd, tns)
            cur = db.cursor()
            cur.execute(execute)
            r = cur.fetchone()
            if r:
                res = {'st': 'normal', 'rcd': r}
            else:
                res = {'st': 'normal', 'rcd': 'None'}
            cur.close()
            db.close()
            return res
        except Exception, e:
            self.log.error(str(e))
            return {'st': 'exception', 'rcd': 'see log'}

    def waitOraTabFlag(self, tns, use, pwd, execute, flag, waitsec, timeoutcnt):
        """wait oracle flag by sql,return wait status[find or timeout]"""
        try:
            status = WAITSTATUS["TIMEOUT"]  # 0 waiting,1 findflag,2 timeout
            db = cx_Oracle.connect(use, pwd, tns)
            cur = db.cursor()
            for i in range(timeoutcnt):
                cur.execute(execute)
                r = cur.fetchone()
                if r and r[0] == flag:
                    status = WAITSTATUS['FINDFLAG']
                    break
                time.sleep(waitsec)
            cur.close()
            db.close()
            return {'st': 'normal', 'rcd': status}
        except Exception, e:
            self.log.error(str(e))
            return {'st': 'exception', 'rcd': status}

    def runOraSp(self, tns, use, pwd, execute, param):
        """call oracle storeprocedure,return data by list"""
        try:
            db = cx_Oracle.connect(use, pwd, tns)
            cur = db.cursor()
            r = cur.callproc(execute, param)
            cur.close()
            db.close()
            return {'st': 'normal', 'rcd': r}
        except Exception, e:
            self.log.error(str(e))
            return {'st': 'exception', 'rcd': str(e)}

    def callShellJob(self, shellname, parameter):
        """call shell or cmd job,and return shell's return code"""
        try:
            cmd = shellname + ' ' + parameter
            # for p in parameter:
            #    cmd= cmd + str(p)
            p = subprocess.Popen(cmd, shell=True)
            p.wait()
            return {'st': 'normal', 'rcd': str(p.returncode)}
        except Exception, e:
            tp = sys.getfilesystemencoding()
            print str(e)
            return {'st': 'exception',
                    'rcd': str(e).decode('utf-8').encode(tp)}


class JobCtlInterface(RunJob):
    '''JCI 作业控制接口'''
    C_JOBSTATUS = ['WAIT', 'RUNNING', 'ERROR', 'FINISH']

    # 全局锁，用于控制更新运行时数据库
    __lock = threading.Lock()
    # 脚本所在路径
    __scriptPath = os.path.split(os.path.realpath(__file__))[0]
    # 运行时数据库全路径
    repDBName = ''
    # 配置信息
    tconfig = {}

    def __getRepDBName(self):
        '''获取运行时资料库全路径'''
        repdbpath = self.__scriptPath + '\\' + 'runtimedb'
        if not os.path.exists(repdbpath):
            os.mkdir(repdbpath)
        return repdbpath + '\\' + self.tconfig['TASKNAME'] + '.DB'

    def __checkBaseParams(self, key, type, default):
        if (key not in self.tconfig.keys()) or \
           (not isinstance(self.tconfig[key], type)):
            if str(default) != 'None':
                self.tconfig[key] = default
                self.log.warning('%s not set,take default %s' %
                                 (key, str(default)))
                return True
            else:
                self.log.error('%s must set!' % (key))
                return False

    def __checkTaskParams(self):
        if 'TASKCONFIG' in self.tconfig.keys():
            for jbcfg in self.tconfig['TASKCONFIG']:
                if ('JOBID' not in jbcfg.keys()) or \
                   (not isinstance(jbcfg['JOBID'], basestring)):
                    self.log.error('%s must set by string!' % ('JOBID',))

                if ('DEP' not in jbcfg.keys()) or \
                   (not isinstance(jbcfg['DEP'], basestring)):
                    self.log.error('%s must set by string!' % ('DEP',))

                if ('JOBTYPE' not in jbcfg.keys()) or \
                   (not isinstance(jbcfg['JOBTYPE'], basestring)):
                    self.log.error('%s must set by string!' % ('JOBTYPE',))
                elif jbcfg['JOBTYPE'] not in self.JOBTYPE.keys():
                    self.log.error('job:%s type:%s is not support' %
                                   (jbcfg['JOBTID'], jbcfg['JOBTYPE']))

                if ('EXECUTE' not in jbcfg.keys()) or \
                   (not isinstance(jbcfg['EXECUTE'], basestring)):
                    self.log.error('%s must set by string!' % ('EXECUTE',))
        else:
            self.log.error('TASKCONFIG NOT FOUND,Please check config file')

    def __formatMsg(self, msg):
        """change list type msg to string
           将LIST和DICT类型的消息转换为分好拼接的字符串
        """
        if isinstance(msg, list) or isinstance(msg, tuple):
            msgtmp = list(msg)
            for i in range(0, msgtmp.__len__()):
                msgtmp[i] = str(msgtmp[i])
            msgtmp = ";".join(msgtmp)
            return msgtmp
        elif isinstance(msg, dict):
            msgtmp = ''
            for key in msg:
                msgtmp = msgtmp + key + '=' + str(msg[key]) + ';'
            return msgtmp
        elif isinstance(msg, basestring):
            return msg
        elif isinstance(msg, int):
            return str(msg)
        else:
            return 'can not format msg'

    def __getSucessFlag(self, rcd, express):
        '''计算表达式的值，用于自定义成功状态'''
        try:
            return eval(express)
        except Exception, e:
            self.log.error(str(e))
            return False

    def initLog(self):
        '''初始化日志'''
        logdir = self.__scriptPath + '\\log'
        if not os.path.exists(logdir):
            os.mkdir(logdir)
        lnm = '%s\\%s_%s%s.%s' % \
              (logdir, 'JCI', self.tconfig['TASKNAME'],
               time.strftime('%Y%m%d%H%M%S', time.localtime(time.time())),
               'log')
        return RunJob.initLog(self, lnm,
                              self.LOGLEVE[self.tconfig['LOGLEVEL']])

    def readConfig(self, fname):
        """read config file,and get repostory db file,init log"""
        if os.path.exists(fname):
            f = open(fname)
            js = f.read()
            try:
                self.tconfig = json.loads(js)
                self.repDBName = self.__getRepDBName()
                '''
                if "LOGSHOWCONSOLE" in self.tconfig.keys() and self.tconfig['LOGSHOWCONSOLE']:
                    sc = True
                else:
                    sc = False
                '''
                self.initLog()
                return True
            except Exception, e:
                self.log.error(str(e))
                return False
            finally:
                f.close()
        else:
            print 'config file:%s not found' % (fname)
            return False

    def printConfig(self):
        """print config"""
        # print "{name:-^80}".format(name='ATTUNITY REPLICATE CONFIG')
        # print "PRO_DIR:" + self.tconfig["ARCONFIG"]["PRO_DIR"]
        # print "DATA_DIR:" + self.tconfig["ARCONFIG"]["DATA_DIR"]
        print "{name:-^80}".format(name='MAIL CONFIG')
        print "MAILTO_LIST:" + str(self.tconfig["MAILCONFIG"]["MAILTO_LIST"])
        print "MAIL_HOST:" + self.tconfig["MAILCONFIG"]["MAIL_HOST"]
        print "MAIL_USER:" + self.tconfig["MAILCONFIG"]["MAIL_USER"]
        print "MAIL_PWD:" + self.tconfig["MAILCONFIG"]["MAIL_PWD"]
        print "MAIL_POSTFIX:" + self.tconfig["MAILCONFIG"]["MAIL_POSTFIX"]
        print "{name:-^80}".format(name='OTHER CONFIG')
        print "ORACLECLIENT:" + self.tconfig["ORACLECLIENT"]
        print "{name:-^80}".format(name='TASK CONFIG')
        for jb in self.tconfig["TASKCONFIG"]:
            print "JOBID:%s;DEP:%s;JOBTYPE:%s" % (jb["JOBID"],
                                                  jb["DEP"], jb["JOBTYPE"])
            print "EXECUTE:" + jb["EXECUTE"]
            if jb["JOBTYPE"] in ['WAIT_DB_FLAG', 'DB_PROC']:
                print "CONNECTINFO:TYPE:" + jb["CONNECTINFO"]['TYPE']
                if jb["CONNECTINFO"]['TYPE'] == 'ORACLE':
                    print "TNS:%s;USER:%s;PWD:%s" % (jb["CONNECTINFO"]['TNS'],
                                                     jb["CONNECTINFO"]['USER'],
                                                     jb["CONNECTINFO"]['PWD'])
            print "PARAMS:%s;SENTMAIL:%s" % (str(jb["PARAM"]),
                                             str(jb["SENTMAIL"]))
            print "{name:+^80}".format(name='')

    def checkConfig(self):
        '''检查参数是否符合规范，并赋默认值'''
        self.__checkBaseParams('TASKNAME', basestring,
                               'T' + str(uuid.uuid1())[0:8])
        self.__checkBaseParams('WAITSEC', int, 5)
        self.__checkBaseParams('MAXJOB', int, 5)
        self.__checkBaseParams('LOGLEVEL', basestring, 'INFO')
        self.__checkTaskParams()

    # Repository FUNCITON
    def resetRepDB(self):
        """重置资料库，仅创建需要的表"""

        # 如果文件已经存在，删除后再创建
        if os.path.exists(self.repDBName):
            try:
                os.remove(self.repDBName)
            except Exception, e:
                self.log.error(str(e))
                return False

        # 连接数据库，创建表
        res = True
        conn = sqlite3.connect(self.repDBName, check_same_thread=False)
        conn.isolation_level = None
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('''CREATE TABLE cur_jobstatus (jobid TEXT(20) NOT NULL,startdate TEXT(20),
                    enddate TEXT(20),status TEXT(10),msg TEXT(500),
                    PRIMARY KEY (jobid));''')
        # 创建当前是否使用状态的表
        cur.execute("create table cur_use (use int);")
        conn.commit()
        cur.close()
        conn.close()
        return res

    def repDBIsUsed(self):
        """判断资料库当前是否正在使用"""
        res = False
        # 如果DB存在
        if os.path.exists(self.repDBName):
            conn = sqlite3.connect(self.repDBName, check_same_thread=False)
            conn.isolation_level = None
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            try:
                cur.execute('select use from cur_use')
                a = cur.fetchone()
                if a and a[0] == 1:
                    res = True
            except Exception, e:
                self.log.error(str(e))
            cur.close()
            conn.close()
        return res

    def initRepDBData(self):
        """初始化资料库数据"""
        for job in self.tconfig['TASKCONFIG']:
            self.writeJobStatus(jobid=job['JOBID'], status='WAIT')

    def lockRepository(self, flag=1):
        '''给资料库加速，至cur_use标志位为1,防止同一个TASK被同时调用'''
        try:
            conn = sqlite3.connect(self.repDBName, check_same_thread=False)
            conn.isolation_level = None
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            if self.__lock.acquire(1):
                cur.execute('delete from cur_use')
                cur.execute('insert into cur_use values(%d)' % (flag,))
                conn.commit()
                self.__lock.release()
            cur.close()
            conn.close()
        except Exception, e:
            self.log.error(str(e))
            return False

    def writeJobStatus(self, jobid, strdt='', enddt='', status='WAIT', msg=''):
        """write job running status to Repository db
           dbfile,jobid,status must set
           if status is wait,only status is need
           if status is running,need startdate
           if status is FINISH or error,neet enddate and msg
        """
        conn = sqlite3.connect(self.repDBName, check_same_thread=False)
        conn.isolation_level = None
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        sql = ''
        if status == 'WAIT':
            sql = "insert into cur_jobstatus(jobid,status) \
            values('%s','WAIT')" % (jobid,)
        if status == 'RUNNING':
            sql = "update cur_jobstatus set startdate='%s',status='RUNNING' \
            where jobid='%s'" % (strdt, jobid,)
        if status in ['FINISH', 'ERROR']:
            sql = "update cur_jobstatus set enddate='%s',status='%s',msg='%s' \
            where jobid='%s'" % (enddt, status, msg, jobid,)
        self.log.debug(sql)
        try:
            if self.__lock.acquire(1):
                cur.execute(sql)
                conn.commit()
                self.__lock.release()
            cur.close()
            conn.close()
        except Exception, e:
            self.log.error(str(e))
            return False

    def resetErrorJobStatus(self):
        """更新错误的作业状态为等待"""
        res = False
        conn = sqlite3.connect(self.repDBName, check_same_thread=False)
        conn.isolation_level = None
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        try:
            sql = 'select count(*) from cur_jobstatus'
            cur.execute(sql)
            dt = cur.fetchone()
            if dt and dt[0] > 0:
                sql = "update cur_jobstatus set status='WAIT' \
                where status = 'ERROR'"
                cur.execute(sql)
                res = True
            conn.commit()
            cur.close()
            conn.close()
            return res
        except Exception, e:
            self.log.error(str(e))
            return False

    def getJobInfo(self, jobid):
        """get job definition,return json string"""
        rst = {}
        for jb in self.tconfig['TASKCONFIG']:
            if jb['JOBID'] == jobid:
                rst = jb
                break
        return rst

    def getJobStatus(self, jobid):
        """read job status from Repositry db,
        if not found jobinfo,return none"""
        conn = sqlite3.connect(self.repDBName, check_same_thread=False)
        conn.isolation_level = None
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        try:
            sql = "SELECT status,msg FROM cur_jobstatus WHERE jobid='%s'" % (
                jobid)
            cur = cur.execute(sql)
            have = cur.fetchone()
            cur.close()
            conn.close()
            # v2.1 change return code to dict
            if have is not None:
                return {'st': have[0], 'msg': have[1]}
            else:
                return {'st': '', 'msg': ''}
        except Exception, e:
            self.log.error(str(e))
            return {'st': 'exception', 'msg': str(e)}

    def getJobState(self, jobid):
        jobcfg = self.getJobInfo(jobid)
        st = self.getJobStatus(jobcfg['EXECUTE'])
        if st['st'] == 'exception':
            return {'st': 'exception', 'rcd': ''}
        else:
            return {'st': 'normal',
                    'rcd': {'status': st['st'], 'msg': st['msg']}}

    def jobCanRun(self, depidlist):
        """get job dependent jobstatus,if all dependent all is FINISH,return True
        根据配置文件中的依赖关系和资料库中的作业状态，
        找到一个可以运行的作业
        """
        rst = True
        for job in depidlist.split(','):
            st = self.getJobStatus(job)
            # v2.1 return code changed dict
            if st['st'] != 'FINISH':
                rst = False
                break
        return rst

    def getTaskStatus(self):
        '''get Repostory task status
            return dict like {'WAIT':4,'RUNNING':2,'FINISH':3,'ERROR':0}
        '''
        res = {}
        conn = sqlite3.connect(self.repDBName)
        conn.isolation_level = None
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        sql = "select status,count(*) FROM cur_jobstatus group by status"
        cur = cur.execute(sql)
        st = cur.fetchall()
        cur.close()
        conn.close()
        if st:
            map(lambda x: res.setdefault(x[0], x[1]), st)
            for i in self.C_JOBSTATUS:
                if i not in res.keys():
                    res[i] = 0
        return res

    def printJobStatus(self):
        """printg repository db all job running status"""
        conn = sqlite3.connect(self.repDBName, check_same_thread=False)
        conn.isolation_level = None
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        sql = 'select * from cur_jobstatus'
        cur.execute(sql)
        format_head = '|{id:^20s}|{stdt:^20s}|{enddt:^20s}|{st:^8s}|{message:^50s}|'
        formatstr = '|{id:<20s}|{stdt:<20s}|{enddt:<20s}|{st:<8s}|{message:<50s}|'
        os.system("cls")
        print "{name:-^124s}".format(name='')
        print format_head.format(id='JOBID', stdt='START', enddt='END',
                                 st='STATUS', message='MESSAGE')
        for row in cur.fetchall():
            print "{name:-^124s}".format(name='')
            print formatstr.format(id=row[0], stdt=row[1], enddt=row[2],
                                   st=row[3], message=row[4])

        print "{name:-^124s}".format(name='')
        cur.close()
        conn.close()

    def send_mail(self, sub, content):
        if 'MAILCONFIG' in self.tconfig.keys():
            RunJob.send_mail(self, self.tconfig['MAILCONFIG']['MAIL_USER'],
                             self.tconfig['MAILCONFIG']['MAIL_PWD'],
                             self.tconfig['MAILCONFIG']['MAIL_HOST'],
                             self.tconfig['MAILCONFIG']['MAILTO_LIST'],
                             sub, content)
        else:
            self.log.warn('MAILCONFIG NOT FOUND')

    def runAttunityTask(self, jobid):
        jbcfg = self.getJobInfo(jobid)
        return RunJob.runAttunityTask(self,
                                      jbcfg['ARCONFIG']['PRO_DIR'],
                                      jbcfg['ARCONFIG']['DATA_DIR'],
                                      jbcfg['EXECUTE'],
                                      jbcfg['PARAM']["OPERATION"],
                                      jbcfg['PARAM']["FLAGS"])

    def getARTaskStatus(self, jobid):
        jbcfg = self.getJobInfo(jobid)
        st = RunJob.getReplicateTaskStatus(self,
                                           jbcfg['ARCONFIG']['PRO_DIR'],
                                           jbcfg['ARCONFIG']['DATA_DIR'],
                                           jbcfg['EXECUTE'])
        if st['state'] != 'OTHER':
            res = {'st': 'normal', 'rcd': st}
        else:
            res = {'st': 'exception', 'rcd': ''}
        return res

    def waitAttunityTask(self, jobid):
        jbcfg = self.getJobInfo(jobid)
        return RunJob.waitAttunityTask(self,
                                       jbcfg['ARCONFIG']['PRO_DIR'],
                                       jbcfg['ARCONFIG']['DATA_DIR'],
                                       jbcfg['EXECUTE'],
                                       jbcfg['PARAM']['WAITSEC'],
                                       jbcfg['PARAM']['TIMEOUTCNT'])

    def select_Oracle_sql(self, jobid):
        jobcfg = self.getJobInfo(jobid)
        return RunJob.select_Oracle_sql(self,
                                        jobcfg['CONNECTINFO']['TNS'],
                                        jobcfg['CONNECTINFO']['USER'],
                                        jobcfg['CONNECTINFO']['PWD'],
                                        jobcfg['EXECUTE'])

    def waitOraTabFlag(self, jobid):
        jobcfg = self.getJobInfo(jobid)
        return RunJob.waitOraTabFlag(self,
                                     jobcfg['CONNECTINFO']['TNS'],
                                     jobcfg['CONNECTINFO']['USER'],
                                     jobcfg['CONNECTINFO']['PWD'],
                                     jobcfg['EXECUTE'],
                                     jobcfg['PARAM']['FLAG'],
                                     jobcfg['PARAM']['WAITSEC'],
                                     jobcfg['PARAM']['TIMEOUTCNT'])

    def runOraSp(self, jobid):
        jobcfg = self.getJobInfo(jobid)
        return RunJob.runOraSp(self, jobcfg['CONNECTINFO']['TNS'],
                               jobcfg['CONNECTINFO']['USER'],
                               jobcfg['CONNECTINFO']['PWD'],
                               jobcfg['EXECUTE'],
                               jobcfg['PARAM'])

    def callShellJob(self, jobid):
        jobcfg = self.getJobInfo(jobid)
        return RunJob.callShellJob(self, jobcfg['EXECUTE'], jobcfg['PARAM'])

    def custConfigTask(self):
        """Reserve function"""
        self.log.info('now cust_config_task is empty')

    def aliveThread(self, tlist):
        """check alive thread,return alive thread list
            返回活动的线程列表
        """
        alist = []
        for i in tlist:
            if i.is_alive():
                alist.append(i)
        return alist

    def getCanRunJob(self):
        """get one can run jobid by repository and config"""
        # 默认值
        jobid = ''
        for job in self.tconfig['TASKCONFIG']:
            # 只处理等待类型的作业
            # v2.1 return code changed dict
            if self.getJobStatus(job['JOBID'])['st'] != 'WAIT':
                continue
            # 如果没有依赖
            if job['DEP'] == '':
                jobid = job['JOBID']
                break
            # 依赖的作业都运行成功
            elif self.jobCanRun(job['DEP']):
                jobid = job['JOBID']
                break
        return jobid

    def checkSucessExpress(self, jobcfg, res):
        """根据成功表达式检查作业是否成功"""
        # 如果有自定义表达式设置，则根据自定义表达式判断作业成功标志
        if 'SUCESSEXPRESSION' in jobcfg.keys():
            if res['st'] == 'normal' and \
               self.__getSucessFlag(res['rcd'], jobcfg['SUCESSEXPRESSION']):
                st = 'FINISH'
            else:
                st = 'ERROR'
        # 如果没有自定义表达式设置，则仅根据是否有异常判断
        else:
            if res['st'] == 'normal':
                st = 'FINISH'
            else:
                st = 'ERROR'
        return st

    def runJobOnce(self, jobid):
        """根据作业类型和数据库类型
        1. 判断是否支持
        2. 运行作业
        3. 判断运行结束的状态
        """

        # 获取作业配置
        jobcfg = self.getJobInfo(jobid)
        if jobcfg['JOBTYPE'] == 'WAIT_DB_FLAG':
            if jobcfg['CONNECTINFO']['TYPE'] == 'ORACLE':
                res = self.waitOraTabFlag(jobid)
                if res['st'] == 'normal' and \
                   res['rcd'] == WAITSTATUS['FINDFLAG']:
                    st = 'FINISH'
                else:
                    st = 'ERROR'
            else:
                st = 'ERROR'
                self.log.error('job:%s,type:%s is not support' %
                               (jobid, jobcfg['JOBTYPE']))

        elif jobcfg['JOBTYPE'] == 'SELECTDBSQL':
            if jobcfg['CONNECTINFO']['TYPE'] == 'ORACLE':
                res = self.select_Oracle_sql(jobid)
                st = self.checkSucessExpress(jobcfg, res)
            else:
                st = 'ERROR'
                self.log.error('job:%s,type:%s is not support' %
                               (jobid, jobcfg['JOBTYPE']))

        elif jobcfg['JOBTYPE'] == 'RUN_AR_TASK':
            res = self.runAttunityTask(jobid)
            if res['st'] == 'normal' and \
               str(res['rcd']) == self.AR_SUCESS_TEXT:
                st = 'FINISH'
            else:
                st = 'ERROR'

        elif jobcfg['JOBTYPE'] == 'WAIT_AR_TASK':
            res = self.waitAttunityTask(jobid)
            if res['st'] == 'normal' and res['rcd'] == WAITSTATUS['FINDFLAG']:
                st = 'FINISH'
            else:
                st = 'ERROR'

        elif jobcfg['JOBTYPE'] == 'DB_PROC':
            if jobcfg['CONNECTINFO']['TYPE'] == 'ORACLE':
                res = self.runOraSp(jobid)
                st = self.checkSucessExpress(jobcfg, res)
            else:
                st = 'ERROR'
                self.log.error('job:%s,type:%s is not support' %
                               (jobid, jobcfg['JOBTYPE']))

        elif jobcfg['JOBTYPE'] == 'SHELL':
            res = self.callShellJob(jobid)
            st = self.checkSucessExpress(jobcfg, res)
        elif jobcfg['JOBTYPE'] == 'GET_JOB_STATE':
            res = self.getJobState(jobid)
            st = self.checkSucessExpress(jobcfg, res)
        elif jobcfg['JOBTYPE'] == 'GET_AR_TASK_STATUS':
            res = self.getARTaskStatus(jobid)
            st = self.checkSucessExpress(jobcfg, res)
        else:
            st = 'ERROR'
            self.log.error('job:%s,type:%s is not support' %
                           (jobid, jobcfg['JOBTYPE']))
        return st, res['rcd']

    def loopRunJob(self, jobid):
        """run job by jobtype,and update Repository db info"""

        # 获取作业配置
        jobcfg = self.getJobInfo(jobid)
        # 记录作业已开始运行到资料库
        self.writeJobStatus(jobid, strdt=self.getTime(), status='RUNNING')
        waitsec = 0
        timeoutcnt = 1
        if 'LOOP' in jobcfg.keys():
            try:
                waitsec = jobcfg['LOOP']['WAITSEC']
                timeoutcnt = jobcfg['LOOP']['TIMEOUTCNT']
            except Exception:
                pass
        for i in range(timeoutcnt):
            st, rcd = self.runJobOnce(jobid)
            if st == 'FINISH':
                break
            time.sleep(waitsec)
        if ('SENTMAIL' in jobcfg.keys()) and (jobcfg['SENTMAIL'] == 1):
            self.send_mail('%s is %s' % (jobid, st),
                           'msg is %s' % (self.__formatMsg(rcd)))
        self.writeJobStatus(jobid, enddt=self.getTime(),
                            status=st, msg=self.__formatMsg(rcd))

    def runTask(self, showStatus=True, rerun=False):
        """run task by config file"""
        self.log.info('start task %s on %s ,pid is :%d' %
                      (self.tconfig['TASKNAME'], self.getTime(), os.getpid()))

        # 根据rerun标志初始化资料库
        # 判断资料库是否正在使用
        if self.repDBIsUsed():
            # 正在使用，则直接退出
            self.log.error(
                'have same task used,please check or delete taskdb rerun')
            return
        self.log.debug('repDB not used')
        # 初始化资料库表结构
        if not rerun:
            if not self.resetRepDB():
                self.log.error('reset RepDatabase Failed,plase check')
                return
            # 初始化资料库数据
            self.initRepDBData()
            self.log.debug('init StatusDatabase date')
        else:
            self.resetErrorJobStatus()
            self.log.debug('rerun setting,update StatusDataBase')

        # 锁定数据库
        self.lockRepository(1)
        self.log.debug('lock StatusDataBase')
        self.log.info('init StatusDataBase FINISH')

        thlist = []
        while True:
            # 检查当前活动的作业
            alivejobcnt = len(self.aliveThread(thlist))
            self.log.debug('now have %d jobs running' % (alivejobcnt))

            # 获取一个符合运行条件的作业
            jobid = self.getCanRunJob()
            self.log.info('Get can run job :%s' % (str(jobid)))
            # 可以获取到有效的作业
            if jobid:
                # 如果当前活动的作业小于最大并发度，调度作业运行
                if alivejobcnt < self.tconfig['MAXJOB']:
                    thd = threading.Thread(target=self.loopRunJob,
                                           args=(jobid,),
                                           name='loopRunJob(%s)' % (jobid))
                    thd.setDaemon(True)
                    thd.start()
                    thlist.append(thd)
                    self.log.info('start job :%s' % (str(jobid)))
            # 如果获取不到有效的作业，并且没有活动作业
            elif alivejobcnt == 0:
                logmsg = 'no job can run,and no alive thread,task FINISH'
                self.log.info(logmsg)
                break
            if showStatus:
                self.printJobStatus()

            time.sleep(self.tconfig["WAITSEC"])

        self.log.info(self.getTaskStatus())
        self.printJobStatus()
        self.lockRepository(0)
        self.log.debug('unlock StatusDataBase')
        self.log.info('task is finish')

    def __init__(self):
        pass
        # self.readConfig(cfgFile)


# main function
# COMMAND LIST
CMD_LIST = ['-runtask', '-rerun', '-debug']
DEBUG_MEMU_LIST = ['-gettaskstatus', '-getjobstatus',
                   '-resetdb', '-showconfig', '-runjob',
                   '-test', '-sendtestmail',
                   '-runtask', '-rerun', '-checkconfig']


def printhelp():
    print '--------------------help--------------------------'
    print 'you need input jobcontrol [configfile] -runtask'
    print '                                       -rerun'
    print '                                       -debug'
    print '                           -help'
    print '--------------------help--------------------------'


def test():
    """debug function"""
    print "this method is empty!"
    jci.checkConfig()


def main():
    # 如果只有一个参数，并且参数为-help，则打印帮助
    if len(sys.argv) == 2 and sys.argv[1] == '-help':
        printhelp()
        exit()

    # 如果有两个参数，并且参数在命令列表中，则执行mainwork
    if len(sys.argv) == 3 and sys.argv[2] in CMD_LIST:
        cmd_type = sys.argv[2]
        cmd_cfg = sys.argv[1]
        mainwork(cmd_type, cmd_cfg, showStatus=False)

    # 如果没有参数，则进入交互式运行模式
    if len(sys.argv) == 1:
        promt = 'Please input parameter:\niconfigfile[filename] option%s\nor input [quit] to exit\n' % (
            CMD_LIST, )
        promt = ' '.join(promt.split())
        while True:
            parmas = raw_input(promt + '\n')
            if parmas == 'quit':
                break
            if parmas.find(' ') > 0:
                p = parmas.split(' ')
                if len(p) == 2 and p[1] in CMD_LIST:
                    mainwork(p[1], p[0])
                    break


def mainwork(cmd_type, cmd_cfg, showStatus=True):
    # 如果配置文件存在,读取配置文件并初始化日志
    if os.path.exists(cmd_cfg):
        # 读取配置文件
        global jci
        jci = JobCtlInterface()
        tt = jci.readConfig(cmd_cfg)
        # print tt
        if tt:
            # 根据命令执行具体动作
            if cmd_type == '-runtask':
                jci.runTask(showStatus)
            elif cmd_type == '-rerun':
                jci.runTask(showStatus, rerun=True)
            elif cmd_type == '-debug':
                debug()
        else:
            logging.error('open config file %s is fail' % (cmd_cfg))
            sys.exit(0)
    else:
        print '%s is not found' % (cmd_cfg)
        sys.exit(0)


def debug():
    promt = 'Please input option:\n%s \nor input [quit] to exit\n' % (
        DEBUG_MEMU_LIST, )
    while True:
        parmas = raw_input(promt)
        # 剔除多余空格，拆分命令和参数
        tmpcmd = parmas.split()
        if tmpcmd:
            if len(tmpcmd) == 1:
                d_cmd = tmpcmd[0]
                d_param = ''
            elif len(tmpcmd) >= 2:
                d_cmd = tmpcmd[0]
                d_param = tmpcmd[1]
        else:
            continue

        if d_cmd == 'quit':
            break
        elif d_cmd == '-gettaskstatus':
            if os.path.exists(jci.repDBName):
                jci.printJobStatus()
            else:
                jci.log.info('not found any running info')
        elif d_cmd == '-showconfig':
            jci.printConfig()
        elif d_cmd == '-test':
            test()
        elif d_cmd == '-sendtestmail':
            jci.send_mail('test', 'test')
        elif d_cmd == '-runjob':
            # 可以正确获取作业信息
            if d_param and jci.getJobInfo(d_param):
                if jci.repDBIsUsed():
                    # 正在使用，则直接退出
                    jci.log.error('runtime db is used,check or resetdb')
                    continue
                jci.loopRunJob(d_param)
            else:
                logging.info('job:%s not found' % (d_param, ))
        elif d_cmd == '-resetdb':
            # 初始化资料库表结构
            if not jci.resetRepDB():
                logging.error('reset RepDatabase Failed,plase check')
                continue
            jci.initRepDBData()
        elif d_cmd == '-getjobstatus':
            print jci.getJobStatus(d_param)
        elif d_cmd == '-runtask':
            jci.runTask(showStatus=True, rerun=False)
        elif d_cmd == '-rerun':
            jci.runTask(showStatus=True, rerun=True)
        elif d_cmd == '-checkconfig':
            jci.checkConfig()


if __name__ == '__main__':
    main()
