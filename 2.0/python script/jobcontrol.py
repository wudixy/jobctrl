# -*- coding: utf-8 -*-

'''作业调度模块

根据配置文件(JSON格式)，并行调度作业执行

Notes:

1.需要安装ORACLE客户端
2.目前支持的作业类型为
    a. Attunity Replicate Task
    b. ORACLE STOPR PROCEDURE
    c. WAIT DATABASE TABLE FLAG
    d. WAIT ATTUNITY REPLICATE TASK FINISH

Example:
  >>> jobcontrol [configfile] -runtask
'''


# @Date    : 2016-11-07 23:35:18
# @Author  : wudi (wudi@xiyuetech.com)
# @Link    : ${link}
# @Version : 1.0

# base module
import json
import os
import smtplib
from email.mime.text import MIMEText
import logging
import time
import subprocess
import sqlite3
import threading
import sys

# Third party module
import cx_Oracle


# constant setting
# Logleve mapping
LOGLEVE = {"DEBUG": logging.DEBUG,
           "INFO": logging.INFO,
           "ERROR": logging.ERROR
           }
# ATTUNITY REPLICATE COMMAND SUCCEEDED STRING
AR_SUCESS_TEXT = 'Succeeded'

# COMMAND LIST
CMD_LIST = ['-runtask', '-rerun', '-debug']
DEBUG_MEMU_LIST = ['-gettaskstatus', '-getjobstatus',
                   '-resetdb', '-showconfig', '-runjob',
                   '-test', '-sendtestmail']

# MAPPING OF WAIT TYPE JOB
WAITSTATUS = {"WAITING": 0, "FINDFLAG": 1, "TIMEOUT": 2}

TASKSTATUS = {"ALLFINISH": 0, "HAVEERROR": 1}

C_JOBSTATUS = ['WAIT', 'RUNNING', 'ERROR', 'Finish']


# BASE FUNCITON
def initLog(logname, level):
    """INIT LOG SETTING,初始化日志设置"""
    # %(asctime)s [%(levelname)s] %(filename) s [line:%(lineno)d]
    # %(funcName) s.%(message) s
    logformat = '%(asctime)s [%(levelname)s] [line:%(lineno)d] %(funcName) s.%(message) s'
    logging.basicConfig(
        level=level,
        format=logformat,
        datefmt='%Y-%m-%dT%H:%M:%S',
        filename=logname)
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(level)
    # set a format which is simpler for console use
    formatter = logging.Formatter(logformat)
    # tell the handler to use this format
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    return logging.getLogger()


def readConfig(fname='TASKCONFIG.JSON'):
    """read config file,return json string"""
    try:
        f = open(fname)
        js = f.read()
        # cfg=json.load(f)
        cfg = json.loads(js)
        f.close()
        return cfg
    except Exception, e:
        logging.error(str(e))
        return False


def printConfig(cfg):
    """print config"""
    print "{name:-^80}".format(name='ATTUNITY REPLICATE CONFIG')
    print "PRO_DIR:" + cfg["ARCONFIG"]["PRO_DIR"]
    print "DATA_DIR:" + cfg["ARCONFIG"]["DATA_DIR"]
    print "{name:-^80}".format(name='MAIL CONFIG')
    print "MAILTO_LIST:" + str(cfg["MAILCONFIG"]["MAILTO_LIST"])
    print "MAIL_HOST:" + cfg["MAILCONFIG"]["MAIL_HOST"]
    print "MAIL_USER:" + cfg["MAILCONFIG"]["MAIL_USER"]
    print "MAIL_PWD:" + cfg["MAILCONFIG"]["MAIL_PWD"]
    print "MAIL_POSTFIX:" + cfg["MAILCONFIG"]["MAIL_POSTFIX"]
    print "{name:-^80}".format(name='OTHER CONFIG')
    print "ORACLECLIENT:" + cfg["ORACLECLIENT"]
    print "{name:-^80}".format(name='TASK CONFIG')
    for jb in cfg["TASKCONFIG"]:
        print "JOBID:%s;DEP:%s;JOBTYPE:%s" % (jb["JOBID"], jb["DEP"],
                                              jb["JOBTYPE"])
        print "EXECUTE:" + jb["EXECUTE"]
        if jb["JOBTYPE"] in ['WAIT_DB_FLAG', 'DB_PROC']:
            print "CONNECTINFO:TYPE:" + jb["CONNECTINFO"]['TYPE']
            if jb["CONNECTINFO"]['TYPE'] == 'ORACLE':
                print "TNS:%s;USER:%s;PWD:%s" % (jb["CONNECTINFO"]['TNS'],
                                                 jb["CONNECTINFO"]['USER'],
                                                 jb["CONNECTINFO"]['PWD'])
        print "PARAMS:%s;SENTMAIL:%s" % (str(jb["PARAM"]), str(jb["SENTMAIL"]))
        print "{name:+^80}".format(name='')


def check(cfg):
    '''检查参数是否符合规范，并赋默认值'''

    pass


def getTime():
    """get now time by yyyy-mm-dd hh:mm:ss format"""
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))


def printhelp():
    print '--------------------help--------------------------'
    print 'you need input jobcontrol [configfile] -runtask'
    print '                                       -rerun'
    print '                                       -debug'
    print '                           -help'
    print '--------------------help--------------------------'

# Repository FUNCITON


# 创建全局锁，供后续对Repository进行写操作使用
global lock
lock = threading.Lock()


def resetRepDB(fname):
    """重置资料库，仅创建需要的表"""

    # 如果文件已经存在，删除后再创建
    if os.path.exists(fname):
        try:
            os.remove(fname)
        except Exception, e:
            logging.error(str(e))
            return False

    # 连接数据库，创建表
    res = True
    conn = sqlite3.connect(fname, check_same_thread=False)
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


def repDBIsUsed(fname):
    """判断资料库当前是否正在使用"""
    res = False
    # 如果DB存在
    if os.path.exists(fname):
        conn = sqlite3.connect(fname, check_same_thread=False)
        conn.isolation_level = None
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        try:
            cur.execute('select use from cur_use')
            a = cur.fetchone()
            if a and a[0] == 1:
                res = True
        except Exception, e:
            logging.error(str(e))
        cur.close()
        conn.close()

    return res


def initRepDBData(fname, joblist):
    """初始化资料库数据"""
    for job in joblist:
        writeJobStatus(fname, job['JOBID'], status='WAIT')

"""

def initRepository(fname):
    '''initialization Repository DataBase,
    create table or truncate table data
    初始化资料库,创建表或删除数据
    '''
    # 创建全局锁，供后续对Repository进行写操作使用
    global lock
    lock = threading.Lock()

    # 连接数据库
    res = True
    conn = sqlite3.connect(fname, check_same_thread=False)
    conn.isolation_level = None
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # find table
    sql = "select count(*) from sqlite_master \
           where type='table' AND name='cur_use'"

    cur = cur.execute(sql)
    have = cur.fetchone()
    # if not found,then create table,else delete data
    if have is not None and have[0] == 0:
        # 创建作业状态表
        cur.execute('''CREATE TABLE cur_jobstatus (jobid TEXT(20) NOT NULL,startdate TEXT(20),
                    enddate TEXT(20),status TEXT(10),msg TEXT(500),
                    PRIMARY KEY (jobid));''')
        # 创建当前是否使用状态的表
        cur.execute("create table cur_use (use int);")
        # cur.execute("insert into cur_use values(0)")
    else:
        # 如果表存在，则检查当前是否正在运行使用
        cur.execute('select use from cur_use')
        a = cur.fetchone()
        # 正在使用
        if a and a[0] == 1:
            res = False
        else:
            # 删除数据
            cur.execute('delete from cur_jobstatus')
            cur.execute('delete from cur_use')
            cur.execute('VACUUM')

    conn.commit()
    cur.close()
    conn.close()
    return res
"""


def lockRepository(fname, flag=1):
    '''给资料库加速，至cur_use标志位为1,防止同一个TASK被同时调用'''
    try:
        conn = sqlite3.connect(fname, check_same_thread=False)
        conn.isolation_level = None
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        if lock.acquire(1):
            cur.execute('delete from cur_use')
            cur.execute('insert into cur_use values(%d)' % (flag,))
            conn.commit()
            lock.release()
        cur.close()
        conn.close()
    except Exception, e:
        logging.error(str(e))
        return False


def writeJobStatus(fname, jobid, strdt='', enddt='', status='WAIT', msg=''):
    """write job running status to Repository db
       dbfile,jobid,status must set
       if status is wait,only status is need
       if status is running,need startdate
       if status is finish or error,neet enddate and msg
    """
    conn = sqlite3.connect(fname, check_same_thread=False)
    conn.isolation_level = None
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    sql = ''
    if status == 'WAIT':
        sql = "insert into cur_jobstatus(jobid,status) values('%s','WAIT')" % (jobid,)
    if status == 'RUNNING':
        sql = "update cur_jobstatus set startdate='%s',status='RUNNING' \
        where jobid='%s'" % (strdt, jobid,)
    if status in ['Finish', 'ERROR']:
        sql = "update cur_jobstatus set enddate='%s',status='%s',msg='%s' \
        where jobid='%s'" % (enddt, status, msg, jobid,)
    logging.debug(sql)
    try:
        if lock.acquire(1):
            cur.execute(sql)
            conn.commit()
            lock.release()
        cur.close()
        conn.close()
    except Exception, e:
        logging.error(str(e))
        return False


def resetErrorJobStatus(fname):
    """更新错误的作业状态为等待"""
    res = False
    conn = sqlite3.connect(fname, check_same_thread=False)
    conn.isolation_level = None
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        sql = 'select count(*) from cur_jobstatus'
        cur.execute(sql)
        dt = cur.fetchone()
        if dt and dt[0] > 0:
            sql = "update cur_jobstatus set status='WAIT' where status = 'ERROR'"
            cur.execute(sql)
            res = True
        conn.commit()
        cur.close()
        conn.close()
        return res
    except Exception, e:
        logging.error(str(e))
        return False


def getJobStatus(fname, jobid):
    """read job status from Repositry db,if not found jobinfo,return none"""
    conn = sqlite3.connect(fname, check_same_thread=False)
    conn.isolation_level = None
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    sql = "SELECT status FROM cur_jobstatus WHERE jobid='%s'" % (jobid)
    cur = cur.execute(sql)
    have = cur.fetchone()
    cur.close()
    conn.close()
    if have is not None:
        return have[0]
    else:
        return None


def jobCanRun(fname, depidlist):
    """get job dependent jobstatus,if all dependent all is finish,return True
    根据配置文件中的依赖关系和资料库中的作业状态，
    找到一个可以运行的作业
    """
    rst = True
    for job in depidlist.split(','):
        st = getJobStatus(fname, job)
        if str(st) != 'Finish':
            rst = False
            break
    return rst


def getTaskStatus(fname):
    '''get Repostory task status
       return dict like {'WAIT':4,'RUNNING':2,'Finish':3,'ERROR':0}
    '''
    res = {}
    conn = sqlite3.connect(fname)
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
        for i in C_JOBSTATUS:
            if i not in res.keys():
                res[i] = 0
    return res


def printJobStatus(fname):
    """printg repository db all job running status"""
    conn = sqlite3.connect(fname, check_same_thread=False)
    conn.isolation_level = None
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    sql = 'select * from cur_jobstatus'
    cur.execute(sql)
    format_head = '|{id:^5s}|{stdt:^20s}|{enddt:^20s}|{st:^8s}|{message:^10s}|'
    formatstr = '|{id:<5s}|{stdt:<20s}|{enddt:<20s}|{st:<8s}|{message:<10s}|'
    os.system("cls")
    print "{name:-^68s}".format(name='')
    print format_head.format(id='JOBID', stdt='START', enddt='END',
                                st='STATUS', message='MESSAGE')
    for row in cur.fetchall():
        print "{name:-^68s}".format(name='')
        print formatstr.format(id=row[0], stdt=row[1], enddt=row[2],
                               st=row[3], message=row[4])

    print "{name:-^68s}".format(name='')
    cur.close()
    conn.close()


# JOB FUNCITON
def send_mail(mail_user, mail_pass, mail_host, to_list, sub, content):
    """sent mail ,if sent success ,return True"""
    content = content + '\n' + 'make mail time:' + getTime()
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
        logging.error(str(e).decode('utf-8').encode(tp))
        return False


def cfg_send_mail(sub, content):
    return send_mail(tconfig['MAILCONFIG']['MAIL_USER'],
                     tconfig['MAILCONFIG']['MAIL_PWD'],
                     tconfig['MAILCONFIG']['MAIL_HOST'],
                     tconfig['MAILCONFIG']['MAILTO_LIST'],
                     sub, content)


def runAttunityTask(pdir, ddir, taskname, operation, flags):
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
            logging.error(r)
        else:
            rcd = r.rstrip()[-9:]
        return {'st': 'normal', 'rcd': rcd}
        # p.wait()
    except Exception, e:
        logging.error(str(e))
        return {'st': 'exception', 'rcd': ''}


def cfg_runAttunityTask(jobid):
    jbcfg = getJobInfo(jobid, tconfig)
    return runAttunityTask(tconfig['ARCONFIG']['PRO_DIR'],
                           tconfig['ARCONFIG']['DATA_DIR'],
                           jbcfg['EXECUTE'],
                           jbcfg['PARAM']["OPERATION"],
                           jbcfg['PARAM']["FLAGS"])


def getReplicateTaskStatus(pdir, ddir, taskname):
    """get Attunity Replicate Task Status"""
    r = 'OTHER'
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
        r = xx['task_status']["state"]
    except Exception, e:
        logging.error(str(e))
    return r


def cfg_getReplicateTaskStatus(jobid):
    jbcfg = getJobInfo(jobid, tconfig)
    return getReplicateTaskStatus(tconfig['ARCONFIG']['PRO_DIR'],
                                  tconfig['ARCONFIG']['DATA_DIR'],
                                  jbcfg['EXECUTE'])


def waitAttunityTask(pdir, ddir, taskname, waitsec, timeoutcnt):
    """wait Attunity Replicate task finish,
    return wait status[find or timeout]"""
    status = WAITSTATUS['TIMEOUT']  # 0 waiting,1 findflag,2 timeout
    for i in range(timeoutcnt):
        st = getReplicateTaskStatus(pdir, ddir, taskname)
        if st == 'STOPPED' or st == 'OTHER':
            break
        time.sleep(waitsec)
    if st == 'STOPPED':
        status = WAITSTATUS['FINDFLAG']
    return {'st': 'normal', 'rcd': status}


def cfg_waitAttunityTask(jobid):
    jbcfg = getJobInfo(jobid, tconfig)
    return waitAttunityTask(tconfig['ARCONFIG']['PRO_DIR'],
                            tconfig['ARCONFIG']['DATA_DIR'],
                            jbcfg['EXECUTE'],
                            jbcfg['PARAM']['WAITSEC'],
                            jbcfg['PARAM']['TIMEOUTCNT'])


def waitOraTabFlag(tns, use, pwd, execute, flag, waitsec, timeoutcnt):
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
        logging.error(str(e))
        return {'st': 'exception', 'rcd': status}


def cfg_waitOraTabFlag(jobid):
    jobcfg = getJobInfo(jobid, tconfig)
    return waitOraTabFlag(jobcfg['CONNECTINFO']['TNS'],
                          jobcfg['CONNECTINFO']['USER'],
                          jobcfg['CONNECTINFO']['PWD'],
                          jobcfg['EXECUTE'],
                          jobcfg['PARAM']['FLAG'],
                          jobcfg['PARAM']['WAITSEC'],
                          jobcfg['PARAM']['TIMEOUTCNT'])


def runOraSp(tns, use, pwd, execute, param):
    """call oracle storeprocedure,return data by list"""
    try:
        db = cx_Oracle.connect(use, pwd, tns)
        cur = db.cursor()
        r = cur.callproc(execute, param)
        cur.close()
        db.close()
        return {'st': 'normal', 'rcd': r}
    except Exception, e:
        logging.error(str(e))
        return {'st': 'exception', 'rcd': str(e)}


def cfg_runOraSp(jobid):
    jobcfg = getJobInfo(jobid, tconfig)
    return runOraSp(jobcfg['CONNECTINFO']['TNS'],
                    jobcfg['CONNECTINFO']['USER'],
                    jobcfg['CONNECTINFO']['PWD'],
                    jobcfg['EXECUTE'],
                    jobcfg['PARAM'])


def callShellJob(shellname, parameter):
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
        return {'st': 'exception', 'rcd': str(e).decode('utf-8').encode(tp)}


def cfg_callShellJob(jobid):
    jobcfg = getJobInfo(jobid, tconfig)
    return callShellJob(jobcfg['EXECUTE'], jobcfg['PARAM'])


# TASK FUNCITON

def getJobInfo(jobid, cfg):
    """get job definition,return json string"""
    rst = {}
    for jb in cfg['TASKCONFIG']:
        if jb['JOBID'] == jobid:
            rst = jb
            break
    return rst


def getDepJob(jobid, cfg):
    """get dependent job info,return json string"""
    rst = ''
    for jb in cfg['TASKCONFIG']:
        if jb['JOBID'] == jobid:
            rst = jb['DEP']
    return rst


def custConfigTask():
    """Reserve function"""
    logging.info('now cust_config_task is empty')


def formatMsg(msg):
    """change list type msg to string
       将LIST类型的消息转换为分好拼接的字符串
    """
    if isinstance(msg, list):
        msgtmp = msg
        for i in range(0, msgtmp.__len__()):
            msgtmp[i] = str(msgtmp[i])
        msgtmp = ";".join(msgtmp)
        return msgtmp
    else:
        return msg


def aliveThread(tlist):
    """check alive thread,return alive thread list
    返回活动的线程列表
    """
    alist = []
    for i in tlist:
        if i.is_alive():
            alist.append(i)
    return alist


def cfg_getCanRunJob(fname):
    """get one can run jobid by repository and config"""
    # 默认值
    jobid = ''

    for job in tconfig['TASKCONFIG']:
        # 只处理等待类型的作业
        if getJobStatus(fname, job['JOBID']) != 'WAIT':
            continue
        # 如果没有依赖
        if job['DEP'] == '':
            jobid = job['JOBID']
            break
        # 依赖的作业都运行成功
        elif jobCanRun(fname, job['DEP']):
            jobid = job['JOBID']
            break
    return jobid


def getSucessFlag(rcd, express):
    '''计算表达式的值，用于自定义成功状态'''
    try:
        return eval(express)
    except Exception, e:
        logging.error(str(e))
        return False


def checkSucessExpress(jobcfg, res):
    """根据成功表达式检查作业是否成功"""

    # 如果有自定义表达式设置，则根据自定义表达式判断作业成功标志
    if 'SUCESSEXPRESSION' in jobcfg.keys():
        if res['st'] == 'normal' and getSucessFlag(res['rcd'], jobcfg['SUCESSEXPRESSION']):
            st = 'Finish'
        else:
            st = 'ERROR'
    # 如果没有自定义表达式设置，则仅根据是否有异常判断
    else:
        if res['st'] == 'normal':
            st = 'Finish'
        else:
            st = 'ERROR'
    return st


def cfg_runJob(fname, jobid):
    """run job by jobtype,and update Repository db info"""

    # 获取作业配置
    jobcfg = getJobInfo(jobid, tconfig)

    # 记录作业已开始运行到资料库
    writeJobStatus(fname, jobid, strdt=getTime(), status='RUNNING')

    # 根据作业类型和数据库类型
    # 1. 判断是否支持
    # 2. 运行作业
    # 3. 判断运行结束的状态
    # 4. 记录状态到资料库
    if jobcfg['JOBTYPE'] == 'WAIT_DB_FLAG':
        if jobcfg['CONNECTINFO']['TYPE'] == 'ORACLE':
            res = cfg_waitOraTabFlag(jobid)
            if res['st'] == 'normal' and res['rcd'] == WAITSTATUS['FINDFLAG']:
                st = 'Finish'
            else:
                st = 'ERROR'
        else:
            st = 'ERROR'
            logging.error('job:%s,type:%s is not support' %
                          (jobid, jobcfg['JOBTYPE']))

    elif jobcfg['JOBTYPE'] == 'RUN_AR_TASK':
        res = cfg_runAttunityTask(jobid)
        if res['st'] == 'normal' and str(res['rcd']) == AR_SUCESS_TEXT:
            st = 'Finish'
        else:
            st = 'ERROR'

    elif jobcfg['JOBTYPE'] == 'WAIT_AR_TASK':
        res = cfg_waitAttunityTask(jobid)
        if res['st'] == 'normal' and res['rcd'] == WAITSTATUS['FINDFLAG']:
            st = 'Finish'
        else:
            st = 'ERROR'

    elif jobcfg['JOBTYPE'] == 'DB_PROC':
        if jobcfg['CONNECTINFO']['TYPE'] == 'ORACLE':
            res = cfg_runOraSp(jobid)
            st = checkSucessExpress(jobcfg, res)
        else:
            st = 'ERROR'
            logging.error('job:%s,type:%s is not support' %
                          (jobid, jobcfg['JOBTYPE']))

    elif jobcfg['JOBTYPE'] == 'SHELL':
        res = cfg_callShellJob(jobid)
        st = checkSucessExpress(jobcfg, res)
    else:
        st = 'ERROR'
        logging.error('job:%s,type:%s is not support' %
                      (jobid, jobcfg['JOBTYPE']))

    writeJobStatus(fname, jobid, enddt=getTime(),
                   status=st, msg=formatMsg(res['rcd']))


def cfg_runTask(showStatus=True, rerun=False):
    """run task by config file"""
    logging.info('start task %s on %s ,pid is :%d' %
                 (tconfig['TASKNAME'], getTime(), os.getpid()))
    dbname = tconfig['TASKNAME'] + '.db'
    # 根据rerun标志初始化资料库
    # 判断资料库是否正在使用
    if repDBIsUsed(dbname):
        # 正在使用，则直接退出
        logging.error('have same task used,please check or delete taskdb rerun')
        return
    logging.debug('repDB not used')
    # 初始化资料库表结构
    if not rerun:
        if not resetRepDB(dbname):
            logging.error('reset RepDatabase Failed,plase check')
            return
        # 初始化资料库数据
        initRepDBData(dbname, tconfig['TASKCONFIG'])
        logging.debug('init StatusDatabase date')
    else:
        resetErrorJobStatus(dbname)
        logging.debug('rerun setting,update StatusDataBase')

    # 锁定数据库
    lockRepository(dbname, 1)
    logging.debug('lock StatusDataBase')
    logging.info('init StatusDataBase Finish')

    thlist = []
    while True:
        # 检查当前活动的作业
        alivejobcnt = len(aliveThread(thlist))
        logging.debug('now have %d jobs running' % (alivejobcnt))

        # 获取一个符合运行条件的作业
        jobid = cfg_getCanRunJob(dbname)
        logging.info('Get can run job :%s' % (str(jobid)))
        # 可以获取到有效的作业
        if jobid:
            # 如果当前活动的作业小于最大并发度，调度作业运行
            if alivejobcnt < tconfig['MAXJOB']:
                thd = threading.Thread(target=cfg_runJob, args=(
                    dbname, jobid,), name='cfg_runJob(%s)' % (jobid))
                thd.setDaemon(True)
                thd.start()
                thlist.append(thd)
                logging.info('start job :%s' % (str(jobid)))
        # 如果获取不到有效的作业，并且没有活动作业
        elif alivejobcnt == 0:
            logmsg = 'no job can run,and no alive thread,task finish'
            logging.info(logmsg)
            break
        if showStatus:
            printJobStatus(dbname)

        time.sleep(tconfig["WAITSEC"])

    logging.info(getTaskStatus(dbname))
    printJobStatus(dbname)
    lockRepository(dbname, 0)
    logging.debug('unlock StatusDataBase')
    logging.info('task is fin')



def test():
    """debug function"""
    print "this method is empty!"


# MAIN FUNCITON
def mainwork(cmd_type, cmd_cfg, showStatus=True):
    # 如果配置文件存在,读取配置文件并初始化日志
    if os.path.exists(cmd_cfg):
        # 读取配置文件
        global tconfig
        tconfig = readConfig(cmd_cfg)
        if str(tconfig) == 'False':
            logging.error('open config file %s is fail' % (cmd_cfg))
            sys.exit(0)
        else:
            # 配置文件读取成功,初始化日志
            logdir = os.getcwd() + '\\log'
            if not os.path.exists(logdir):
                os.mkdir(logdir)
            lnm = '%s\\%s_%s%s.%s' % \
                (logdir, 'JCI', tconfig['TASKNAME'], time.strftime(
                    '%Y%m%d%H%M%S', time.localtime(time.time())), 'log')
            initLog(lnm, LOGLEVE[tconfig['LOGLEVEL']])
    else:
        print '%s is not found' % (cmd_cfg)
        sys.exit(0)

    # 根据命令执行具体动作
    if cmd_type == '-runtask':
        cfg_runTask(showStatus)
    elif cmd_type == '-rerun':
        cfg_runTask(showStatus, rerun=True)
    elif cmd_type == '-debug':
        debug()


def debug():

    promt = 'Please input option:\n%s \nor input [quit] to exit\n' % (DEBUG_MEMU_LIST, )
    dbname = tconfig['TASKNAME'] + '.db'
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
            if os.path.exists(dbname):
                printJobStatus(dbname)
            else:
                logging.info('not found any running info')
        elif d_cmd == '-showconfig':
            printConfig(tconfig)
        elif d_cmd == '-test':
            test()
        elif d_cmd == '-sendtestmail':
            cfg_send_mail('test', 'test')
        elif d_cmd == '-runjob':
            # 可以正确获取作业信息
            if d_param and getJobInfo(d_param, tconfig):
                if repDBIsUsed(dbname):
                    # 正在使用，则直接退出
                    logging.error('have same task used,please check or delete taskdb rerun')
                    continue
                # 初始化资料库表结构
                if not resetRepDB(dbname):
                    logging.error('reset RepDatabase Failed,plase check')
                    continue
                # 初始化资料库数据
                initRepDBData(dbname, tconfig['TASKCONFIG'])
                # 锁定数据库
                lockRepository(dbname, 1)
                # 初始化资料库成功
                cfg_runJob(dbname, d_param)
                lockRepository(dbname, 0)
                printJobStatus(dbname)
            else:
                logging.info('job:%s not found' % (d_param, ))
        elif d_cmd == '-resetdb':
            # 初始化资料库表结构
            if not resetRepDB(dbname):
                logging.error('reset RepDatabase Failed,plase check')
                continue
            initRepDBData(dbname, tconfig['TASKCONFIG'])
        elif d_cmd == '-getjobstatus':
            print getJobStatus(dbname, d_param)


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
        promt = 'Please input parameter:\niconfigfile[filename] option%s\nor input [quit] to exit\n' % (CMD_LIST, )
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


if __name__ == '__main__':
    main()
