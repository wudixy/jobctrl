[TOC]

# Job Control Interface-JCI
##SUPPORT JOB TYPE
|JOBTYPE             |DESC1                                       |DESC2                                                                    |
|:-------------------|:-------------------------------------------|:------------------------------------------------------------------------|
|WAIT_DB_FLAG        | WAIT FLAG BY SQL                           |等待一个数据表标志位，通常用于等待一个标志位后执行后续作业               |
|SELECTDBSQL         |EXECUTE SELECT SQL STATEMENT         | 执行一条查询语句,并根据表达式判断成功失败，WAIT_DB_FLAG的最新实现方式，更灵活  |
|RUN_AR_TASK         |RUN ATTUNITY REPLICATE TASK                 |运行AR任务                                                               |
|GET_AR_TASK_STATUS  |GET ATTUNITY REPLICATE TASK STATUS|获取AR任务状态，返回[状态，开始时间，full load结束标志]，根据自定义表达式判断JOB状态|
|WAIT_AR_TASK        |WAIT ATTUNITY REPLICATE TASK STOPPED AND FULL LOAD COMPLETE| 等待AR任务停止，并fullload结束                           |
|DB_PROC             |EXECUTE DATABASE STORE PROCEDURE            | 执行存储过程，根据自定义表达式判断成功失败                              |
|SHELL               |EXECUTE SHELL OR BIN FILE                   | 执行脚本或可执行程序                                                    |
|GET_JOB_STATE       |GET JOB RUNNING STATUS AND RETURN MSG   | 获取当前TASK指定作业状态和返回值，通常用于根据前一个作业的返回值进行分支处理|

##TASK CONFIG
CONFIG FILE TYPE:JSON

### BASE CONFIG
|KEY               |  SAMPLE    |     MEMO                       |
|:-----------------|:----------:|:-------------------------------|
|TASKNAME          |    TEST    |    MUST STRING TYPE            |
|WAITSEC           |      5     |    循环等待时间                |
|MAXJOB            |      5     |    最大并发作业数              |
|LOGLEVEL          |    DEBUG   |    "DEBUG","INFO","ERROR"      |

```
  "TASKNAME":"TEST",
  "WAITSEC":5,
  "MAXJOB":5,
  "LOGLEVEL":"DEBUG",
```


### MAIL SERVER CONFIG
如果不需要发送邮件，可以不配置此属性
```
  "MAILCONFIG":{
       "MAILTO_LIST":["abc@mmmm.com"], 即使只有一个收件人，也需要[]，多个收件人使用逗号分割
       "MAIL_HOST":"smtp.xxxx.com",  发件服务器地址
       "MAIL_USER":"xxx@xxxx.com",  用户名
       "MAIL_PWD":"password",  密码
       "MAIL_POSTFIX":"xxxx.com"
  },
```

### TASK CONFIG
"TASKCONFIG":[{JOBINFO},{JOBINFO},{...}]

#### BASE JOB CONFIG
以下是一个JOB的基础属性
```
      {
        "JOBID":"作业编号，20位之内的字符串，在TASK所有JOB定义中，必须唯一",
        "DEP":"依赖关系，可依赖1个或多个作业运行成功后再运行当前JOB，例如 JOB1,JOB2",
        "DES":"描述信息",
        "JOBTYPE":"作业类型，必须是支持的JOBTYPE，例如DB_PROC,SHELL",
        "EXECUTE":"执行的动作，根据不同作业类型，含义不同",
        "PARAM":"参数，根据不同作业类型，类型和用法不同",
        "SENTMAIL":作业结束是否发送邮件，1：发送；0：不发送
        "LOOP":{"WAITSEC":2,"TIMEOUTCNT":5},  --可选，配置LOOP，意味这程序会循环调度这个作业，直到超时或程序成功
        "SUCESSEXPRESSION":"用于判断作业是否执行成功的表达式，如果没有此属性，作业只要执行并返回（没有异常），即认定成功，不同作业类型的表达式不同"
      }
```

#### DATABASE CONNECT CONFIG
对于数据库相关作业，需要配置对应的连接信息
```
"CONNECTINFO":{
            "DBID":"d1",  --暂时未使用
            "TYPE":"ORACLE",  --数据库类型，目前只支持ORACLE
            "TNS":"MY-DEV-203.2-TEST",  --连接字符串
            "USER":"test",     -- 用户名
            "PWD" :"test"      --密码
},
```

#### WAIT_DB_FLAG CONFIG
- PARAM必须包含
    - FLAG，用于和SQL的第一条记录的第一个字段进行对比，相等则等待成功
    - WAITSEC,每次等待的时间，单位秒
    - TIMEOUTCNT,等待的次数，超过次数则返回超时
- EXECUTE必须是一个可执行的数据库查询语句
- CONNECTINFO

```
      {
        "JOBID":"J_WAITFLAG", 
        "DEP":"",
        "DES":"等待flagtest标志位变为2",
        "JOBTYPE":"WAIT_DB_FLAG",
        "CONNECTINFO":{
            "DBID":"d1",
            "TYPE":"ORACLE",
            "TNS":"MY-DEV-203.2-TEST",
            "USER":"test",
            "PWD" :"test"},
        "EXECUTE":"select flag from flagtest where flag=2",
        "PARAM":{"FLAG":2,"WAITSEC":5,"TIMEOUTCNT":6},
        "SENTMAIL":1
      }
```

#### SELECTDBSQL CONFIG
- PARAM，无要求
- EXECUTE必须是一个可执行的数据库查询语句
- CONNECTINFO
- SUCESSEXPRESSION ，用于判断select 语句是否成功的表达式，例如rcd[0]==2,代表返回值的第一个字段等于2，可以使用任何PYTHON支持的语法实现此表达式。

```
      {
        "JOBID":"J_WAITFLAG2",
        "DES":"和J_WAITFLAG相同，改不同方式实现",
        "DEP":"",
        "JOBTYPE":"SELECTDBSQL",
        "CONNECTINFO":{
            "DBID":"d1",
            "TYPE":"ORACLE",
            "TNS":"MY-DEV-203.2-TEST",
            "USER":"test",
            "PWD" :"test"},
        "EXECUTE":"select flag from flagtest where flag=2",
        "PARAM":{},
        "SUCESSEXPRESSION":"rcd[0]==2",
        "LOOP":{"WAITSEC":2,"TIMEOUTCNT":5},
        "SENTMAIL":0
      }
```

#### RUN_AR_TASK
- PARAM，格式必须是{"OPERATION":1,"FLAGS":1}，其中各个子参数需参考AR说明

>operation:required enum, valid values:
        01 - EXECUTE_OPERATIONS_LOAD
        02 - EXECUTE_OPERATIONS_CDC
        03 - EXECUTE_OPERATIONS_BOTH

>flags:optional enum, valid values:
        00 - RESUME
        01 - FRESH
        02 - METADATA_ONLY
        03 - FRESH_METADATA_ONLY
        04 - COLLECTION

```
      {
        "JOBID":"J_RUNARTEST",
        "DEP":"J_WAITFLAG,J_WAITFLAG2",
        "DES":"运行Attunity Replicate task,test full load only",
        "JOBTYPE":"RUN_AR_TASK",
        "EXECUTE":"TEST",
        "PARAM":{"OPERATION":1,"FLAGS":1},
        "ARCONFIG":{
          "PRO_DIR": "D:/attunity/bin",
          "DATA_DIR":"D:/attunity/Data"
        },
        "SENTMAIL":0
      },
```

#### GET_AR_TASK_STATUS
- PARAM，此处无用
- EXECUTE ，填写TASK名，字符串类型
- SUCESSEXPRESSION: 正常RCD会返回一个字典格式的数据，包含state,full_load_completed,start_time,类型都是字符串，可以通过子定义表达式判断TASK状态，从而决定作业的状态，例如如下代码用于判断AR TASK当前状态是停止，并且FULL_LOAD结束
```rcd['state']=='STOPPED' and rcd['full_load_completed']=='True'```

```
      {
        "JOBID":"J_WAIT_ARTEST1",
        "DEP":"J_RUNARTEST",
        "DES":"等该AR TASK运行完成",
        "JOBTYPE":"GET_AR_TASK_STATUS",
        "EXECUTE":"TEST",
        "SUCESSEXPRESSION":"rcd['state']=='STOPPED' and rcd['full_load_completed']=='True'",
        "LOOP":{"WAITSEC":2,"TIMEOUTCNT":5},
        "PARAM":{},
        "ARCONFIG":{
          "PRO_DIR": "D:/attunity/bin",
          "DATA_DIR":"D:/attunity/Data"
        },
        "SENTMAIL":0
      },
```

#### WAIT_AR_TASK
- PARAM，必须是如下格式{"WAITSEC":2,"TIMEOUTCNT":5}
- EXECUTE，TASK名称，字符串类型

```
      {
        "JOBID":"J_WAIT_ARTEST2",
        "DEP":"J_RUNARTEST",
        "DES":"等该AR TASK运行完成,方式二",
        "JOBTYPE":"WAIT_AR_TASK",
        "EXECUTE":"TEST",
        "PARAM":{"WAITSEC":2,"TIMEOUTCNT":5},
        "ARCONFIG":{
          "PRO_DIR": "D:/attunity/bin",
          "DATA_DIR":"D:/attunity/Data"
        },
        "SENTMAIL":0
      },
```

#### DB_PROC
- EXECUTE,存储过程名
- PARAM，必须是列表类型[]，例如按照顺序和类型，填入存储过程参数
- SUCESSEXPRESSION，过程执行成功后会返回一个列表，是存储过程的返回值，可通过顺序编写表达式判断存储过程的实际状态，例如rcd[0]==2，代表第一个返回值等于2

```
      {
        "JOBID":"J4",
        "DEP":"J_WAIT_ARTEST1,J_WAIT_ARTEST2",
        "DES":"执行存储过程",
        "JOBTYPE":"DB_PROC",
        "DBID":"d1",
        "EXECUTE":"sp_t1",
        "CONNECTINFO":{
            "DBID":"d1",
            "TYPE":"ORACLE",
            "TNS":"MY-DEV-203.2-TEST",
            "USER":"test",
            "PWD" :"test"},
        "PARAM":[1,"1"],
        "SUCESSEXPRESSION":"rcd[0]==2",
        "SENTMAIL":0
      },
```

#### SHELL
- EXECUTE,脚本名称
- PARAM，字符串类型，例如需要传入两个参数，"parms1 params2"
- SUCESSEXPRESSION，可根据脚本返回值判断成功标志，注意，返回值即使是数值，判断时也需要当作字符串处理

```
      {
        "JOBID":"J5",
        "DEP":"",
        "DES":"执行脚本",
        "JOBTYPE":"SHELL",
        "EXECUTE":"tmp.cmd",
        "PARAM":"",
        "SUCESSEXPRESSION":"rcd=='0'",
        "SENTMAIL":0
      },
```

#### GET_JOB_STATE
一种内置作业类型，用于判断任务中某个作业的状态，通常可以使用这种类型进行分支判断，例如针对一个作业配置多个GET_JOB_STATE，每个GET_JOB_STATE的成功表达式不通，从而实现根据一个作业的不同返回值运行不同作业的配置

- EXECUTE,代表要判断哪个作业的状态
- SUCESSEXPRESSION，作业执行后返回一个字典类型的信息，包含status和msg两个字段，都是字符串，可自定义表达式判断，下面的例子代表J4的状态是FINISH，并且返回值的第一个字段（J4是一个存储过程，返回两个数值）等于2

```
      {
        "JOBID":"J6",
        "DEP":"J4",
        "JOBTYPE":"GET_JOB_STATE",
        "EXECUTE":"J4",
        "SUCESSEXPRESSION":"rcd['status']=='FINISH' and str(rcd['msg'].split(';')[0])=='2'",
        "SENTMAIL":0,
        "PARAM":""
      }
```

### SAMPLE
```
{
  "TASKNAME":"TEST",
  "WAITSEC":5,
  "MAXJOB":5,
  "LOGLEVEL":"DEBUG",
  "ARCONFIG":{
       "PRO_DIR": "D:/attunity/bin",
       "DATA_DIR":"D:/attunity/Data"
    },
  "ORACLECLIENT":"D:\\oracleClient64",
  "MAILCONFIG":{
       "MAILTO_LIST":["xxx@xxxx.com"],
       "MAIL_HOST":"smtp.xxxx.com",
       "MAIL_USER":"xxx@xxxx.com",
       "MAIL_PWD":"password",
       "MAIL_POSTFIX":"xxxx.com"
  },
  "TASKCONFIG":[
      {
        "JOBID":"J_WAITFLAG",
        "DEP":"",
        "DES":"等待flagtest标志位变为2",
        "JOBTYPE":"WAIT_DB_FLAG",
        "CONNECTINFO":{
            "DBID":"d1",
            "TYPE":"ORACLE",
            "TNS":"MY-DEV-203.2-TEST",
            "USER":"test",
            "PWD" :"test"},
        "EXECUTE":"select flag from flagtest where flag=2",
        "PARAM":{"FLAG":2,"WAITSEC":5,"TIMEOUTCNT":6},
        "SENTMAIL":1
      },
      {
        "JOBID":"J_WAITFLAG2",
        "DES":"和J_WAITFLAG相同，改不同方式实现",
        "DEP":"",
        "JOBTYPE":"SELECTDBSQL",
        "CONNECTINFO":{
            "DBID":"d1",
            "TYPE":"ORACLE",
            "TNS":"MY-DEV-203.2-TEST",
            "USER":"test",
            "PWD" :"test"},
        "EXECUTE":"select flag from flagtest where flag=2",
        "PARAM":{},
        "SUCESSEXPRESSION":"rcd[0]==2",
        "LOOP":{"WAITSEC":2,"TIMEOUTCNT":5},
        "SENTMAIL":0
      },
      {
        "JOBID":"J_RUNARTEST",
        "DEP":"J_WAITFLAG,J_WAITFLAG2",
        "DES":"运行Attunity Replicate task,test full load only",
        "JOBTYPE":"RUN_AR_TASK",
        "EXECUTE":"TEST",
        "PARAM":{"OPERATION":1,"FLAGS":1},
        "SENTMAIL":0
      },
      {
        "JOBID":"J_WAIT_ARTEST1",
        "DEP":"J_RUNARTEST",
        "DES":"等该AR TASK运行完成",
        "JOBTYPE":"GET_AR_TASK_STATUS",
        "EXECUTE":"TEST",
        "SUCESSEXPRESSION":"rcd['state']=='STOPPED' and rcd['full_load_completed']=='True'",
        "LOOP":{"WAITSEC":2,"TIMEOUTCNT":5},
        "PARAM":{},
        "SENTMAIL":0
      },
      
      {
        "JOBID":"J_WAIT_ARTEST2",
        "DEP":"J_RUNARTEST",
        "DES":"等该AR TASK运行完成,方式二",
        "JOBTYPE":"WAIT_AR_TASK",
        "EXECUTE":"TEST",
        "PARAM":{"WAITSEC":2,"TIMEOUTCNT":5},
        "SENTMAIL":0
      },
      {
        "JOBID":"J4",
        "DEP":"J_WAIT_ARTEST1,J_WAIT_ARTEST2",
        "DES":"执行存储过程",
        "JOBTYPE":"DB_PROC",
        "DBID":"d1",
        "EXECUTE":"sp_t1",
        "CONNECTINFO":{
            "DBID":"d1",
            "TYPE":"ORACLE",
            "TNS":"MY-DEV-203.2-TEST",
            "USER":"test",
            "PWD" :"test"},
        "PARAM":[1,"1"],
        "SUCESSEXPRESSION":"rcd[0]==2",
        "SENTMAIL":0
      },
      {
        "JOBID":"J5",
        "DEP":"",
        "DES":"执行脚本",
        "JOBTYPE":"SHELL",
        "EXECUTE":"tmp.cmd",
        "PARAM":"",
        "SUCESSEXPRESSION":"rcd=='0'",
        "SENTMAIL":0
      },
      {
        "JOBID":"J6",
        "DEP":"J4",
        "JOBTYPE":"GET_JOB_STATE",
        "EXECUTE":"J4",
        "SUCESSEXPRESSION":"rcd['status']=='FINISH' and str(rcd['msg'].split(';')[0])=='2'",
        "SENTMAIL":0,
        "PARAM":""
      }
    ]
}
```

##V2.1 CHANGE

**status:finish coding,start test and find bug**
**状态：编码结束，开始测试和除虫****

1. use class 
2. add new jobtype support 
    - SELECTDBSQL
    - GET_JOB_STATE
3. add loop params to loop run job 
4. log and runtime database path use script path

1. 使用class实现代码
2. 添加新的作业类型
    - SELECTDBSQL
    - GET_JOB_STATE
3.  增添loop参数，用于循环执行作业，通常可用于等待标志位或作业失败重做
4. 修改日志和运行时数据库的位置，使用脚本当前位置，而非执行程序当前路径


#Job Control Manager - JCM
- 类似crontab的调度

##jcm配置
统计目录下配置一个jcm.cfg文件

###jcm.cfg
```
{
  "PORT":6001,
  "IP":"127.0.0.1",
  "LOGLEVEL":"DEBUG",
  "SCHEDULER":[
        {
            "SNAME":"S1",
            "TASKCFG":"taskcfg/jcmt1.json",
            "YEAR":"",
            "YEARDES":"4-DIGIT YEAR",
            "MONTH":"",
            "MONTHDES": "(INT|STR) – MONTH (1-12)",
            "DAY":"",            
            "DAYDES":"(INT|STR) – DAY OF THE (1-31)",
            "WEEK":"",
            "WEEKDES":"(INT|STR) – ISO WEEK (1-53)",
            "DAY_OF_WEEK":"",
            "DAY_OF_WEEKDES":"(INT|STR) – NUMBER OR NAME OF WEEKDAY (0-6 OR MON,TUE,WED,THU,FRI,SAT,SUN)",
            "HOUR":"",    
            "HOURDES":" (INT|STR) – HOUR (0-23)",
            "MINUTE":"*/1",
            "MINUTEDES":" (INT|STR) – MINUTE (0-59)",
            "SECOND":"",
            "SECONDDES":" (INT|STR) – SECOND (0-59)",
            "START_DATE":"",
            "START_DATEDES":" (DATETIME|STR) – EARLIEST POSSIBLE DATE/TIME TO TRIGGER ON (INCLUSIVE)",
            "END_DATE":"",
            "END_DATE_DES":" (DATETIME|STR) – LATEST POSSIBLE DATE/TIME TO TRIGGER ON (INCLUSIVE)"
        },
        {
            "SNAME":"S2",
            "TASKCFG":"taskcfg/jcmt2.json",
            "YEAR":"",
            "MONTH":"",
            "DAY":"",            
            "WEEK":"",
            "DAY_OF_WEEK":"",
            "HOUR":"",    
            "MINUTE":"*/1",
            "SECOND":"",
            "START_DATE":"",
            "END_DATE":""
        }
  ]
}
```



