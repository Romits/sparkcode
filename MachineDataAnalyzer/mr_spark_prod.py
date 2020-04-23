#This code is used to parse machine data and generate stats
#from pyspark.sql import SparkSession
from pyspark.sql import SQLContext, Row
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import count
from pyspark import SparkConf, SparkContext
import pandas as pd
import numpy as np
import time
import uuid
import copy 
import datetime
import sys
import math
from pyspark.sql.window import Window
import argparse

# This function is used to initialize spark and create Spark and SQLContext for 1.6

def initializeSpark(appName):
    #conf = SparkConf().setAppName(appName).setMaster("yarn-client")
    conf = SparkConf().setAppName(appName).setMaster("local")
    sc = SparkContext.getOrCreate(conf=conf)
    quiet_logs(sc)
    sqlCtx = HiveContext(sc)
    return sqlCtx

# This method controls level of logging in Spark

def quiet_logs( sc ):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getRootLogger().setLevel( logger.Level.ERROR )


# This function pulls data from Greenplum instance using JDBC connector
# Schema is applied on the data pulled

def getDataFromGPDB(url,tableName,sqlContext,startDate,endDate,inputFile,filterFile):
    #df = sqlContext.read.format("jdbc").options(url=url,dbtable=tableName,driver="org.postgresql.Driver").load()

    #df.registerTempTable("mrgesyslogdtl")

    syslogMRSchema = StructType([StructField('dummy',StringType(), True),
                     StructField('tarfilename', StringType(), True), 
                     StructField('inputfilename', StringType(), True), 
                     StructField('tar_systemid', StringType(), True), 
                     StructField('tar_date', StringType(), True),
                     StructField('tar_datetime', StringType(), True),
                     StructField('log_systemid',StringType(), True),
                     StructField('log_mln',StringType(), True),
                     StructField('log_usn',StringType(), True),
                     StructField('log_datetime',StringType(), True),
                     StructField('log_datetime_ts',StringType(), True),
                     StructField('log_timezone',StringType(), True),
                     StructField('msg_text',StringType(), True),
                     StructField('msg_code',StringType(), True),
                     StructField('msg_source',StringType(), True),
                     StructField('msg_time',StringType(), True),
                     StructField('msg_datetime',StringType(), True),
                     StructField('log_type',StringType(), True),
                     StructField('tag',StringType(), True),
                     StructField('time_seq',StringType(), True),
                     StructField('seq_num',StringType(), True),
                     StructField('format',StringType(), True),
                     StructField('view_level',StringType(), True),
                     StructField('suit_name',StringType(), True),
                     StructField('host_name',StringType(), True),
                     StructField('msg_date',StringType(), True),
                     StructField('dl_load_date',StringType(), True)])

    #sysLogData = sqlContext.sql(s"select * from mrgesyslogdtl where msg_date >= $startDate and msg_date < $endDate")
    #sysLogData = sqlContext.sql('select * from mrgesyslogdtl where msg_date >= "2016-01-01" and msg_date < "2016-02-01"')

    #sysLogDataFull = sqlContext.read.format("csv").option("header", "false").option("parserLib", "univocity").option("quote", "~").schema(syslogMRSchema).load(inputFile)
    #sysLogDataFull = sqlContext.read.format("csv").option("header", "false").option("parserLib", "univocity").schema(syslogMRSchema).load(inputFile)
    sysLogDataFull = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("parserLib", "univocity").option("quote","~").schema(syslogMRSchema).load(inputFile)
    #sysLogDataFull = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("parserLib", "univocity").option("treatEmptyValuesAsNulls", "true" ).option("quote", "~").schema(syslogMRSchema).load(inputFile).na.fill("NA")
    #sysLogDataFull = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("quote", "~").schema(syslogMRSchema).load(inputFile)
    sysidFilterSchema = StructType([StructField('sysid',StringType(), True)])
    sysidFilterDF = sqlContext.read.format("csv").option("header", "false").option("parseLib", "univocity").schema(sysidFilterSchema).load(filterFile)
    sysLogDataFull.cache()
    print sysLogDataFull.count()
    sysidFilterDF.cache()
    #sysidFilterDF.show()
    sysLogData = sysLogDataFull.join(sysidFilterDF, sysLogDataFull["tar_systemid"] == sysidFilterDF["sysid"])
    #print "Count after Joining"
    #print sysLogData.count()
    sysLogData = sysLogData.drop('sysid')
    # Add filter information for dates...
    outputData = sysLogData.filter((sysLogData['msg_date'] >= startDate) & (sysLogData['msg_date'] <= endDate))
    #outputData.printSchema()
    #print "Count after filtering for dates"
    #print outputData.count()
    #outputData.select('tar_systemid').show(5)
    #### To remove disabled filtering by sysid's and date
    outputData = sysLogDataFull

    return outputData

#This method is used to read the details of the various codes from the input files.


def get_codes(sparkSess, levelWeightFile, errorCodeActionFile):
    #create DF for reading levels

    mebefSchema = StructType([StructField('level',StringType(), True), StructField('wt',StringType(), True)])

    mebefDF = sparkSess.read.format("csv").option("header","true").schema(mebefSchema).load(levelWeightFile)
    mebefDF.cache()
    mebefDF.registerTempTable("mebefTable")
    #mebefDF.show()
    ecActionSchema = StructType([StructField('error_code',StringType(), True),
                     StructField('description', StringType(), True), 
                     StructField('action', StringType(), True), 
                     StructField('level', StringType(), True)])
    ecActionDF = sparkSess.read.format("csv").option("header","true").schema(ecActionSchema).load(errorCodeActionFile)

    #ecActionDF.printSchema()
    #ecActionDF.show()
    ecActionDF.registerTempTable("MRErrorCodeActionTable")
    stopCodeList = sparkSess.sql("Select error_code from MRErrorCodeActionTable where action IN ('Stop', 'Disable')")
    #stopCodeList.show()
    inpStopCodeList = stopCodeList.collect()
    startCodeList = sparkSess.sql("Select error_code from MRErrorCodeActionTable where action IN ('Start')")
    #startCodeList.show()
    inpStartCodeList = startCodeList.collect()
    monitorCodeList = sparkSess.sql("Select error_code from MRErrorCodeActionTable where action IN ('Start','Monitor','Stop','Enable','Disable')")
    #monitorCodeList.show()
    inpMonitorCodeList = monitorCodeList.collect()
    ignoreCodeList = sparkSess.sql("Select error_code from MRErrorCodeActionTable where action IN ('Ignore')")
    #ignoreCodeList.show()
    inpIgnoreCodeList = ignoreCodeList.collect()
    tpsResetStartedCodeList = sparkSess.sql("Select error_code from MRErrorCodeActionTable where description Like concat('%','TPS Reset Started','%')")
    #tpsResetStartedCodeList.show()
    inpTPSResetStartedCodeList = tpsResetStartedCodeList.collect()
    tpsResetSuccessfulCodeList = sparkSess.sql("Select error_code from MRErrorCodeActionTable where description Like concat('%','TPS Reset successful','%')")
    #tpsResetSuccessfulCodeList.show()
    inpTPSResetSuccessfulCodeList = tpsResetSuccessfulCodeList.collect()
    tpsResetFailedCodeList = sparkSess.sql("Select error_code from MRErrorCodeActionTable where description Like concat('%','TPS Reset failed','%')")
    #tpsResetFailedCodeList.show()
    inpTPSResetFailedCodeList = tpsResetFailedCodeList.collect()
    systemShutdownCodeList = sparkSess.sql("Select error_code from MRErrorCodeActionTable where description Like concat('%','Sytem shutdown','%')")
    #systemShutdownCodeList.show()
    inpSystemshutdownCodeList = systemShutdownCodeList.collect()
    serviceExamCodeList = sparkSess.sql("Select error_code from MRErrorCodeActionTable where description Like ' Service Exam%'")
    #serviceExamCodeList.show()
    inpServiceExamCodeList = serviceExamCodeList.collect()
    nonServiceExamCodeList = sparkSess.sql("Select error_code from MRErrorCodeActionTable where description Like concat('%','Non Service Exam','%')")
    #nonServiceExamCodeList.show()
    inpNonServiceExamCodeList = nonServiceExamCodeList.collect()

    MRActionTakenRow = Row("ActionTaken", "ListOfErrorCodes")
    MRActionTakenRow1 = Row(ActionTaken='Stop', ListOfErrorCodes=inpStopCodeList)
    MRActionTakenRow2 = Row(ActionTaken='Start', ListOfErrorCodes=inpStartCodeList)
    MRActionTakenRow3 = Row(ActionTaken='Monitor', ListOfErrorCodes=inpMonitorCodeList)
    MRActionTakenRow4 = Row(ActionTaken='Ignore', ListOfErrorCodes=inpIgnoreCodeList)
    MRActionTakenRow5 = Row(ActionTaken='TPSResetStarted', ListOfErrorCodes=inpTPSResetStartedCodeList)
    MRActionTakenRow6 = Row(ActionTaken='TPSResetSuccessful', ListOfErrorCodes=inpTPSResetSuccessfulCodeList)
    MRActionTakenRow7 = Row(ActionTaken='TPSResetFailed', ListOfErrorCodes=inpTPSResetFailedCodeList)
    MRActionTakenRow8 = Row(ActionTaken='Shutdown', ListOfErrorCodes=inpSystemshutdownCodeList)
    MRActionTakenRow9 = Row(ActionTaken='ServiceExam', ListOfErrorCodes=inpServiceExamCodeList)
    MRActionTakenRow10 = Row(ActionTaken='NonServiceExam', ListOfErrorCodes=inpNonServiceExamCodeList)
    MRActionTakeSeq = [MRActionTakenRow1,  MRActionTakenRow2, MRActionTakenRow3 , MRActionTakenRow4, MRActionTakenRow5, MRActionTakenRow6, MRActionTakenRow7, MRActionTakenRow8, MRActionTakenRow9,MRActionTakenRow10]

    MRDF = sparkSess.createDataFrame(MRActionTakeSeq)
    MRDF.cache()
    MRDF.registerTempTable("MRDFTable")
    return MRDF

def process_external_data(sparkSess, sysid, software_rev, config):
    #print sysid
    input_df_spark = sparkSess.sql("select * from MRGeSysLogData WHERE tar_systemid = '%s' order by float(time_seq) asc" %sysid)
    input_df = input_df_spark.toPandas()
    tempfinalStartCodes = sparkSess.sql("Select ListOfErrorCodes from MRDFTable where ActionTaken IN ('Start')")
    finalStartCodes = tempfinalStartCodes.collect()
    startCodesList =[]
    for errCode in finalStartCodes[0].ListOfErrorCodes:
        #print errCode.error_code
        startCodesList.append(errCode.error_code)
    tempfinalStopCodes = sparkSess.sql("Select ListOfErrorCodes from MRDFTable where ActionTaken IN ('Stop')")
    finalStopCodes = tempfinalStopCodes.collect()
    stopCodesList =[]
    for errCode in finalStopCodes[0].ListOfErrorCodes:
        #print errCode.error_code
        stopCodesList.append(errCode.error_code)
    monitorCodesList =[]
    tempfinalMonitorCodes = sparkSess.sql("Select ListOfErrorCodes from MRDFTable where ActionTaken IN ('Monitor')")
    finalMonitorCodes = tempfinalMonitorCodes.collect()
    for errCode in finalMonitorCodes[0].ListOfErrorCodes:
        #print errCode.error_code
        monitorCodesList.append(errCode.error_code)
    ignoreCodesList =[]
    tempfinalIgnoreCodes = sparkSess.sql("Select ListOfErrorCodes from MRDFTable where ActionTaken IN ('Ignore')")
    finalIgnoreCodes = tempfinalIgnoreCodes.collect()
    for errCode in finalIgnoreCodes[0].ListOfErrorCodes:
        #print errCode.error_code
        ignoreCodesList.append(errCode.error_code)
    tpsresetstartedCodesList =[]
    tempfinaltpsresetstartedCodes = sparkSess.sql("Select ListOfErrorCodes from MRDFTable where ActionTaken IN ('TPSResetStarted')")
    finaltpsresetstartedCodes = tempfinaltpsresetstartedCodes.collect()
    for errCode in finaltpsresetstartedCodes[0].ListOfErrorCodes:
        #print errCode.error_code
        tpsresetstartedCodesList.append(errCode.error_code)
    tpsresetsuccessfulCodesList =[]
    tempfinaltpsresetsuccessfulCodes = sparkSess.sql("Select ListOfErrorCodes from MRDFTable where ActionTaken IN ('TPSResetSuccessful')")
    finaltpsresetsuccessfulCodes = tempfinaltpsresetsuccessfulCodes.collect()
    for errCode in finaltpsresetstartedCodes[0].ListOfErrorCodes:
        #print errCode.error_code
        tpsresetsuccessfulCodesList.append(errCode.error_code)
    tpsresetfailedCodesList =[]
    tempfinaltpsresetfailedCodes = sparkSess.sql("Select ListOfErrorCodes from MRDFTable where ActionTaken IN ('TPSResetFailed')")
    finaltpsresetfailedCodes = tempfinaltpsresetfailedCodes.collect()
    for errCode in finaltpsresetfailedCodes[0].ListOfErrorCodes:
        #print errCode.error_code
        tpsresetfailedCodesList.append(errCode.error_code)
    shutdownCodesList =[]
    tempfinalshutdownCodes = sparkSess.sql("Select ListOfErrorCodes from MRDFTable where ActionTaken IN ('Shutdown')")
    finalshutdownCodes = tempfinalshutdownCodes.collect()
    for errCode in finalshutdownCodes[0].ListOfErrorCodes:
        #print errCode.error_code
        shutdownCodesList.append(errCode.error_code)
    serviceExamCodesList =[]
    tempfinalserviceExamCodes = sparkSess.sql("Select ListOfErrorCodes from MRDFTable where ActionTaken IN ('ServiceExam')")
    finalserviceExamCodes = tempfinalserviceExamCodes.collect()
    for errCode in finalserviceExamCodes[0].ListOfErrorCodes:
        #print errCode.error_code
        serviceExamCodesList.append(errCode.error_code)
    nonServiceExamCodesList =[]
    tempfinalnonServiceExamCodes = sparkSess.sql("Select ListOfErrorCodes from MRDFTable where ActionTaken IN ('NonServiceExam')")
    finalnonServiceExamCodes = tempfinalnonServiceExamCodes.collect()
    for errCode in finalnonServiceExamCodes[0].ListOfErrorCodes:
        #print errCode.error_code
        nonServiceExamCodesList.append(errCode.error_code)
    

    old_parameter_last_ticks = 0.0
    new_parameter_last_ticks = 0.0
    
    parse_record = False
    service_flag = False
    exam_id = ''

    offset = 0.0
    last_ticks = 0

    headers = ['system_id', 'sw_rev', 'file_name', 'log_local_ts','log_message_ts', 'code', 'exam_id', 'mebef_level', 'mebef_wt',
               'time_seq', 'source', 'psd', 'coil', 'scan', 'config', 'msg_text']
   
    tmp_lst = []
    exam_recs = []
    start_ticks = 0
    service_ticks = 0
    for idx, row in input_df.iterrows():
        tmp_dict = {}
        new_code = row['msg_code']
        ticks_temp = (row['time_seq'])
        try:
            ticks = float(ticks_temp)
        except ValueError:
            #print "Not a valid number"
            #print ticks_temp
            #print row['dummy']
            continue

        if new_code in serviceExamCodesList:
            service_flag = True
            service_ticks = ticks

        if new_code in startCodesList:
            if service_flag:
                if (math.fabs(ticks - service_ticks) < 5):
                    # this is a service exam
                    parse_record = False
                    exam_recs = []
                    continue
                if (math.fabs(ticks - service_ticks) > 30):
                    service_flag = False
                    service_ticks = 0
            parse_record = True
            exam_id = str(uuid.uuid1())
            start_ticks = ticks
        
            if len(exam_recs) > 0:
                tmp_recs = copy.deepcopy(exam_recs)
                tmp_lst = tmp_lst + tmp_recs
                exam_recs = []   

        if not parse_record:
            continue

        if parse_record:
            if new_code not in monitorCodesList:
                continue

            if (( ticks > (start_ticks + 3*3600))):
                if tmp_dict:
                    exam_recs.append(copy.deepcopy(tmp_dict))
                if len(exam_recs) > 0:
                    tmp_recs = copy.deepcopy(exam_recs)
                    tmp_lst = tmp_lst + tmp_recs
                exam_recs = []
                parse_record = False
                continue 
            if new_code in stopCodesList:
                tmp_dict['system_id'] = row['tar_systemid'] 
                tmp_dict['sw_rev']  = row['software_rev'] 
                tmp_dict['file_name'] = row['inputfilename'] 
                tmp_dict['log_local_ts'] = row['msg_time'] 
                tmp_dict['log_message_ts'] = row['msg_datetime'] 
                tmp_dict['source'] = row['msg_source'] 
                tmp_dict['msg_text'] = row['msg_text'] 
                tmp_dict['time_seq'] = row['time_seq'] 

                tmp_dict['code'] = new_code
                tmp_dict['exam_id'] = exam_id
                tmp_dict['config'] = row['config'] 


                tmp_dict['psd'] = 'psd'
                tmp_dict['coil'] = 'coil'
                tmp_dict['scan'] = 'scan'
                tmp_resultDF = sparkSess.sql("Select level from MRErrorCodeActionTable  where error_code = '%s'" %new_code)
                tmp_result = tmp_resultDF.collect()
                tmp_dict['mebef_level'] = -1
                tmp_dict['mebef_wt'] = -1.0
                if len(tmp_result) != 0:
                    tmp_dict['mebef_level'] = tmp_result[0]
                    tmp_qry_result = sparkSess.sql("select wt from mebefTable  WHERE level = '%s'" %tmp_result[0])
                    tmp_dict_result = tmp_qry_result.collect()
                    tmp_dict['mebef_wt'] = tmp_dict_result[0]
                exam_recs.append(copy.deepcopy(tmp_dict))
                tmp_recs = copy.deepcopy(exam_recs)
                tmp_lst = tmp_lst + tmp_recs
                exam_recs = []
                parse_record = False
                continue
            tmp_dict['system_id'] = row['tar_systemid']
            tmp_dict['sw_rev'] = row['software_rev']
            tmp_dict['file_name'] = row['inputfilename']
            tmp_dict['log_local_ts'] = row['msg_time']
            tmp_dict['log_message_ts'] = row['msg_datetime']
            tmp_dict['source'] = row['msg_source']
            tmp_dict['msg_text'] = row['msg_text']
            tmp_dict['time_seq'] = row['time_seq']
    
            tmp_dict['code'] = new_code
            tmp_dict['exam_id'] = exam_id
            tmp_dict['config'] = row['config']
    
            tmp_dict['psd'] = 'psd'
            tmp_dict['coil'] = 'coil'
            tmp_dict['scan'] = 'scan'
            tmp_result_qry = sparkSess.sql("Select level from MRErrorCodeActionTable  where error_code='%s'" %new_code) 
            tmp_result_qry.cache()
            tmp_result = tmp_result_qry.collect() 
            tmp_dict['mebef_level'] = -1
            tmp_dict['mebef_wt'] = -1.0
            if len(tmp_result) != 0:
               tmp_dict['mebef_level'] = tmp_result[0]
               tmp_dict_qry = sparkSess.sql("select wt from mebefTable  WHERE level = '%s'" %tmp_result[0])
               tmp_dict_result = tmp_dict_qry.collect()
               tmp_dict['mebef_wt'] = tmp_dict_result[0]
            exam_recs.append(copy.deepcopy(tmp_dict)) 

    return pd.DataFrame(tmp_lst)


def calc_sevone(sumipst, numshutdown):
    if numshutdown > 1:
        #return sumipst - 1.0
        return sumipst
    else:
        return sumipst

### Get Day information...

def compute_day(inputDF):
    updatedRDD = inputDF.rdd.map(lambda row: Row(row.__fields__ + ["day"])(row + (unicode(time.strftime(u"%Y%m%d",time.gmtime(float(row.time_seq)))),)))
    return updatedRDD

### UDF to Get and Add the day information

getDay_udf = udf(lambda time_seq: unicode(time.strftime(u"%Y%m%d",time.gmtime(float(time_seq)))))

#examDuration_udf = udf(lambda time_seq: max(time_seq) - min(time_seq)) 

getipst_udf = udf(lambda sum_shutdown_crashes: 1 if sum_shutdown_crashes > 0 else  0)

#actual_num_crashes = count(inputDF['code_new' == '200002352']).over(sev1WindowSpec)

checkshutdown_udf = udf(lambda errorCode: 1 if errorCode == '200002352' else 0)

checkcrashes_udf = udf(lambda errorCode: 1 if errorCode == '200002364' else 0)

calc_sevone_udf = udf(calc_sevone, IntegerType())

### Method to calculate and add the exam duration to the dataframe

def compute_exam_duration(inputDF):
    windowSpec = Window.partitionBy('system_id', 'sw_rev', 'config', 'exam_id').orderBy('time_seq').rowsBetween(-sys.maxsize, sys.maxsize)
    exam_dur_max = max("time_seq").over(windowSpec)
    exam_dur_min = min("time_seq").over(windowSpec)
    exam_dur_sec = exam_dur_max - exam_dur_min
    resultDF = inputDF.select(col("*"),
                        exam_dur_max.alias('exam_dur_max'),
                        exam_dur_min.alias('exam_dur_min'),
                        exam_dur_sec.alias('exam_dur_sec'))
    return resultDF

def rank_exam_duration(inputDF):
    windowSpec = Window.partitionBy('system_id', 'sw_rev', 'config', 'exam_id').orderBy(inputDF['exam_dur_sec'].desc())
    resultDF = (inputDF.withColumn('rank_val', rank().over(windowSpec))
                      .where(col('rank_val') == 1)
                      .select(col("*")))
    return resultDF




### Method used to calculate and add time_seq_lag column to the dataframe

def compute_timeseq_lag(inputDF):
    lagWindowSpec = Window.partitionBy('system_id', 'sw_rev', 'config', 'exam_id').orderBy('time_seq')
    time_seq_lag = lag('time_seq').over(lagWindowSpec) 

    #calResultDF = (inputDF.withColumn('row_num', rowNumber().over(lagWindowSpec))
    #                      .where(col('row_num') == 1)
    #                      .select(col("*"),
    #                      time_seq_lag.alias("time_seq_lag")))

    calResultDF = inputDF.select(col("*"),
                          time_seq_lag.alias("time_seq_lag"))
    return calResultDF

##This method sums the ipst values
def sum_ipst(inputDF):
    ipstWindowSpec = Window.partitionBy('exam_id').orderBy('time_seq').rowsBetween(-sys.maxsize, sys.maxsize)
    sum_ipst = sum(inputDF['ip_st']).over(ipstWindowSpec)
    calResultDF = inputDF.select(col("*"),
                          sum_ipst.alias("sum_ipst"))
    return calResultDF

### This method is used to calculate the total shutdown

def calc_total_shutdowns(inputDF):
    shutdownWindowSpec = Window.partitionBy('system_id', 'sw_rev', 'config').orderBy('time_seq').rowsBetween(-sys.maxsize, sys.maxsize)
    num_shutdowns = sum(inputDF['num_shutdowns_day']).over(shutdownWindowSpec)
    calResultDF = inputDF.select(col("*"),
                          num_shutdowns.alias("num_shutdowns"))
    return calResultDF

### Add additional stats to the Dataset

def get_final_stats(inputDF):
    finalStatsWindowSpec = Window.partitionBy('system_id', 'sw_rev', 'config').orderBy(inputDF['ipte_severity'].desc(),inputDF['system_id'].desc())
    calResultDF = (inputDF.withColumn('row_num', rowNumber().over(finalStatsWindowSpec))
                          .where(col('row_num') == 1)
                          .select(col("*"))
                  )
    return calResultDF

###Get final stats from the data...

def add_additional_stats(inputDF):
    addStatsWindowSpec = Window.partitionBy('system_id', 'sw_rev', 'config').orderBy('time_seq')
    #addStatsWindowSpec = Window.partitionBy('system_id', 'sw_rev', 'config').orderBy(inputDF['time_seq'].desc())
    num_tps_resets = count(inputDF['code_new' == '2227557']).over(addStatsWindowSpec)
    #time_seq_max = max(inputDF['time_seq']).over(addStatsWindowSpec)
    time_seq_max = count(inputDF['time_seq']).over(addStatsWindowSpec)
    #time_seq_min = min(inputDF['time_seq']).over(addStatsWindowSpec)
    time_seq_min = count(inputDF['time_seq']).over(addStatsWindowSpec)
    calResultDF = inputDF.select(col("*"),
                          num_tps_resets.alias("num_tps_resets"),
                          time_seq_min.alias("time_seq_min"),
                          time_seq_max.alias("time_seq_max"))
    return calResultDF


### This method is used to calculate the number of shutdown and crashes 
def calc_shutdown_and_crashes(inputDF):
    #sev1WindowSpec = Window.partitionBy('system_id', 'sw_rev', 'config', 'day','exam_id').orderBy('time_seq').rowsBetween(-sys.maxsize, sys.maxsize)
    sev1WindowSpec = Window.partitionBy('system_id', 'sw_rev', 'config', 'day','exam_id').orderBy('time_seq').rowsBetween(0,0)
    actual_num_shutdowns = count(inputDF['code_new' == '200002364']).over(sev1WindowSpec)
    actual_num_crashes = count(inputDF['code_new' == '200002352']).over(sev1WindowSpec)
    calResultDF = inputDF.select(col("*"),
                           actual_num_shutdowns.alias("actual_num_shutdowns"),
                           actual_num_crashes.alias("actual_num_crashes"))
    return calResultDF

### This method is used to calculate and add ipte
def compute_and_add_ipte(inputDF):
    ipteWindowSpec = Window.partitionBy('system_id', 'sw_rev', 'config').orderBy('time_seq')
    num_exams = count(inputDF['code_new' == '2218714']).over(ipteWindowSpec)
    num_sev1 = min(inputDF['num_shutdowns']).over(ipteWindowSpec)
    ipte_sev_1 = 1000.0*num_sev1/num_exams
    calResultDF = (inputDF.withColumn('row_num', rowNumber().over(ipteWindowSpec))
                          .where(col('row_num') == 1)
                          .select(col("*"),
                           num_exams.alias("num_exams"),
                           num_sev1.alias("num_sev1"),
                           ipte_sev_1.alias("ipte_severity")))
    return calResultDF

### This method is used to calculate and add weighted ipte
def compute_and_add_weighted_ipte(inputDF):
    iptewtWindowSpec = Window.partitionBy('system_id', 'sw_rev', 'config').orderBy('time_seq')
    wtWindowSpec = Window.partitionBy('system_id', 'sw_rev', 'config', 'exam_id').orderBy('time_seq')
    num_exams = count(inputDF['code_new' == '2218714']).over(iptewtWindowSpec)
    max_wt_per_exam = max(inputDF['mebef_wt_new.wt']).over(wtWindowSpec)
    sum_max_wt_all_exams = sum(max_wt_per_exam).over(iptewtWindowSpec)
    ipte_weighted = 1000.0*sum_max_wt_all_exams/num_exams
    calResultDF = (inputDF.withColumn('row_num', rowNumber().over(iptewtWindowSpec))
                           .where(col('row_num') == 1)
                           .select(col("*"),
                             #num_exams.alias("num_exams"),
                              sum_max_wt_all_exams.alias("sum_max_wt_all_exams"),
                              ipte_weighted.alias("ipte_weighted")))
    return calResultDF


##This method is used to get stats for the exams

def get_exam_stats(inputDF):
    tempDFless3 = inputDF.filter(inputDF['exam_dur_sec'] < 180.0)
    tempDFmore3 = inputDF.filter(inputDF['exam_dur_sec'] >= 180.0)
    print ("Exams with duration less than 3 min %d" %tempDFless3.count())
    print ("Exams with duration more than 3 min %d" %tempDFmore3.count())
    print ("Total Number of Exams %d" %inputDF.count())
    return tempDFmore3



# This function is used to transform the data pulled from GreenPlum.
# Also multiple csv files are read and error codes related to various states of
# a machine are pulled


def transform_and_process_data(inputDF,sparkSess,levelWeightFile,errorActionFile):
    #print inputDF.count()
    inputDF = inputDF.drop('dummy')  
    inputDF = inputDF.dropDuplicates()
    inputDF = inputDF.withColumn('config', lit('MR750W'))
    inputDF = inputDF.withColumn('software_rev', lit('DV25.0_R02_1549.b'))
    print " I am here for the new count"
    print inputDF.count()
    #inputDF.printSchema()
    dfKeys = ('tar_systemid', 'software_rev', 'config')
    subsetDF = inputDF.select('tar_systemid','software_rev','config').dropDuplicates(subset=dfKeys)
    inputDF.registerTempTable("MRGeSysLogData")
    #inputDF.createOrReplaceTempView("MRGeSysLogData").cache()
    codeDF = get_codes(sparkSess,levelWeightFile,errorActionFile)
    subsetDF.cache()
    unique_sysid_swrev_config = subsetDF.collect()
    compPandasDF = pd.DataFrame()
    for val in unique_sysid_swrev_config:
        sysid, software_rev,config = val
        returnPandasDF = process_external_data(sparkSess, sysid, software_rev, config)
        #print sysid
        #print len(returnPandasDF)
        compPandasDF = compPandasDF.append(returnPandasDF, ignore_index = True)
    dfKeys = ['system_id', 'sw_rev', 'code', 'time_seq']

    if len(compPandasDF.index) !=0:
        compPandasDF = compPandasDF.reset_index(drop=True)
        compSparkDF =  sparkSess.createDataFrame(compPandasDF)
        print "Before dropping duplicates"
        print("Number of records to process: %d " %compSparkDF.count())
        compSparkDF.printSchema()
        prodDF = compSparkDF.drop_duplicates(subset=dfKeys)
        print("Number of records to process: %d " %prodDF.count())
        ## Calculation for IPTE
        uniqueProdDF = prodDF.drop_duplicates()
        ## Remove rows with blank exam_id
        finalProdDF = uniqueProdDF.filter(uniqueProdDF.exam_id != '')
        print ("Number of 200002364 - User initiated shutdown codes:")
        print((finalProdDF.filter(finalProdDF['code'] == '200002364')).count())
        print ("Number of 200002352 codes:")
        print((finalProdDF.filter(finalProdDF['code'] == '200002352')).count())
        print ("Number of records with wt = 1:")
        print((finalProdDF.filter(finalProdDF['mebef_wt.wt'] == '1')).count())
        print "COUNT FOR FINAL PRODUCT DF"
        print finalProdDF.count()
        updatedDF = finalProdDF.withColumn("day", getDay_udf(finalProdDF.time_seq))
        #print "COUNT FOR UPDATED DF"
        #print updatedDF.count()
        updatedDFExamDuration = compute_exam_duration(updatedDF)
        updatedDFExamDuration.show(20)
        #updatedDFExamDuration = rank_exam_duration(tempDFExamDuration)
        #print "Count after getting RANK for Duration"
        #print updatedDFExamDuration.count()
        print("Count for UpdatedDF: %d" %updatedDF.select('exam_id').distinct().count())
        print("Count for UpdatedDFExamDuration: %d" %updatedDFExamDuration.select('exam_id').distinct().count())

        #updatedDFExamDuration.show(20)
        #updatedDFExamDuration.printSchema()
        ## Getting the exam details
        #returnDFmore3 = get_exam_stats(updatedDFExamDuration)
        updatedDFTSL = get_exam_stats(updatedDFExamDuration)
        print("Count for exams more than 3 min: %d" %updatedDFTSL.select('exam_id').distinct().count())
        print("Count for distict sites for records to process: %d" %updatedDFTSL.select('system_id').distinct().count())
        numOfDistinctSites = updatedDFTSL.select('system_id').distinct()
        #updatedDFTSL.show()

        ##### COMMENTED FOR TESTING...
        #updatedDFTSL = compute_timeseq_lag(updatedDFExamDuration)
        #updatedDFTSL.show(5)
        tempnewDFTSL = updatedDFTSL.withColumn('mebef_wt_new', updatedDFTSL.mebef_wt)
        newDFTSL = tempnewDFTSL.withColumn('code_new', updatedDFTSL.code)
        #newDFTSL.show(5)
        #shutdownCrashesDF = calc_shutdown_and_crashes(newDFTSL)
        shutdownDF = newDFTSL.withColumn('actual_num_shutdowns', checkshutdown_udf(newDFTSL.code_new))
        shutdownCrashesDF = shutdownDF.withColumn('actual_num_crashes', checkcrashes_udf(shutdownDF.code_new))
        shutdownCrashesDF.show()
        sumCrashesShutdownDF = shutdownCrashesDF.withColumn('sum_shutdown_crashes', (shutdownCrashesDF['actual_num_shutdowns'] + shutdownCrashesDF['actual_num_crashes'])) 
        sumCrashesShutdownDF.show()
        addIPSTDF = sumCrashesShutdownDF.withColumn("ip_st", getipst_udf(sumCrashesShutdownDF.sum_shutdown_crashes)) 
        #addIPSTDF.show(5)
        sumIPSTDF = sum_ipst(addIPSTDF)
        #sumIPSTDF.show(5)
        sevOneDF = sumIPSTDF.withColumn("num_shutdowns_day",when(sumIPSTDF['actual_num_shutdowns'] > 0,(sumIPSTDF['sum_ipst'] - 1)).otherwise(sumIPSTDF['sum_ipst']))
        #sevOneDF.show(5)
        totalShutdownDF = calc_total_shutdowns(sevOneDF)
        #totalShutdownDF.show(20)
        print ("Total Number of distinct Sites : %d" %totalShutdownDF.select("system_id").distinct().count())
        print ("Total Number of shutdown and crashes after applying subraction rule: %d" %totalShutdownDF.groupBy().sum('num_shutdowns').head()[0])
        #totalShutdownDF.printSchema()
        ipteDF = compute_and_add_ipte(totalShutdownDF)
        ipteweightedDF = compute_and_add_weighted_ipte(ipteDF)
        finalDF = add_additional_stats(ipteweightedDF)
        #finalDF.show(10)
        #### Sorting and Saving the data
        finalSortedDF = get_final_stats(finalDF)
        topIPTESevOneSites = finalSortedDF.select("system_id", "ipte_weighted", "ipte_severity", "num_shutdowns", "num_tps_resets", "num_exams", "time_seq_max", "time_seq_min")
        return topIPTESevOneSites
    else:
            print("Did not find any valid record")
            exit(0)
def calculate_cumulative_ipte(finalDS):
     num_sys = finalDS[finalDS.num_shutdowns > 0].count()
     cum_shutdowns = finalDS.groupBy('num_shutdowns').sum().head()[0]
     cum_exams = finalDS.groupBy('num_exams').sum().head()[0]
     cum_ipte = ((1000.0*cum_shutdowns)/cum_exams)
     print('CUMULATIVE IPTE %f' %cum_ipte)
     print ('NUM SYS %d' %num_sys)
     print ('CUM EXAMS %d' %cum_exams)
     print ('CUM SHUTDOWNS %d' %cum_shutdowns)


# Start of the MR Spark Code
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--appName', action="store", dest="appName")
    parser.add_argument('--startDate', action="store", dest="startDate")
    parser.add_argument('--endDate', action="store", dest="endDate")
    parser.add_argument('--inputFile', action="store", dest="inputFile")
    parser.add_argument('--sysidFilterFile', action="store", dest="sysidFilterFile")
    parser.add_argument('--levelWeightFile', action="store", dest="levelWeightFile")
    parser.add_argument('--errorCodeActionFile', action="store", dest="errorCodeActionFile")
    parser.add_argument('--outputFilePath', action="store", dest="outputFilePath")
    args = parser.parse_args()

    sqlCtx = initializeSpark(args.appName)
    #myaccum = sparkCtx.accumulator(0)
    ## It will be provided as input later...
    start_date = "2016-01-01"
    end_date = "2016-01-03"
    #spark.sqlCtx.udf.register("ProcessExternalData", process_external_data) 
    mrSysLogDataOrig = getDataFromGPDB("jdbc:postgresql://url/xxxx_data?user=xxxxxxxxx&password=xxxxxxxx","xxxxxx.mr_xxxxslog_dtl",sqlCtx,args.startDate,args.endDate,args.inputFile,args.sysidFilterFile)
    print "After getting data from GreenPlum"
    print mrSysLogDataOrig.count()
    ### test code
    uniqueDS = mrSysLogDataOrig.dropDuplicates()
    print "COUNT AFTER DROPPING DUPLICATES"
    print uniqueDS.count()
    mrSysLogData = uniqueDS.dropna(how='any', thresh=None, subset=None)

    print "Before Calling Transform Function"
    print mrSysLogData.count()
    ### Avoid Dropping na for tesring....and Check toRemove as needed.
    #mrSysLogData = mrSysLogDataOrig
    finalDS = transform_and_process_data(mrSysLogData,sqlCtx,args.levelWeightFile,args.errorCodeActionFile)
    #finalDS.show()
    curr_ts = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    outfile = args.outputFilePath + curr_ts + ".csv"
    finalDS.repartition(1).write.format('csv').save(outfile,header = 'false')
    calculate_cumulative_ipte(finalDS)

