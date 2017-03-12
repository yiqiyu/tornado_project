import os,sys
import logging  
import logging.handlers 
loggerLevel = logging.INFO
logdir = "./logs"
loggerDict = {}


def createLogger(logName) :
    gLogger = logging.getLogger(logName)  
    logfile = logName +".logs"
    log_file = "%s/%s"%(logdir,logfile)  
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')  
#    handler = logging.handlers.RotatingFileHandler(log_file)
    handler = logging.FileHandler(log_file, mode='w')
    handler.setFormatter(formatter)  
    gLogger.addHandler(handler)  
    gLogger.setLevel(loggerLevel)  
    return gLogger  


def getLogger(logName) :
    if logName not in loggerDict:
        createLogger(logName)
        loggerDict[logName] = logging.getLogger(logName) 
    return logging.getLogger(logName)




