import aenum
import collections
import logging.handlers
import multiprocessing
import orchestration
import os
import time

#=======================================================================================================================
# Used log levels
CRITICAL =  logging.CRITICAL
ERROR    =  logging.ERROR
WARNING  =  logging.WARNING
NOTICE   =  logging.INFO  + 5
INFO     =  logging.INFO
DEBUG1   =  logging.DEBUG
DEBUG2   =  logging.DEBUG - 2
DEBUG3   =  logging.DEBUG - 3
DEBUG4   =  logging.DEBUG - 4
DEBUG5   =  logging.DEBUG - 5
NOTSET   =  logging.NOTSET

#=======================================================================================================================
# Levels values by names
_LEVELS_VALUES_BY_NAMES = {'CR' : CRITICAL,
                           'E'  : ERROR,    
                           'W'  : WARNING,  
                           'N'  : NOTICE,   
                           'I'  : INFO,     
                           'D1' : DEBUG1,   
                           'D2' : DEBUG2,   
                           'D3' : DEBUG3,   
                           'D4' : DEBUG4,   
                           'D5' : DEBUG5,  
                           'NS' : NOTSET}

#=======================================================================================================================
class Logger(logging.Logger):
    
    __FORMAT_RECORD_DEFAULT = "LG: %(asctime)s.%(msecs)03d %(levelname)-4s [%(processName)s/%(identifier)s] %(message)s"
    __FORMAT_DATE_DEFAULT = "%Y%m%d-%H%M%S"
    
#-----------------------------------------------------------------------------------------------------------------------        
    def __init__(self, name, level=NOTSET):
        """ C'tor
        @see Base class
        """
        
        logging.Logger.__init__(self, name, level)
        
        for (levelName, level) in _LEVELS_VALUES_BY_NAMES.iteritems():
            logging.addLevelName(level, "<%s>" % levelName)

        self.__fileHandler = None
        self.__stderrHandler = logging.StreamHandler(logging.sys.stderr)
        self.__stderrHandler.setLevel(NOTICE)
        formatter = logging.Formatter(self.__FORMAT_RECORD_DEFAULT, self.__FORMAT_DATE_DEFAULT)
        self.__stderrHandler.setFormatter(formatter)
        self.addHandler(self.__stderrHandler)
        
#-----------------------------------------------------------------------------------------------------------------------        
    def exception(self, msg, *args, **kwargs):
        """ Overwriting in favor of contextual logging
        @see logging.exception()
        """
        
        kwargs['exc_info'] = 1
        self.log(CRITICAL, msg, *args, **kwargs)            

#-----------------------------------------------------------------------------------------------------------------------        
    def error(self, msg, *args, **kwargs):
        """ Overwriting in favor of contextual logging
        @see logging.error()
        """
        
        self.log(ERROR, msg, *args, **kwargs)  
        
#-----------------------------------------------------------------------------------------------------------------------        
    def warning(self, msg, *args, **kwargs):
        """ Overwriting in favor of contextual logging
        @see logging.warning()
        """
        
        self.log(WARNING, msg, *args, **kwargs)   
        
#-----------------------------------------------------------------------------------------------------------------------        
    def notice(self, msg, *args, **kwargs):
        """ Adding notice log level 
        """
        
        self.log(NOTICE, msg, *args, **kwargs)                   
        
#-----------------------------------------------------------------------------------------------------------------------        
    def info(self, msg, *args, **kwargs):
        """ Overwriting in favor of contextual logging
        @see logging.info()
        """
        
        self.log(INFO, msg, *args, **kwargs)             
 
#-----------------------------------------------------------------------------------------------------------------------        
    def debug1(self, msg, *args, **kwargs):
        """ Adding debug1 log level over debug
        @see logging.debug()
        """
        
        self.log(DEBUG1, msg, *args, **kwargs)   
        
#-----------------------------------------------------------------------------------------------------------------------        
    def debug2(self, msg, *args, **kwargs):
        """ Adding debug2 log level 
        """
        
        self.log(DEBUG2, msg, *args, **kwargs)   
        
#-----------------------------------------------------------------------------------------------------------------------        
    def debug3(self, msg, *args, **kwargs):
        """ Adding debug3 log level 
        """
        
        self.log(DEBUG3, msg, *args, **kwargs)   
        
#-----------------------------------------------------------------------------------------------------------------------        
    def debug4(self, msg, *args, **kwargs):
        """ Adding debug4 log level 
        """
        
        self.log(DEBUG4, msg, *args, **kwargs)   
        
#-----------------------------------------------------------------------------------------------------------------------        
    def debug5(self, msg, *args, **kwargs):
        """ Adding debug5 log level 
        """
        
        self.log(DEBUG5, msg, *args, **kwargs)                                   
        
#-----------------------------------------------------------------------------------------------------------------------
    def log(self, level, msg, *args, **kwargs):
        """
        @see Base class
        """
        
        #Always adding the 'extra' kwarg
        if "extra" not in kwargs:
            kwargs["extra"] = LoggerAdapter._EXTRA_DEFAULT_DICT

        try:       
            logging.Logger.log(self, level, msg, *args, **kwargs)
            
        except:
            # Well if we are connected to listener try again
            if self.__listenerHandler is not None:
                self._disconnectFromListener()
                logging.Logger.log(self, level, msg, *args, **kwargs)
                
            else:
                
                raise
  
#-----------------------------------------------------------------------------------------------------------------------        
    def createAdapter(self, identifier):
        """ Create a contextual logger adapter
        @param identifier: Contextual identifier to appear in log messages
        """
        
        return LoggerAdapter(self, identifier)
    
#-----------------------------------------------------------------------------------------------------------------------      
    def _configure(self, configuration):
        """ 
        Configures this instance
        @param configuration: Orchestration configuration object
        @see configuration.Configuration
        """
        
        self.__stderrHandler.setLevel(_LEVELS_VALUES_BY_NAMES[configuration.logStderrLevel])
        self.setLevel(_LEVELS_VALUES_BY_NAMES[configuration.logStderrLevel])    
        
        # Done here...
        if configuration.disableFileLogging == True:
            
            return
        
        # Handle the file handlers
        dirPath =  os.path.dirname(configuration.logFilePath)
        
        if not os.path.exists(dirPath):
            os.makedirs(dirPath)
            
        self.__fileHandler = logging.handlers.RotatingFileHandler(configuration.logFilePath,
                                                                         'a',
                                                                          configuration.logFileMaxSize,
                                                                          configuration.logFileMaxFiles)
        
        self.__fileHandler.setLevel(_LEVELS_VALUES_BY_NAMES[configuration.logFileLevel])
        self.__fileHandler.setFormatter(logging.Formatter(self.__FORMAT_RECORD_DEFAULT, self.__FORMAT_DATE_DEFAULT))
        self.addHandler(self.__fileHandler)
        self.setLevel(min(_LEVELS_VALUES_BY_NAMES[configuration.logStderrLevel],
                           _LEVELS_VALUES_BY_NAMES[configuration.logFileLevel]))    
    
#-----------------------------------------------------------------------------------------------------------------------            
    def _connectToListener(self, listenerHandler):
        """ Connect to the logger listener through a given listener handler
        @param listenerHandler: Log handler which sends log records to the listener
        @attention: Should be called within an execution context(e.g. Executer main loop or orchestrator context)
        """
        
        listenerHandler.connect()
        self.addHandler(listenerHandler)
        self.__listenerHandler = listenerHandler
        
        if self.__fileHandler:
            self.removeHandler(self.__fileHandler)
        
        if self.__stderrHandler:
            self.removeHandler(self.__stderrHandler)
    
#-----------------------------------------------------------------------------------------------------------------------        
    def _disconnectFromListener(self):
        """ Disconnect from logger listener and restore previous handlers
        @attention: Should be called within an execution context(e.g. Executer main loop or orchestrator context)
        """

        if self.__listenerHandler is not None:        
            self.__listenerHandler.disconnect()
            self.removeHandler(self.__listenerHandler)    
            self.__listenerHandler = None           
            
            if self.__fileHandler:
                self.addHandler(self.__fileHandler)
            
            if self.__stderrHandler:
                self.addHandler(self.__stderrHandler)
                
#=======================================================================================================================
class LoggerAdapter(logging.LoggerAdapter):

    _EXTRA_IDENTIFIER_KEY = "identifier"
    _EXTRA_DEFAULT_DICT = { _EXTRA_IDENTIFIER_KEY : "-"}

    def __init__(self, logger, identifier):
        """C'tor
        @param identifier: Contextual identifier to appear in log messages
        """
        
        logging.LoggerAdapter.__init__(self, logger, {self._EXTRA_IDENTIFIER_KEY : str(identifier)})
        self.__effectiveLevel = logger.getEffectiveLevel()

#-----------------------------------------------------------------------------------------------------------------------      
    def exception(self, msg, *args, **kwargs):
        """ Delegate an exception call to the underlying logger, after adding
        contextual information from this adapter instance.
        @see Logger.exception
        """        
        
        msg, kwargs = self.process(msg, kwargs)
        self.logger.exception(msg, *args, **kwargs)
        
#-----------------------------------------------------------------------------------------------------------------------      
    def error(self, msg, *args, **kwargs):
        """ Delegate a errord call to the underlying logger, after adding
        contextual information from this adapter instance.
        @see Logger.error
        """        
        
        msg, kwargs = self.process(msg, kwargs)
        self.logger.error(msg, *args, **kwargs)

#-----------------------------------------------------------------------------------------------------------------------      
    def warning(self, msg, *args, **kwargs):
        """ Delegate a warning call to the underlying logger, after adding
        contextual information from this adapter instance.
        @see Logger.warning
        """        
        
        msg, kwargs = self.process(msg, kwargs)
        self.logger.warning(msg, *args, **kwargs)
        
#-----------------------------------------------------------------------------------------------------------------------      
    def notice(self, msg, *args, **kwargs):
        """ Delegate a notice call to the underlying logger, after adding
        contextual information from this adapter instance.
        @see Logger.notice
        """        
        
        msg, kwargs = self.process(msg, kwargs)
        self.logger.notice(msg, *args, **kwargs)
        
#-----------------------------------------------------------------------------------------------------------------------      
    def info(self, msg, *args, **kwargs):
        """ Delegate a notice call to the underlying logger, after adding
        contextual information from this adapter instance.
        @see Logger.notice
        """        
        
        msg, kwargs = self.process(msg, kwargs)
        self.logger.info(msg, *args, **kwargs)        
        
#-----------------------------------------------------------------------------------------------------------------------          
    def debug1(self, msg, *args, **kwargs):
        """ Delegate a debug1 call to the underlying logger, after adding
        contextual information from this adapter instance.
        @see Logger.debug1
        """
        
        msg, kwargs = self.process(msg, kwargs)
        self.logger.debug1(msg, *args, **kwargs)
        
#-----------------------------------------------------------------------------------------------------------------------          
    def debug2(self, msg, *args, **kwargs):
        """
        Delegate a debug2 call to the underlying logger, after adding
        contextual information from this adapter instance.
        @see Logger.debug2        
        """
        
        msg, kwargs = self.process(msg, kwargs)
        self.logger.debug2(msg, *args, **kwargs)
                           
#-----------------------------------------------------------------------------------------------------------------------                            
    def debug3(self, msg, *args, **kwargs):
        """ Delegate a debug3 call to the underlying logger, after adding
        contextual information from this adapter instance.
        @see Logger.debug3        
        """
        
        msg, kwargs = self.process(msg, kwargs)
        self.logger.debug3(msg, *args, **kwargs)
                            
#-----------------------------------------------------------------------------------------------------------------------                            
    def debug4(self, msg, *args, **kwargs):
        """ Delegate a debug4 call to the underlying logger, after adding
        contextual information from this adapter instance.
        @see Logger.debug4   
        """
        
        msg, kwargs = self.process(msg, kwargs)
        self.logger.debug4(msg, *args, **kwargs)
                           
#-----------------------------------------------------------------------------------------------------------------------                            
    def debug5(self, msg, *args, **kwargs):
        """ Delegate a debug call to the underlying logger, after adding
        contextual information from this adapter instance.
        @see Logger.debug5
        """
        
        msg, kwargs = self.process(msg, kwargs)
        self.logger.debug5(msg, *args, **kwargs )
        
#-----------------------------------------------------------------------------------------------------------------------        
    def createAdapter(self, identifier):
        """ 
        @see Base class
        """
        
        return LoggerAdapter(self.logger, identifier)   

#-----------------------------------------------------------------------------------------------------------------------       
    def isEnabledFor(self, level):
        """ Overriding in favor of fast accessing since we are not changing log levels on runtime
        @see Base class
        """
        
        return level >= self.__effectiveLevel                                                                                                       

#=======================================================================================================================
class _Listener(object):
    
    UNIX_DOMAIN_SOCKET = "logging.sock"

#-----------------------------------------------------------------------------------------------------------------------      
    def __init__(self, logger, configuration):    
        """ C'tor
        @param logger: Parent logger
        @param configuration: Orchestration configuration
        @see configuration.Configuration        
        """
        
        self.__logger = logger.createAdapter("LoggerListener")
        self.__configuration = configuration
        self.__shouldRun = multiprocessing.Value('b', False)
        self.__wasLaunched = multiprocessing.Value('b', False)
        self.__process = multiprocessing.Process(target = self.__processTarget, name="logger-listener")
        self.__startSemaphore = multiprocessing.Semaphore()
        self.__unixDomainSocketPath = os.path.join(self.__configuration.runDirectoryPath, self.UNIX_DOMAIN_SOCKET)
        self.__queue = orchestration.queues._QueueZmq(logger, 0, "ingress-logger-listener", 
                                                      self.__unixDomainSocketPath,
                                                      orchestration.queues._SerializerPickle())
        
        self.__buffer = collections.deque()
        self.__lastFlushTime = time.time()

#-----------------------------------------------------------------------------------------------------------------------      
    @property        
    def isAlive(self):
        """ Is the logger listener alive
        """
        
        return self.__process.is_alive()
        
#-----------------------------------------------------------------------------------------------------------------------
    def createHandler(self, level=NOTSET):
        """ Create a handler
        @param level: Level to set for this handler 
        """
        return _ListenerHandler(orchestration.queues._QueueZmq(self.__logger, 
                                                               0, 
                                                               "egress-logger-listener", 
                                                               self.__unixDomainSocketPath, 
                                                               orchestration.queues._SerializerPickle()), 
                                level)
        
#-----------------------------------------------------------------------------------------------------------------------  
    def start(self):
        """ Start the logger listener
        """
        
        self.__logger.debug2("Starting logger listener")
        self.__shouldRun.value = True
        self.__process.start()
        
        # Acquire first to negate the semaphore and then try again till it is released
        self.__startSemaphore.acquire()
        self.__startSemaphore.acquire(True)
        
        if not self.__wasLaunched.value:
            self.__logger.error("Failed to launch logger listener")
            
            raise orchestration.QuietError()

#-----------------------------------------------------------------------------------------------------------------------  
    def stop(self):
        """ Stop the logger listener
        """
        if self.__wasLaunched.value:
            self.__logger.debug2("Stopping logger listener")
            self.__shouldRun.value = False
            self.__process.join()

#-----------------------------------------------------------------------------------------------------------------------        
    def __flush(self):
        """ Flush sort buffer
        """
        while self.__buffer:
            self.__logger.logger.handle(self.__buffer.popleft())
        
        self.__lastFlushTime = time.time()   
            
#-----------------------------------------------------------------------------------------------------------------------  
    def __processTarget(self):
        """ Logger listener's process target 
        """
        
        try:
            # Bind to queue and then release start semaphore
            self.__queue.bind()
            self.__wasLaunched.value = True
            self.__startSemaphore.release()
            self.__logger.debug2("Logger listener started")
            
            while self.__shouldRun.value:
                record = self.__queue.pop()
                
                if record is not None:
                   
                    # Buffering in favor of sorting creation time
                    self.__buffer.append(record)
                    self.__buffer = collections.deque(sorted(self.__buffer, key=lambda x : x.created))
                    
                    if len(self.__buffer) > self.__configuration.loggerListenerBufferSize:
                        self.__logger.logger.handle(self.__buffer.popleft())
                
                else:
                    time.sleep(self.__configuration.loggerListenerIdleTime)
                    
                # Flush if reached maximum time
                if time.time() - self.__lastFlushTime > self.__configuration.loggerListenerMaxBufferingTime:
                    self.__flush()
              
        except KeyboardInterrupt:
            pass
                        
        except:
            self.__logger.exception("Unexpected exception")   
            
        finally:
            
            if not self.__wasLaunched.value:
                self.__startSemaphore.release()
            
            self.__flush()
            self.__queue.unbind()
            self.__logger.debug2("Logger listener stopped")
            
#=======================================================================================================================
class _ListenerHandler(logging.Handler):

#-----------------------------------------------------------------------------------------------------------------------      
    def __init__(self, queue, level=NOTSET):
        """ C'tor
        @param level: Level to set for this handler
        @param queue: Listener queue 
        """
        
        logging.Handler.__init__(self, level)
        self.__queue = queue
        self.__exceptionFormatter = logging.Formatter()

#-----------------------------------------------------------------------------------------------------------------------          
    def connect(self):
        """ Connect to listener queue
        """
        
        self.__queue.connect()

#-----------------------------------------------------------------------------------------------------------------------          
    def disconnect(self):
        """ Disconnect from listener queue
        """
        
        self.__queue.disconnect()

#-----------------------------------------------------------------------------------------------------------------------      
    def emit(self, record):
        """ 
        @see Base class
        """
        
        # Making exception info pickleable :/
        if record.exc_info is not None:
            record.exc_text = self.__exceptionFormatter.formatException(record.exc_info)
            record.exc_info = None
            
        # Make sure to pass only built in types in args            
        for i in xrange(0, len(record.args)):
            if (not isinstance(record.args[i], (int, str, long, float, complex, unicode, bool, list, tuple, dict)) or
                isinstance(record.args[i], (aenum.Enum, aenum.Flag))):
                record.args[i] = "%s" %  (record.args[i],)
                        
        self.__queue.push(record)
        
#=======================================================================================================================
def _getTopLogger():
    """  top logger instance
    """
    return logging.getLogger('main')
