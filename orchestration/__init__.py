import atexit
import collections
import inspect
import multiprocessing
import argparse
import orchestration.timing
import orchestration.configuration
import orchestration.events
import orchestration.execution
import orchestration.logger
import orchestration.stats
import os
import re
import shutil
import sys
import signal
import time

#======================================================================================================================
# Register logger
orchestration.logger.logging.setLoggerClass(orchestration.logger.Logger)

#=======================================================================================================================
class Orchestrator(object):
    
#-----------------------------------------------------------------------------------------------------------------------            
    def __init__(self, logger):
        """
        C'tor
        @param logger: Parent logger
        """
        
        self.__logger = logger.createAdapter("Orchestrator")
        self.__configuration = None
        self.__executers = []
        self.__loggerListener = None
        self.__statsCollector = None
        
#-----------------------------------------------------------------------------------------------------------------------
    def createExecuter(self, name = ""):
        """ Create an executer to register events handlers to
        @param name: Optional name for the executer
        @see execution.Executer
        @return A new executer to register events handlers to
        """
        
        if not name:
            name = "executer-%s" % len(self.__executers)
            
        executerId = len(self.__executers)
        executer = orchestration.execution.Executer(self.__logger, self.__loggerListener, self.__statsCollector, 
                                                    executerId, name, self.__eventsBus, self.__configuration)
        
        self.__executers.append(executer)
        
        return executer
    
#-----------------------------------------------------------------------------------------------------------------------             
    def _createConfigurationBuilder(self):
        """ Create a configuration builder specified for an orchestrator
        @return Orchestrator configuration builder
        @see configuration.Builder
        """ 
        
        fields = [orchestration.configuration.Field(
                    "userConfigurationFile",
                    "-u",
                    "--user-configuration-file", 
                    "user_configuration_file", 
                    str, 
                    "Path to user configuration file",
                    "PATH"),
            
                  orchestration.configuration.Field(
                    "logStderrLevel",
                    "-l",
                    "--log-stderr-level", 
                    "log_stderr_level", 
                    str, 
                    "Log level to use when logger to STDERR",
                    "{%s}" % '|'.join(orchestration.logger._LEVELS_VALUES_BY_NAMES.keys())),
                  
                  orchestration.configuration.Field(
                    "logFileLevel",
                    "",
                    "--log-file-level", 
                    "log_file_level", 
                    str, 
                    "Log level to use when logger to a file",
                    "{%s}" % '|'.join(orchestration.logger._LEVELS_VALUES_BY_NAMES.keys())),     
                  
                  orchestration.configuration.Field(
                    "logFilePath",
                    "",
                    "--log-file-path", 
                    "log_file_path", 
                    str, 
                    "Full log file path",
                    "PATH"),
                  
                  orchestration.configuration.Field(
                    "logFileMaxSize",
                    "",
                    "--log-file-max-size", 
                    "log_file_max_size", 
                    int, 
                    "Maximum log file size in bytes before rotating",
                    "BYTES"),          
                  
                  orchestration.configuration.Field(
                    "logFileMaxFiles",
                    "",
                    "--log-file-max-files", 
                    "log_file_max_files", 
                    int, 
                    "Maximum amount of log files before rotating out",
                    "FILES"),                            
            
                  orchestration.configuration.Field(
                    "disableFileLogging",
                    "",
                    "--disable-file-logging", 
                    "disable_file_logging", 
                    bool, 
                    "Disable logging to a file",
                    ""),               
            
                  orchestration.configuration.Field(
                    "runDirectoryPath",
                    "",
                    "--run-directory-path", 
                    "run_directory_path", 
                    str, 
                    "Path to a directory which will hold runtime data",
                    "PATH"),
                  
                  orchestration.configuration.Field(
                    "lockFilePath",
                    "",
                    "--lock-file-path", 
                    "lock_file_path", 
                    str, 
                    "Lock file path",
                    "PATH"),                  
                  
                  orchestration.configuration.Field(
                    "eventsBusConnectionTimeout", 
                    "", 
                    "--events-bus-connection-timeout", 
                    "events_bus_connection_timeout", 
                    int, 
                    "Timeout in seconds for connecting to the event bus", 
                    "SECONDS"),
                  
                  orchestration.configuration.Field(
                    "executersStateMoveTimeout", 
                    "", 
                    "--executers-state-move-timeout", 
                    "executers_state_move_timeout", 
                    int, 
                    "Timeout in seconds for executers to move to states in startup and shutdown sequences", 
                    "SECONDS"),
                  
                orchestration.configuration.Field(
                    "executersIdleTime", 
                    "", 
                    "--executers-idle-time", 
                    "executers_idle_time", 
                    float, 
                    "Idle time in seconds of an executers between event polls when there are no events to handle " + 
                    "(High time will decrease CPU utilization but will increase latency)",
                    "SECONDS"),
                  
                orchestration.configuration.Field(
                    "executersLoggerListenerDisconnectionTimeout", 
                    "", 
                    "--executers-logger-listener-disconnection-timeout", 
                    "executers_logger_listener_disconnection_timeout", 
                    int, 
                    "Maximum time in seconds to wait until all logger records were sent to from executers to logger "+
                    "listener in shutdown sequence",
                    "SECONDS"),                       
                  
                orchestration.configuration.Field(
                    "orchestratorIdleTime", 
                    "", 
                    "--orchestrator-idle-time", 
                    "orchestrator_idle_time", 
                    float, 
                    "Idle time in seconds of the orchestrator between event polls when there are no events to handle " + 
                    "(High time will decrease CPU utilization but will increase latency)",
                    "SECONDS"), 
                  
                orchestration.configuration.Field(
                    "loggerListenerIdleTime", 
                    "", 
                    "--logger-listener-idle-time", 
                    "logger_listener_idle_time", 
                    float, 
                    "Idle time in seconds of the logger listener between record polls when there are no records to " + 
                    "handle (High time will decrease CPU utilization but will increase latency)",
                    "SECONDS"),
                  
                orchestration.configuration.Field(
                    "loggerListenerBufferSize", 
                    "", 
                    "--logger-listener-buffer-size", 
                    "logger_listener_buffer_size", 
                    int, 
                    "Size in records of logger listener buffer (High number will sort records more accurately but" +
                    "will increase memory usage and logger latency)",
                    "RECORDS"),
                  
                orchestration.configuration.Field(
                    "loggerListenerMaxBufferingTime", 
                    "", 
                    "--logger-listener-max-buffering-time", 
                    "logger_listener_max_buffering_time", 
                    int, 
                    "Maximum time in seconds for a record to reside in the logger listener buffer (High number " +
                    "will sort records more accurately but will increase memory usage and logger latency)",
                    "SECONDS"),
                  
                orchestration.configuration.Field(
                    "loggerListenerDisconnectionTimeout", 
                    "", 
                    "--logger-listener-disconnection-timeout", 
                    "logger_listener_disconnection_timeout", 
                    int, 
                    "Time in seconds to wait until all logger records were sent from orchestrator to logger "+
                    "listener before disconnection",
                    "SECONDS"),                     
                  
                orchestration.configuration.Field(
                    "disableStats", 
                    "", 
                    "--disable-stats", 
                    "disable_stats", 
                    bool, 
                    "Disable counters collecting and publication to Influxdb",
                    ""),                              
                  
                orchestration.configuration.Field(
                    "countersContainersStatsCollectorPublishInterval", 
                    "", 
                    "--counters-containers-stats-collector-publish-interval", 
                    "counters_containers_stats_collector_publish_interval", 
                    int, 
                    "Interval in seconds in which counters within counters containers will be published to stats " +
                    "collector (High time will decrease CPU utilization but will also decrease counters' time " +
                    "granularity)",
                    "SECONDS"),
                  
                orchestration.configuration.Field(
                    "statsCollectorInfluxdbPublishInterval", 
                    "", 
                    "--stats-collector-influxdb-publish-interval", 
                    "stats_collector_influxdb_publish_interval", 
                    int, 
                    "Interval in seconds in which counters within stats collector will be published to Influxdb" +
                    "(High time will decrease CPU utilization and load on Influxdb server but will also decrease " + 
                    "counters' time granularity",
                    "SECONDS"),
                  
                orchestration.configuration.Field(
                    "statsCollectorIdleTime", 
                    "", 
                    "--stats-collector-idle-time", 
                    "stats_collector_idle_time", 
                    float, 
                    "Idle time in seconds of the stats collector between data points polls when there are no data " + 
                    "points to handle (High time will decrease CPU utilization but will increase latency)",
                    "SECONDS"),                  
                  
                orchestration.configuration.Field(
                    "influxdb", 
                    "", 
                    "--influxdb", 
                    "influxdb", 
                    str, 
                    "Connection details to Influxdb in the following structure: " + 
                    "<Database name>@<Hostname or IP address>:<TCP port>",
                    "DATABASE@HOSTNAME:PORT"), 
                  
                orchestration.configuration.Field(
                    "influxdbMeasurementHistoryMaxSize", 
                    "", 
                    "--influxdb-measurement-history-max-size", 
                    "influxdb_measurement_history_max_size", 
                    int, 
                    "Maximum amount of past measurements to retain before removing oldest "+
                    "(By the date in 'inception' tag)",
                    "HISTORY_SIZE"),  
                  
                orchestration.configuration.Field(
                    "profile", 
                    "", 
                    "--profile", 
                    "profile", 
                    bool, 
                    "Should run with a profiler",
                    ""),                           
                  
                orchestration.configuration.Field(
                    "profilingDirectoryPath", 
                    "", 
                    "--profiling-directory-path", 
                    "profiling_directory_path", 
                    str, 
                    "Path to a directory used to contain profiling results data",
                    "PATH")                      
        ]
        
        class ValidatorOrchestrator(orchestration.configuration.ValidatorBase):
            
                def validate(self, configuration):
                    """
                    @see Base class
                    """
                    
                    if not configuration.logStderrLevel:
                        self.logger.error("Log STDERR level must be set")
                        
                        return False   
                    
                    if configuration.logStderrLevel not in orchestration.logger._LEVELS_VALUES_BY_NAMES:
                        self.logger.error("Set log STDERR level is invalid: '%s", configuration.logStderrLevel)
                        
                        return False      
                    
                    if not configuration.logFileLevel:
                        self.logger.error("Log file level must be set")
                        
                        return False   
                    
                    if configuration.logFileLevel not in orchestration.logger._LEVELS_VALUES_BY_NAMES:
                        self.logger.error("Set log file level is invalid: '%s", configuration.logFileLevel)
                        
                        return False    
                    
                    if not configuration.disableFileLogging and not configuration.logFilePath:
                        self.logger.error("Log file path must be set")
                        
                        return False  
                    
                    if  configuration.logFileMaxSize is None:
                        self.logger.error("Log file max size must be set")
                        
                        return False                      
                    
                    if configuration.logFileMaxSize < 0:
                        self.logger.error("Set log file max size is negative: '%s'",
                                          configuration.logFileMaxSize)
                        
                        return False
                    
                    if  configuration.logFileMaxFiles is None:
                        self.logger.error("Log file max files must be set")
                        
                        return False                      
                    
                    if configuration.logFileMaxFiles < 0:
                        self.logger.error("Set log file max files is negative: '%s'",
                                          configuration.logFileMaxSize)
                        
                        return False                    
                    
                    if not configuration.runDirectoryPath:
                        self.logger.error("Run directory path must be set")
                        
                        return False
                    
                    if not configuration.lockFilePath:
                        self.logger.error("Lock file path must be set")
                        
                        return False                    
                    
                    if configuration.eventsBusConnectionTimeout is None:
                        self.logger.error("Event bus connection timeout must be set") 
                        
                        return False               
                        
                    if configuration.eventsBusConnectionTimeout < 0:
                        self.logger.error("Set event bus connection timeout is negative: '%s'",
                                          configuration.eventsBusConnectionTimeout)
                        
                        return False
                    
                    if configuration.executersStateMoveTimeout is None:
                        self.logger.error("Executers startup sequence state move timeout must be set")
                        
                        return False                
                    
                    if configuration.executersStateMoveTimeout < 0:
                        self.logger.error("Set executers startup sequence state move timeout is negative: '%s'",  
                                          configuration.executersStateMoveTimeout)
                        
                        return False    
                    
                    
                    if configuration.executersIdleTime is None:
                        self.logger.error("Executers idle time must be set")
                        
                        return False                    
                    
                    if configuration.executersIdleTime < 0:
                        self.logger.error("Set executers idle time is negative: '%s'", 
                                          configuration.executersIdleTime)
                        
                        return False     
                    
                    if configuration.loggerListenerIdleTime is None:
                        self.logger.error("Logger listener idle time must be set")
                        
                        return False                   
                    
                    if configuration.loggerListenerIdleTime < 0:
                        self.logger.error("Set logger listener idle time is negative: '%s'", 
                                          configuration.loggerListenerIdleTime)
                        
                        return False 
                    
                    if configuration.loggerListenerBufferSize is None:
                        self.logger.error("Logger listener buffer size must be set")
                        
                        return False                      
                    
                    if not configuration.loggerListenerBufferSize > 0:
                        self.logger.error("Set logger buffer size is not positive: '%s'",
                                          configuration.loggerListenerBufferSize)
                        
                        return False  
                    
                    if configuration.loggerListenerMaxBufferingTime is None:
                        self.logger.error("Logger listener max buffering time must be set")
                        
                        return False                 
                    
                    if not configuration.loggerListenerMaxBufferingTime > 0:
                        self.logger.error("Set logger listener max buffering time is not positive: '%s'",
                                          configuration.loggerListenerMaxBufferingTime)
                        
                        return False
                    
                    if configuration.loggerListenerDisconnectionTimeout is None:
                        self.logger.error("Logger listener disconnection timeout must be set")
                        
                        return False                 
                    
                    if not configuration.loggerListenerDisconnectionTimeout >= 0:
                        self.logger.error("Logger listener disconnection timeout is negative: '%s'", 
                                          configuration.loggerListenerDisconnectionTimeout)
                        
                        return False    
                    
                    if not configuration.disableStats:
                    
                        if configuration.countersContainersStatsCollectorPublishInterval is None:
                            self.logger.error("Counters containers stats collector publish interval must be set")
                            
                            return False                    
                    
                        if not configuration.countersContainersStatsCollectorPublishInterval > 0:
                            self.logger.error("Set counters containers stats collector publish interval is not " + 
                                         "positive: '%s'",
                                         configuration.countersContainersStatsCollectorPublishInterval)
                            
                            return False
                        
                        if configuration.statsCollectorIdleTime is None:
                            self.logger.error("Stats collector idle time must be set")
                            
                            return False                   
                    
                        if configuration.statsCollectorIdleTime < 0:
                            self.logger.error("Stats collector idle time is negative: '%s'", 
                                              configuration.statsCollectorIdleTime)
                            
                            return False                         
                        
                        if configuration.statsCollectorInfluxdbPublishInterval is None:
                            self.logger.error("Stats collector influxdb publish interval must be set")
                            
                            return False                             
                    
                        if not configuration.statsCollectorInfluxdbPublishInterval > 0:
                            self.logger.error("Set stats collector influxdb publish interval is not positive: '%s'",
                                              configuration.statsCollectorInfluxdbPublishInterval)
                            
                            return False                                  
                        
                        if not configuration.influxdb:
                            self.logger.error("Influxdb must be set")
                            
                            return False                                                                                                                                                                      
                            
                        matcher = re.compile("\S+@\S+:\d+")
                        
                        if not matcher.match(configuration.influxdb):
                            self.logger.error("Set influxdb details are invalid: '%s'", configuration.influxdb)
                        
                            return False
                        
                        if not configuration.influxdbMeasurementHistoryMaxSize:
                            self.logger.error("Influxdb measurement history max size must be set")
                            
                            return False    
                        
                        if configuration.influxdbMeasurementHistoryMaxSize < 0:
                            self.logger.error("Set influxdb measurement history max is negative: '%s" % 
                                              configuration.influxdbMeasurementHistoryMaxSize)
                            
                            return False 
                        
                        if configuration.profile:

                            if configuration.profilingDirectoryPath is None:
                                self.logger.error("Profiling directory path must be set")
                                
                                return False                              
                        
                    return True   
                    
        return orchestration.configuration.Builder("Orchestration", 'orchestration', ValidatorOrchestrator(), fields)  
    
#-----------------------------------------------------------------------------------------------------------------------
    def _configure(self, configuration):
        """ Configure and prepare for running
        @param configuration: Orchestration configuration        
        """
        
        self.__configuration = configuration
        self.__eventsBus = orchestration.events._Bus(self.__logger, configuration)
        self.__loggerListener = orchestration.logger._Listener(self.__logger, self.__configuration)
        self.__statsCollector = orchestration.stats._Collector(self.__logger, self.__loggerListener, 
                                                               self.__configuration)

#-----------------------------------------------------------------------------------------------------------------------
    def _start(self):
        """ Start orchestration
        """
        
        try:
            os.open(self.__configuration.lockFilePath, os.O_CREAT | os.O_EXCL)
            atexit.register(lambda: os.remove(self.__configuration.lockFilePath))
            
        except OSError:
            self.__logger.error("Another orchestration is being performed currently, can not start another one")
             
            raise QuietError()
        
        try:
            # Starting log listener
            self.__loggerListener.start()
            
            # Starting stats collector
            self.__statsCollector.start()
            
            # Start all of the executers
            self.__logger.notice("Starting orchestration...")            
           
            for executer in self.__executers:
                executer._start()
            
            # Wait for them to get separated (or forked...)
            self.__waitForExecutersStartupSequenceState(orchestration.execution._ExecuterStatesE.SEPERATED)
            
            # Connecting to logger 
            self.__logger.logger._connectToListener(self.__loggerListener.createHandler())
            
            # Wait for them to start completely and start orchestration routines
            self.__waitForExecutersStartupSequenceState(orchestration.execution._ExecuterStatesE.STARTED)        
            self.__logger.notice("Started!")     
        
        except QuietError:
            
            raise
        
        except:
            self.__logger.exception("Unexpected exception")
            
            raise

#-----------------------------------------------------------------------------------------------------------------------
    def _poll(self):
        """ Poll orchestratration routines
        """   

        # Check executers are alive
        for executer in self.__executers:
            if executer._state == orchestration.execution._ExecuterStatesE.STOPPED:
                self.__logger.error("Executer is down: executer=%s", executer)
                
                raise QuietError()
            
        if not self.__loggerListener.isAlive:
            self.__logger.logger._disconnectFromListener()
            self.__logger.error("Log listener is down")

            raise QuietError()
        
        if not self.__statsCollector.isAlive:
            self.__logger.error("Stats collector is down")
            raise QuietError()        
            
        time.sleep(self.__configuration.orchestratorIdleTime)
        
#-----------------------------------------------------------------------------------------------------------------------
    def _stop(self):
        """ Stop orchestration
        """

        # Stopping orchestration routines and executers
        self.__logger.notice("Stopping orchestration...")

        for executer in self.__executers:
            executer._stop()
            
        for executer in self.__executers:
            executer._collect()                
            
        # Stop stats collector
        self.__statsCollector.stop()           
        
        # Disconnect and stop logger listener
        time.sleep(self.__configuration.loggerListenerDisconnectionTimeout)            
        self.__logger.logger._disconnectFromListener()
        self.__loggerListener.stop()
        self.__logger.notice("Stopped!")       

#-----------------------------------------------------------------------------------------------------------------------        
    def __waitForExecutersStartupSequenceState(self, state):
        """ Wait for all executers to move to a certain state within the startup sequence
        @param state: State executers should move to
        """
        
        self.__logger.debug2("Waiting for executers to move to state: state=%s", state)
        timeout = self.__configuration.executersStateMoveTimeout
        waitedExecutersIds = set([executer.id for executer in self.__executers])
        
        # As long as there are still waited executers
        while waitedExecutersIds:
            self.__logger.debug3("Waiting for executers to move to state: state=%s, executers=%s", state, 
                                 waitedExecutersIds)               
            
            # Scan all executers and remove the identifiers of executers which moved
            for executer in self.__executers:
                
                # State had changed? Remove from the set
                if executer._state >= state:
                    waitedExecutersIds.discard(executer.id)
            
            # Got some more timeout and set is not empty 
            if timeout > 0 and waitedExecutersIds:
                time.sleep(1)
                timeout -= 1
                
            elif waitedExecutersIds:
                self.__logger.error("Following executers failed to move to state: state=%s executers=%s", state, 
                                    waitedExecutersIds)
            
                raise QuietError()    
            
#=======================================================================================================================
class JobBase(object):
        
#-----------------------------------------------------------------------------------------------------------------------        
    def __init__(self, name):
        """ C'tor
        @param name: Job's name
        """
        
        self.__name = name 
        
        # Top object, create logger directly from main logger
        self.__logger = orchestration.logger._getTopLogger().createAdapter(name)
        
#-----------------------------------------------------------------------------------------------------------------------    
    @property
    def logger(self): 
        """ Job's logger
        """
        
        return self.__logger        

#-----------------------------------------------------------------------------------------------------------------------        
    @property
    def name(self):
        """ Job's name
        """
        
        return self.__name

#-----------------------------------------------------------------------------------------------------------------------       
    def createConfigurationBuilder(self):
        """ Create a configuration builder object for specific job configuration
        @attention: To be implemented by derived (Not mandatory)
        @return A configuration builder instance
        @see: configuration.Builder
        """
        
        return None
        
#-----------------------------------------------------------------------------------------------------------------------           
    def prepareOrchestrator(self, configuration, orchestrator, stopRun):
        """ Prepare the orchestrator for running the instance of this job
        @attention: To be implemented by derived 
        @param configuration: Orchestration configuration object which contains configuration set in configuration
                              builder if created
        @see configuration.Configuration
        @param orchestrator: Orchestrator to prepare for job running
        @param stopRun: A non argument callable to call in order to stop orchestrator job run
        @see Orchestrator
        """ 
        
        pass
 
#======================================================================================================================
class QuietError(Exception):
    """ Raise this exception to indicate an error without making all the stack trace fuss
    """
    
    pass

#=======================================================================================================================
def run(job, defaultConfigurationFilesPaths, args = []):
    """ Run an orchestration job
    @param: job: Job to run
    @see: JobBase
    @param defaultConfigurationFilesPaths: List of paths to default configuration files
    @param args: Optional command line arguments
    @return True on success
    """
    
    # Stop run flag and calls
    global shouldStopRun
    shouldStopRun = multiprocessing.Value('b', False)
    
    def stopRunCall(*args, **kwargs): 
        global shouldStopRun
        shouldStopRun.value = True
    
    # Ignore signals
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

    # Get logger
    logger = orchestration.logger._getTopLogger()
    
    # Create orchestrator
    orchestrator = Orchestrator(logger)    
    wasOrchestratorStarted = False
    
    try:
        # Create configuration builders
        orchestratorConfigurationBuilder = orchestrator._createConfigurationBuilder()
        jobConfigurationBuilder = job.createConfigurationBuilder()
        
        # Create option parser
        parser = argparse.ArgumentParser()

        # Add parsing group
        orchestratorConfigurationBuilder._addArgParseArgumentGroup(parser)
        
        if jobConfigurationBuilder is not None:
            jobConfigurationBuilder._addArgParseArgumentGroup(parser)

        # Parse options
        argparseNamespace = parser.parse_args(sys.argv[1:] + args)
        
        # And build
        orchestratorConfiguration = orchestratorConfigurationBuilder._buildConfiguration(argparseNamespace, 
                                                                                         defaultConfigurationFilesPaths)
        
        jobConfiguration = orchestration.configuration.Configuration({})
        
        if jobConfigurationBuilder is not None:
            jobConfiguration = jobConfigurationBuilder._buildConfiguration(argparseNamespace, 
                                                                           defaultConfigurationFilesPaths)        
        
        # Configure logger
        logger._configure(orchestratorConfiguration)
        
        # Configure orchestrator
        orchestrator._configure(orchestratorConfiguration)
        
        # Prepare orchestrator with job
        job.prepareOrchestrator(jobConfiguration, orchestrator, stopRunCall)
        
        logger.notice("Running job: %s", job.name)
        
        # Set graceful signal handlers            
        signal.signal(signal.SIGINT, stopRunCall)
        signal.signal(signal.SIGTERM, stopRunCall)   
        
        # Go!
        wasOrchestratorStarted = True
        orchestrator._start()
        
        # Hang in the air while should not stop this run...
        while not shouldStopRun.value: orchestrator._poll() 
            
    except QuietError:
        return False
    
    except KeyboardInterrupt:
        return False
    
    except SystemExit:
        return False
    
    except:
        logger.exception("Unexpected exception")
        
        return False
    
    finally:
        
        if wasOrchestratorStarted:
            orchestrator._stop()
        
    return True
