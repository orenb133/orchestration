import datetime
import influxdb
import multiprocessing
import time
import orchestration.timing
import orchestration.queues
import os

#=======================================================================================================================
class Counter(object):
    __slots__ = ['__name', '__group', '__unit', '__time' ,'value']

#-----------------------------------------------------------------------------------------------------------------------        
    def __init__(self, name, group, unit):   
        """ C'tor
        @param name: Counter name
        @param group: The contextual group in which this counter belongs to (e.g. disk-red-bytes and disk-written-bytes 
                      belongs to group disk-io)
        @param unit: The unit name in which this counter is counting for (e.g. Events handler name)
        """
        
        self.value = 0
        self.__time = -1
        self.__name = name
        self.__group = group
        self.__unit = unit

#-----------------------------------------------------------------------------------------------------------------------         
    @property
    def name(self):
        """ The counter's name
        """
         
        return self.__name
    
#-----------------------------------------------------------------------------------------------------------------------     
    @property
    def group(self):
        """ The counter's group name
        """
         
        return self.__group
    
#----------------------------------------------------------------------------------------------------------------------- 
    @property
    def unit(self):
        """ The counter's unit name
        """
         
        return self.__unit  
    
#----------------------------------------------------------------------------------------------------------------------- 
    def at(self, time):
        """ Set a time for the counter so next value set will be regarded as set in this time
        @param time: Unix time
        @return: This object with the time set for chaining value set
        """
         
        self.__time = time
        
        return self   
    
#----------------------------------------------------------------------------------------------------------------------- 
    @property
    def _time(self):
        """ Unix time in case at() value set was used, -1 if was never used
        """
         
        return self.__time
    
#-----------------------------------------------------------------------------------------------------------------------    
    def __repr__(self): return "Counter(name=%s, group=%s, unit=%s, value=%s)" % (self.__name, self.__group, 
                                                                                  self.__unit, self.value)  
    
#=======================================================================================================================
class CountersContainer(object):
    
#-----------------------------------------------------------------------------------------------------------------------    
    def __init__(self, logger, id_, name, configuration):   
        """ C'tor
        @param logger: Parent logger
        @param id_: Integer to identify the container with
        @param name: container name
        @param orchestration configuration
        @see configuration.Configuration        
        """
        
        self.__id = id_
        self.__name = name
        self.__logger = logger.createAdapter(self)
        self.__configuration = configuration
        self.__counters = []   
        self.__queue = orchestration.queues._QueueZmq(self.__logger, 0, "egress-stats-collector", 
                                                      os.path.join(self.__configuration.runDirectoryPath, 
                                                                   _Collector.UNIX_DOMAIN_SOCKET),
                                                      orchestration.queues._SerializerPickle())

#-----------------------------------------------------------------------------------------------------------------------    
    def registerCounter(self, counter):
        """ Register a counter to this container
        @param counter: Counter to register
        @return: Registered counters
        """
        
        self.__logger.debug2("Registering counter: counter=%s", counter)
        self.__counters.append(counter)
        
        return counter
        
#-----------------------------------------------------------------------------------------------------------------------            
    @property
    def _name(self): 
        """ The counters container name
        """
        
        return self.__name        
                   
#-----------------------------------------------------------------------------------------------------------------------                     
    def _connectToCollector(self):
        """ Connect to counters collector
        """
        
        self.__logger.debug2("Connecting to collector")
        self.__queue.connect()
        
#-----------------------------------------------------------------------------------------------------------------------          
    def _disconnectFromCollector(self):
        """ Disconnect from counters collector
        """

        self.__logger.debug2("Disconnecting from collector")
        self.__queue.disconnect()
       
#-----------------------------------------------------------------------------------------------------------------------        
    def _iterCounters(self):
        """ Create an iterator for counters
        @return: Iterator for counters
        """
        
        for counter in self.__counters:
            yield counter
        
#-----------------------------------------------------------------------------------------------------------------------           
    def _publish(self):
        """ Publish to collector
        """
        
        currentTime = int(time.time())
        
        if self.__counters:
            values = []
            times = []
            
            for counter in self.__counters:
                values.append(counter.value)
                times.append(currentTime if counter._time == -1 else counter._time)
            
            dataPoints = _DataPoints(self.__id, values, times)
            
            if self.__logger.isEnabledFor(orchestration.logger.DEBUG5):
                self.__logger.debug5("Sending data points to collector: dataPoints=%s", dataPoints)
        
            self.__queue.push(dataPoints)
        
#-----------------------------------------------------------------------------------------------------------------------    
    def __repr__(self): return "CountersContainer(name=%s, id=%s)" % (self.__name, self.__id)
   
#=======================================================================================================================
class _DataPoints(object):
    __slots__ = ['countersContainerId', 'values', 'times']

#-----------------------------------------------------------------------------------------------------------------------        
    def __init__(self, countersContainerId, values, times):   
        """ C'tor
        @param countersContainerId: Id of counters container
        @param values: List of counters values 
        @param times: List of counters times in Unix time
        """
        
        self.countersContainerId = countersContainerId
        self.values = values
        self.times = times
        
#-----------------------------------------------------------------------------------------------------------------------    
    def __repr__(self): return "_DataPoints(countersContainerId=%s, values=%s)" % (self.countersContainerId, 
                                                                                   self.values)   
   
#=======================================================================================================================
class _Collector(object):    

    UNIX_DOMAIN_SOCKET = "stats.sock"
    INFLUXDB_TAG_INCEPTION = "inception"
    
#-----------------------------------------------------------------------------------------------------------------------    
    def __init__(self, logger, loggerListener, configuration):   
        """ C'tor
        @param logger: Parent logger
        @param loggerListener: Logger listener to connect to
        @param orchestration configuration
        @see configuration.Configuration        
        """
        
        self.__logger = logger.createAdapter("StatsCollector")
        self.__loggerListener = loggerListener
        self.__configuration = configuration
        self.__shouldRun = multiprocessing.Value('b', False)
        self.__wasLaunched = multiprocessing.Value('b', False)
        self.__process = multiprocessing.Process(target = self.__processTarget, name="stats-collector")
        self.__startSemaphore = multiprocessing.Semaphore()
        self.__queue = orchestration.queues._QueueZmq(self.__logger, 0, "ingress-stats-collector", 
                                                      os.path.join(self.__configuration.runDirectoryPath, 
                                                                   self.UNIX_DOMAIN_SOCKET),
                                                      orchestration.queues._SerializerPickle())
        self.__countersContainers = []
        self.__influxdbDataPoints = []
        self.__influxdbClient = None
        self.__inception = datetime.datetime.now().strftime("%x-%X")
        
        if not self.__configuration.disableStats:
            splitData = self.__configuration.influxdb.split('@')
            self.__influxdbDatabaseName = splitData[0]
            self.__influxdbHost = splitData[1].split(':')[0]
            self.__influxdbPort = splitData[1].split(':')[1]
            self.__flush_Poller = orchestration.timing._Poller(self.__flush, 
                                                            self.__configuration.statsCollectorInfluxdbPublishInterval)
        
#-----------------------------------------------------------------------------------------------------------------------      
    @property        
    def isAlive(self):
        """ Is the logger listener alive
        """
        
        return self.__configuration.disableStats or self.__process.is_alive()        
        
#-----------------------------------------------------------------------------------------------------------------------            
    def createCountersContainer(self, name):
        """ Create a counters container 
        @param name: container name
        @return: New counters container
        """
        
        containerId = len(self.__countersContainers)
        container = CountersContainer(self.__logger,containerId, name, self.__configuration)
        self.__logger.debug2("Counters container created: countersContainer=%s", (container,))
        self.__countersContainers.append(container)
        
        return container
    
#-----------------------------------------------------------------------------------------------------------------------  
    def start(self):
        """ Start the stats collector
        """
        if not self.__configuration.disableStats:
            self.__logger.debug2("Starting stats collector")
            self.__shouldRun.value = True
            self.__process.start()
            
            # Acquire first to negate the semaphore and then try again till it is released
            self.__startSemaphore.acquire()
            self.__startSemaphore.acquire(True)
            
            if not self.__wasLaunched.value:
                self.__logger.error("Failed to launch stats collector")
                
                raise orchestration.QuietError()

#-----------------------------------------------------------------------------------------------------------------------  
    def stop(self):
        """ Stop the stats collector
        """
        
        if not self.__configuration.disableStats:
            
            if self.__wasLaunched.value:
                self.__logger.debug2("Stopping stats collector")
                self.__shouldRun.value = False
                self.__process.join()    

#----------------------------------------------------------------------------------------------------------------------- 
    def __connectToInfluxdb(self):
        """ Connect to influxdb
        """
        
        self.__logger.debug2("Connecting to influxdb: host=%s, port=%s, database=%s", self.__influxdbHost, 
                             self.__influxdbPort, self.__influxdbDatabaseName)
        
        try:
            self.__influxdbClient = influxdb.InfluxDBClient(host = self.__influxdbHost, port = self.__influxdbPort, 
                                                            database = self.__influxdbDatabaseName)
            
        except:
            self.__logger.error("Failed connecting to influxdb: host=%s, port=%s, database=%s", 
                                self.__influxdbHost, self.__influxdbPort, self.__influxdbDatabaseName, e)
            
            raise orchestration.QuietError()
        
#----------------------------------------------------------------------------------------------------------------------- 
    def __validateInfluxdbDatabase(self):
        """ Validate the existence of the database in influxdb
        """
            
        self.__logger.debug2("Making sure database exists in influxdb: host=%s, port=%s, database=%s", 
                             self.__influxdbHost, self.__influxdbPort, self.__influxdbDatabaseName)            
            
        try:            
            results = self.__sendInfluxdbQuery("SHOW DATABASES")
            databaseFound = self.__influxdbDatabaseName in [str(x['name']) for x in results['databases']]
                    
            if not databaseFound:
                self.__logger.debug2("Influxdb database was not found, creating: host=%s, port=%s, database=%s", 
                                     self.__influxdbHost, self.__influxdbPort, self.__influxdbDatabaseName)  
               
                self.__sendInfluxdbQuery("CREATE DATABASE \"%s\"" % self.__influxdbDatabaseName)
                
        except:
            self.__logger.error("Failed validating influxdb database: host=%s, port=%s, database=%s", 
                                self.__influxdbHost, self.__influxdbPort, self.__influxdbDatabaseName)     
            
            raise orchestration.QuietError() 
        
#----------------------------------------------------------------------------------------------------------------------- 
    def __retainInfluxdbDatabaseSize(self):
        """ Retain the history size of our measures
        """
            
        self.__logger.debug2("Retaining influxdb database size: host=%s, port=%s, database=%s", 
                             self.__influxdbHost, self.__influxdbPort, self.__influxdbDatabaseName)    
        
        try:
            results = self.__sendInfluxdbQuery("SHOW TAG VALUES ON \"%s\" WITH KEY = \"%s\"" % 
                                             (self.__influxdbDatabaseName, self.INFLUXDB_TAG_INCEPTION))                       
            
            for measurementName in [str(x[0]) for x in results.keys()]:
                history = sorted([x['value'] for x in results[measurementName]])
                
                if len(history) > self.__configuration.influxdbMeasurementHistoryMaxSize:
                    
                    size = len(history) - self.__configuration.influxdbMeasurementHistoryMaxSize
                    
                    for serieInception in history[:size]:
                        self.__sendInfluxdbQuery("DROP SERIES FROM \"%s\" WHERE \"%s\" = '%s'" % 
                                                 (measurementName, self.INFLUXDB_TAG_INCEPTION, serieInception))                    
                        
        except:
            self.__logger.error("Failed retaining influxdb database size: host=%s, port=%s, database=%s", 
                                self.__influxdbHost, self.__influxdbPort, self.__influxdbDatabaseName)     
            
            raise orchestration.QuietError()                   
  
#-----------------------------------------------------------------------------------------------------------------------
    def __sendInfluxdbQuery(self, query):   
        """ Send an influxdb query
        @param query: Query to send
        @return: influxdb.resultset object 
        """  
        
                        
        self.__logger.debug3("Sending query to influxdb: host=%s, port=%s, database=%s, query=%s", 
                             self.__influxdbHost, self.__influxdbPort, self.__influxdbDatabaseName, query)                           
        
        
        return self.__influxdbClient.query(query)        
        
#-----------------------------------------------------------------------------------------------------------------------
    def __flush(self):   
        """ Flush data points to influxdb   
        """
        
        if self.__influxdbDataPoints:
        
            if self.__logger.isEnabledFor(orchestration.logger.DEBUG5):
                self.__logger.debug5("Flushing data points to influxdb: influxdbDataPoint=%s", 
                                     self.__influxdbDataPoints)            
            
            rc = self.__influxdbClient.write_points(self.__influxdbDataPoints, time_precision='s')
            
            if not rc:
                self.__logger.warning("Failed writing points to influxdb: points=%s", 
                                      self.__influxdbDataPoints)  
                
            self.__influxdbDataPoints = []      

#-----------------------------------------------------------------------------------------------------------------------      
    def __processTarget(self):
        """ Stats collector process target 
        """
        
        try:
            # Connect to logger listener, bind to queue, connect to influxdb and then release start condition
            self.__logger.logger._connectToListener(self.__loggerListener.createHandler())
            self.__queue.bind()
            self.__connectToInfluxdb()
            self.__validateInfluxdbDatabase()
            self.__retainInfluxdbDatabaseSize()
            self.__wasLaunched.value = True
            self.__startSemaphore.release()
            self.__logger.debug2("Stats collector started")
            
            while self.__shouldRun.value:
                dataPoints = self.__queue.pop()
                
                if dataPoints is not None:
                    
                    if self.__logger.isEnabledFor(orchestration.logger.DEBUG5):
                        self.__logger.debug5("Received data points: dataPoints=%s", dataPoints)                       
                    
                    # Aggregating to batch
                    countersContainer = self.__countersContainers[dataPoints.countersContainerId]
                    index = 0
                    
                    for counter in countersContainer._iterCounters():
                        
                        influxdbDataPoint = {'measurement' : counter.group,
                                             'tags'        : {'source' : countersContainer._name, 
                                                              'unit' : counter.unit,
                                                              self.INFLUXDB_TAG_INCEPTION : self.__inception},
                                             'time'        : dataPoints.times[index],
                                             'fields'      : {counter.name : dataPoints.values[index]}}
                        
                        if self.__logger.isEnabledFor(orchestration.logger.DEBUG5):
                            self.__logger.debug5("Created influxdb data point: influxdbDataPoint=%s",
                                                 influxdbDataPoint)     
                        
                        self.__influxdbDataPoints.append(influxdbDataPoint)
                        index += 1
                        
                else:
                    time.sleep(self.__configuration.statsCollectorIdleTime)

                self.__flush_Poller.poll()
                        
        except KeyboardInterrupt:
            pass        
        
        except orchestration.QuietError:
            pass
        
        except:
            self.__logger.exception("Unexpected exception")   
            
        finally:
            
            if not self.__wasLaunched.value:
                self.__startSemaphore.release()
                             
            self.__queue.unbind()
            self.__logger.debug2("Stats collector stopped")
            self.__logger.logger._disconnectFromListener()
    