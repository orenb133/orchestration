import aenum
import events
import multiprocessing
import orchestration.timing
import os
import time
import types
import yappi

#======================================================================================================================= 
class Executer(object):
    
#-----------------------------------------------------------------------------------------------------------------------      
    def __init__(self, logger, loggerListener, statsCollector, id_, name, eventsBus, configuration):    
        """ C'tor
        @param logger: Parent logger
        @param loggerListener: Logger listener to connect to 
        @param statsCollector:  Stats collector to connect to
        @param id_: Executer identifier
        @param name: Executer name
        @param eventsBus: Events bus for executer to bind with
        @param configuration: Orchestration configuration
        @see configuration.Configuration        
        """
        
        self.__id = id_
        self.__name = name
        self.__logger = logger.createAdapter(self)
        self.__loggerListener = loggerListener
        self.__statsCollector = statsCollector
        self.__eventsBus = eventsBus
        self.__configuration = configuration
        self.__eventsHandlers = events._HandlersMap()
        self.__eventsChannel = events.Channel(self.__eventsBus)
        self.__shouldRun = multiprocessing.Value('b', False)
        self.__state = multiprocessing.Value('i', _ExecuterStatesE.STOPPED.value)        
        self.__process = multiprocessing.Process(target = self.__processTarget, name = self.__name)
        self.__countersContainer = statsCollector.createCountersContainer(name)
        self.__canCollect = False
       
        if not self.__configuration.disableStats:
            self.__countersContainerPublish_Poller = orchestration.timing._Poller(
                self.__countersContainer._publish, configuration.countersContainersStatsCollectorPublishInterval)
                
        # Counters
        self.__counterNumHandledEvents = self.__countersContainer.registerCounter(
            orchestration.stats.Counter("handled", "events", name))
        
#-----------------------------------------------------------------------------------------------------------------------          
    def __repr__(self):
        return "Executer(name=%s, id=%s)" % (self.__name, self.__id)      
    
#-----------------------------------------------------------------------------------------------------------------------  
    @property
    def id(self):
        """ The executer's id
        """
        
        return self.__id

#-----------------------------------------------------------------------------------------------------------------------  
    @property
    def _state(self):
        """ Get this executer state
        """
        
        return self.__state.value     
    
#-----------------------------------------------------------------------------------------------------------------------  
    def registerEventsHandler(self, eventsHandler):
        """ Register an events handler to this executer
        @param eventsHandler: Events handler to register
        @return Registered event handler
        """ 
               
        # First register to events bus in order to get an instanced identifier
        self.__eventsBus.registerHandler(eventsHandler, self.__id)
        
        # Now register locally
        self.__logger.debug2("Registering events handler: eventHandler=%s", eventsHandler)
        self.__eventsHandlers.put(eventsHandler.id, eventsHandler)
        
        # Initialize counters
        eventsHandler.initRegisterCounters(self.__countersContainer)
        
        return eventsHandler       
        
#-----------------------------------------------------------------------------------------------------------------------  
    def _start(self):
        """ Start running this executer in a new process
        """
        
        self.__logger.debug2("Starting executer")
        self.__shouldRun.value = True
        self.__process.start()

#-----------------------------------------------------------------------------------------------------------------------  
    def _stop(self):
        """ Stop the run of this executer
        """
        
        if self.__process.is_alive():
            self.__logger.debug2("Stopping executer")
            self.__shouldRun.value = False
            self.__canCollect = True
            
        else:
            self.__shouldRun.value = False
            self.__logger.debug2("Executer is not alive")
            
#-----------------------------------------------------------------------------------------------------------------------  
    def _collect(self):
        """ Collect the stopped executer's process
        """
        
        if self.__canCollect:
            self.__logger.debug2("Collecting executer")
            
            # Terminate if not stopped by now and join
            self.__process.terminate()        
            self.__process.join()

#-----------------------------------------------------------------------------------------------------------------------  
    def __moveToState(self, state):
        """ Move the executer to a new state
        @param state: New state to move to
        """

        self.__logger.debug2("Moving to state: toState=%s", state)
        self.__state.value = state.value                   
            
#-----------------------------------------------------------------------------------------------------------------------  
    def __startupSequence(self):
        """ The executer's startup sequence
        """    
        
        # We are forked - Switch logger to listener and connect to stats collector
        self.__logger.logger._connectToListener(self.__loggerListener.createHandler())
        
        if not self.__configuration.disableStats:
            self.__countersContainer._connectToCollector()
        
        self.__moveToState(_ExecuterStatesE.SEPERATED)

        # Bind and Connect to event bus
        self.__eventsBus.bindAndConnect(self.__id)
        self.__moveToState(_ExecuterStatesE.CONNECTED)        
        
        # Starting handlers
        for eventsHandler in self.__eventsHandlers.iterValues():
            self.__logger.debug2("Starting event handler: eventsHandler=%s", eventsHandler)
            self.__eventsChannel._setSourceHandler(eventsHandler)
            eventsHandler.start(self.__eventsChannel)
            
        self.__moveToState(_ExecuterStatesE.STARTED)
        self.__logger.info("Executer is online!")
        
        # Profile if needed
        if self.__configuration.profile:
            self.__logger.info("Profiling...")
            yappi.start(True, True)        
  
#-----------------------------------------------------------------------------------------------------------------------  
    def __shutdownSequence(self):
        """ The executer's shutdown sequence
        """
        
        if self.__configuration.profile:
          
            try:
                yappi.stop()
                
                if not os.path.exists(self.__configuration.profilingDirectoryPath):
                    os.makedirs(self.__configuration.profilingDirectoryPath)
                    
                filePath = os.path.join(self.__configuration.profilingDirectoryPath, 
                                        "yappi-data-%s-%s" % (self.__name, time.strftime('%Y%m%d-%H%M%S')))
                
                yappi.get_func_stats().save(filePath)
           
            except:
                self.__logger.exception("Unexpected exception")
        
        try:
            
            if self.__state.value >= _ExecuterStatesE.STARTED:

                # Stopping handlers                    
                for eventsHandler in self.__eventsHandlers.iterValues():
                    self.__logger.debug2("Stopping event handler: eventsHandler=%s", eventsHandler)
                    eventsHandler.stop()   
            
                self.__moveToState(_ExecuterStatesE.CONNECTED)         
                    
        except:
            self.__logger.exception("Unexpected exception")

        try:
            
            if self.__state.value >= _ExecuterStatesE.CONNECTED:
    
                # Disconnecting and unbind from events bus            
                self.__eventsBus.disconnectAndUnbind()
                self.__moveToState(_ExecuterStatesE.SEPERATED)        
                
        except:
            self.__logger.exception("Unexpected exception")

        if self.__state.value >= _ExecuterStatesE.SEPERATED:  
            
            self.__moveToState(_ExecuterStatesE.STOPPED)   
            self.__logger.info("Executer is offline!")
            
            # Disconnect from stats collector
            if not self.__configuration.disableStats:
                self.__countersContainer._disconnectFromCollector()
            
            # We are stopped - Disconnect from logger listener after configured timeout
            time.sleep(self.__configuration.loggerListenerDisconnectionTimeout)
            self.__logger.logger._disconnectFromListener() 
            
#-----------------------------------------------------------------------------------------------------------------------  
    def __processTarget(self):
        """ The executer's main loop
        """
        
        try:
            self.__startupSequence()
            yieldedEventsHandlerGenerator = None
            yieldedEventsHandler = None
            
            # Keep on running while you can
            while self.__shouldRun.value:
                
                if yieldedEventsHandlerGenerator is not None:
                    # Event handler had yielded continue running it
                    
                    try:
                        
                        if self.__logger.isEnabledFor(orchestration.logger.DEBUG5):
                            self.__logger.debug5("Resuming yielded event handler: eventsHandler=%s",
                                                 yieldedEventsHandler)
                            
                        yieldedEventsHandlerGenerator.next()
                        
                    except StopIteration:
                        
                        # Done running yielded event handler
                        yieldedEventsHandlerGenerator = None
                        pass
                        
                else:
                    
                    # Pickup a new event transport
                    eventTransport = self.__eventsBus.receive()
                    
                    # Got one? Get the destination handlers
                    if eventTransport:
                        eventsHandlers = self.__eventsHandlers.getIter(eventTransport.header.destinationHandlerId, 
                                                                       eventTransport.event.contextId)
                        
                        # Handle the event
                        for eventsHandler in eventsHandlers:
                            
                            # Not handling event from self unless in unicast
                            if eventsHandler.id == eventTransport.header.sourceHandlerId and \
                                eventsHandler.id != eventTransport.header.destinationHandlerId:
                                
                                if self.__logger.isEnabledFor(orchestration.logger.DEBUG5):
                                    self.__logger.debug5(("Discarding event to self event: event=%s, " + 
                                                         "header=%s, eventsHandler=%s"), 
                                                         eventTransport.event, eventTransport.header, eventsHandler)  
                                    
                                continue  
                            
                            if self.__logger.isEnabledFor(orchestration.logger.DEBUG5):
                                self.__logger.debug5("Handling event: event=%s, header=%s, eventsHandler=%s", 
                                                     eventTransport.event, eventTransport.header, eventsHandler)    
                            
                            self.__eventsChannel._setSourceHandler(eventsHandler)
                            yieldedEventsHandlerGenerator = eventsHandler.handleEvent(eventTransport.event, 
                                                                                     eventTransport.header, 
                                                                                     self.__eventsChannel)
                            
                            if isinstance(yieldedEventsHandlerGenerator, types.GeneratorType):
                                yieldedEventsHandler = eventsHandler
                            
                            # Count
                            self.__counterNumHandledEvents.value += 1
                        
                        if not eventsHandlers:
                            self.__logger.warning("Failed finding event handler for event: event=%s, header=%s",  
                                                  eventTransport.event, eventTransport.header)
                            
                    else:
                        time.sleep(self.__configuration.executersIdleTime)
                    
                    
                # Poll for counters publishing   
                if not self.__configuration.disableStats:   
                    self.__countersContainerPublish_Poller.poll()
                    
        except orchestration.QuietError:
            pass
        
        except KeyboardInterrupt:
            pass
        
        except:
            self.__logger.exception("Unexpected exception")

        finally:
            self.__shutdownSequence()

#======================================================================================================================= 
class _ExecuterStatesE(aenum.IntEnum):
    STOPPED = 0
    SEPERATED = 1
    CONNECTED = 2
    STARTED = 3
    