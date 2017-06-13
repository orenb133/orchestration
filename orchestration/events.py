import aenum
import collections
import multiprocessing
import orchestration.logger
import os
import queues
import time

#=======================================================================================================================
class Event(collections.namedtuple("Event","type data contextId")):

#-----------------------------------------------------------------------------------------------------------------------
    def __new__(cls, type_, data = None, contextId = None):
        """ C'tor
        @param type_: Enum representing the event type
        @see aenum.IntEnum
        @param data: Event data to attach
        @attention: data can be ONLY tuple, dict, list or Python built in primitives in any nesting combination.
                    Any other type will cause unpredictable results (Usually serialization exception)
        @param contextId: Unique identifier across several events to gather them as a single context (e.g. A sends 
                          event to B with context identifier 2 and place some context related data in a table with 
                          key 2. B send an event back to A with the same context identifier, A can fetch the contextual
                          data saved in its table with the event B sent by getting it using the context id as key)
        """
        return super(Event, cls).__new__(cls, type_, data, contextId)

#=======================================================================================================================
Header = collections.namedtuple("Header","sourceHandlerId destinationHandlerId")
""" C'tor
@param sourceHandlerId: Source events handler identifier
@see HeandlerIdentifier
@param destinationHandlerId: Destination events handler identifier or group
@see HeandlerIdentifier
"""

#=======================================================================================================================
class HandlerIdentifier (collections.namedtuple("HandlerIdentifier", "type instance")):

#-----------------------------------------------------------------------------------------------------------------------
    def __new__(cls, type_,  instance):
        """ C'tor
        @param type: Enum representing the handler type (Negative values are reserved)
        @see aenum.IntEnum
        @param instance: Handler instance of the above type (Negative values are reserved)
        """
        
        return super(HandlerIdentifier, cls).__new__(cls, type_, instance)            

#-----------------------------------------------------------------------------------------------------------------------                
    def __eq__(self, other): return self.type == other.type and self.instance == other.instance
    
#-----------------------------------------------------------------------------------------------------------------------        
    def __ne__(self, other): return self.type != other.type or self.instance != other.instance
    
#=======================================================================================================================
class HandlerBase(object):

#-----------------------------------------------------------------------------------------------------------------------       
    def __init__(self, type_, name):    
        """ C'tor
        @param logger: Parent logger
        @param type_: Enum representing handler type
        @see aenum.IntEnum
        @param name: Handler name
        """
         
        self.__id = HandlerIdentifier(type_, None)
        self.__name = name

#-----------------------------------------------------------------------------------------------------------------------       
    def __repr__(self): return "Handler(name=%s, id=%s)" % (self.__name, self.__id)        

#-----------------------------------------------------------------------------------------------------------------------   
    @property
    def id(self):
        """ Handler identifier
        @see HandlerIdentifier
        """
         
        return self.__id

#-----------------------------------------------------------------------------------------------------------------------   
    @property
    def name(self):
        """ Handler name
        """
         
        return self.__name    
    
#-----------------------------------------------------------------------------------------------------------------------   
    @property
    def logger(self):
        """ Handler's logger
        @attention: Available only after registration
        """
        
        return self.__logger      
 
#-----------------------------------------------------------------------------------------------------------------------   
    def initRegisterCounters(self, countersContainer):
        """ Register counters, called before execution context
        @attention To be implemented by derived
        @param countersContainer: A counters container to register counters to
        @attention: Channel is volatile, do not save as member
        @see: Stats.CountersContainer        
        """ 
        
        pass
    
#-----------------------------------------------------------------------------------------------------------------------   
    def start(self, channel):
        """ First call to this event handler in execution context
        @attention To be implemented by derived
        @param channel: A channel to the event bus allowing to send events 
        @attention: Channel is volatile, do not save as member
        @see: Channel  
        """ 
        
        pass

#-----------------------------------------------------------------------------------------------------------------------   
    def handleEvent(self, event, header, channel):
        """ Handle an event
        @attention To be implemented by derived
        @param event: Event to handle
        @see Event
        @param header: Header describing event in transport
        @see Header
        @param channel: A channel to the event bus allowing to send events
        @attention: Channel is volatile, do not save as member 
        @see: Channel
        @return Generator to be called again (by calling yield) | None when done
        """ 
        
        pass
 
#-----------------------------------------------------------------------------------------------------------------------   
    def stop(self):
        """ Last call to this event handler in execution context
        @attention To be implemented by derived 
        """
        
        pass
        
#-----------------------------------------------------------------------------------------------------------------------    
    def _initRegistration(self, logger, id_):
        """ Initialize this handler upon registration
        @param logger: parent logger
        @param id_: Identifier to set
        """
        
        self.__id = id_
        self.__logger = logger.createAdapter(self)
 
#=======================================================================================================================
class Channel(object):
    
#-----------------------------------------------------------------------------------------------------------------------     
    def __init__(self, bus):
        """ C'tor
        @param bus: Event bus
        """
        
        self.__bus = bus
        self.__sourceHandler = None

#-----------------------------------------------------------------------------------------------------------------------         
    def unicast(self, event, destinationHandlerId):
        """ Send a unicast event to a specific event handler
        @param event: Event to send
        @param destinationHandlerId: Handler identifier of the destination
        @see HandlerIdentifier
        """
        
        self.__bus.send(_Transport(Header(self.__sourceHandler.id, destinationHandlerId), event))
    
#-----------------------------------------------------------------------------------------------------------------------         
    def anycast(self, event, destinationHandlerType):
        """ Send an anycast event to an event handler of this type 
        @param event: Event to send
        @note The handler instance will be picked by a hash over the context id                          
        @param destinationHandlerType: The type of the destination handler
        """
        
        self.__bus.send(_Transport(Header(self.__sourceHandler.id, 
                                           HandlerIdentifier(destinationHandlerType, 
                                                              _HandlerIdentifierInstanceGroupE.ANY)), event))
        
#-----------------------------------------------------------------------------------------------------------------------         
    def multicast(self, event, destinationHandlerType):
        """ Send an multicast event to all event handler of this type 
        @param event: Event to send
        @param destinationHandlerType: The type of the destination handler
        """
        self.__bus.send(_Transport(Header(self.__sourceHandler.id,
                                           HandlerIdentifier(destinationHandlerType, 
                                                              _HandlerIdentifierInstanceGroupE.ALL)), event))
              
#-----------------------------------------------------------------------------------------------------------------------         
    def broadcast(self, event):
        """ Send an broadcast event to all event handlers 
        @param event: Event to send
        """
        
        self.__bus.send(_Transport(Header(self.__sourceHandler.id, 
                                          HandlerIdentifier(_HandlerIdentifierInstanceGroupE.ALL, 
                                                              _HandlerIdentifierInstanceGroupE.ALL)), event))
    
#-----------------------------------------------------------------------------------------------------------------------         
    def _setSourceHandler(self, sourceHandler):
        """ Set the source events handler
        @param sourceHandler: Source events handler
        """
        
        self.__sourceHandler = sourceHandler   

#======================================================================================================================= 
class _HandlerIdentifierInstanceGroupE(aenum.IntEnum):
    ALL = -1
    ANY = -2
    LAST = -2

#=======================================================================================================================         
class _HandlerIdentifierTypeGroupE(aenum.IntEnum):
    ALL = -1
    LAST = -1

#=======================================================================================================================
class _Transport(collections.namedtuple("_Transport","header event")):
    """ C'tor
    @param header: Header describing event in transport
    @param event: Event to be in transport
    """

#-----------------------------------------------------------------------------------------------------------------------       
    def __repr__(self): return "Transport(header=%s, event=%s)" % (self.header, self.event)

#=======================================================================================================================        
class _Bus(object):
    
    UNIX_DOMAIN_SOCKET_PATTERN = "events-%s.sock"
    
#-----------------------------------------------------------------------------------------------------------------------   
    def __init__(self, logger, configuration):
        """ C'tor
        @param logger: Parent logger
        @param Configuration: Orchestration configuration
        @see configuration.Configuration
        """ 
        
        self.__logger = logger.createAdapter("Bus")
        self.__configuration = configuration
        self.__handlersIdsToBusIds = _HandlersMap()
        self.__busIdsToQueues = []
        self.__localQueue = None
        self.__ingressQueue = None
        self.__egressQueues = []
        self.__typesCount = {}
        self.__localBusId = None
        self.__registeredBusIds = set()
        self.__unbindedBusIds = multiprocessing.Value('i', 0)

#-----------------------------------------------------------------------------------------------------------------------   
    def registerHandler(self, handler, busId):
        """ Assign an instanced identifier to the handler and maps its identifier to a bus identifier
        @param handler: Handler to register
        @param busId: Bus identifier to represent handler identifier
        """
       
        handlerType = handler.id.type
        
        if handlerType not in self.__typesCount:
            self.__typesCount[handlerType] = 0

        # Instance is set number of handlers with the same type            
        handler._initRegistration(self.__logger, HandlerIdentifier(handlerType, self.__typesCount[handlerType]))
        self.__typesCount[handler.id.type] += 1

        # And registering        
        self.__logger.debug2("Registering handler: busId=%s, handler=%s" , busId, handler)
        self.__handlersIdsToBusIds.put(handler.id, busId)
        self.__registeredBusIds.add(busId)
        self.__unbindedBusIds.value = len(self.__registeredBusIds)
 
#-----------------------------------------------------------------------------------------------------------------------   
    def bindAndConnect(self, busId):
        """  Bind to this bus identifier and connect to the rest of the bus users
        @attention: Should be called within an execution context(e.g. Executer main loop or orchestrator context)
        @param busId: Bus identifier to bind to        
        """

        self.__localBusId = busId

        if busId in self.__registeredBusIds:
            self.__logger.debug2("Binding: busId=%s", busId)

            # Bind to ingress queue
            self.__ingressQueue = queues._QueueZmq(self.__logger, busId, "ingress-%s" % (self.__localBusId,), 
                                                   os.path.join(self.__configuration.runDirectoryPath, 
                                                                self.UNIX_DOMAIN_SOCKET_PATTERN) % (self.__localBusId,),
                                                   queues._SerializerMessagePack())
            self.__ingressQueue.bind()
            
            # Decrease number of unbinded identifiers
            self.__unbindedBusIds.value -= 1
            
        else:
            self.__logger.error("No registered handlers to bus identifier, not binding: busId=%s", busId) 
            
            raise orchestration.QuietError()
        
        self.__logger.debug2("Connecting: localBusId=%s", self.__localBusId)
        timeout = self.__configuration.eventsBusConnectionTimeout

        # While there are still unbinded identifiers
        while self.__unbindedBusIds.value > 0:
            self.__logger.debug3("Waiting for all binds to complete: localBusId=%s leftBinds=%s", self.__localBusId, 
                                                                                        self.__unbindedBusIds.value)

            # Retry reconnecting
            if timeout > 0 and self.__unbindedBusIds.value > 0:
                time.sleep(1)
                timeout -= 1

            elif self.__unbindedBusIds.value > 0:
                self.__logger.error("Connection failed: localBusId=%s leftBinds=%s", self.__localBusId, 
                                    self.__unbindedBusIds.value)    
                
                raise orchestration.QuietError()          

        # Create a local queue for this execution context
        self.__localQueue = queues._QueueLocal(self.__logger, self.__localBusId, "local-%s" % (self.__localBusId,))
        
        # Go over all the mapped bus identifiers, set local queue to given bus identifier and set and connect to egress
        # queues of the rest
        for mappedBusId in self.__handlersIdsToBusIds.iterValues():                
            
            # If the bus identifier to queue list length is smaller than given bus identifier, extend the list 
            busIdToQueueLength = len(self.__busIdsToQueues)
            
            if busIdToQueueLength < mappedBusId + 1:
                self.__busIdsToQueues.extend( [None] * (mappedBusId - busIdToQueueLength + 1))

            # On empty elements place local queue or new egress queue
            if self.__busIdsToQueues[mappedBusId] is None:
            
                if mappedBusId == self.__localBusId:
                    self.__busIdsToQueues[mappedBusId] = self.__localQueue
                
                else:
                    egressQueue = queues._QueueZmq(self.__logger, mappedBusId, "egress-%s" % mappedBusId, 
                                                        os.path.join(self.__configuration.runDirectoryPath, 
                                                                self.UNIX_DOMAIN_SOCKET_PATTERN) % (mappedBusId,),
                                                   queues._SerializerMessagePack())

                    egressQueue.connect()
                    self.__busIdsToQueues[mappedBusId] = egressQueue
                    self.__egressQueues.append(egressQueue)
                
#-----------------------------------------------------------------------------------------------------------------------                    
    def disconnectAndUnbind(self):
        """ Disconnect from the bus
        """
        
        self.__logger.debug2("Disconnecting: localBusId=%s", self.__localBusId) 
        
        for queue in self.__egressQueues:
            queue.disconnect()
            
        self.__logger.debug2("Unbinding: localBusId=%s", self.__localBusId)             
            
        self.__ingressQueue.unbind()
                
#-----------------------------------------------------------------------------------------------------------------------
    def send(self, transport):
        """ Send a transport to this bus
        @param transport: Event transport to send
        """
        
        # Getting all bus identifiers as frozen set so we send an event only once to its destination
        busIds = frozenset(self.__handlersIdsToBusIds.getIter(transport.header.destinationHandlerId, 
                                                          transport.event.contextId))
        
        for busId in busIds:
            queue = self.__busIdsToQueues[busId]
            
            if self.__logger.isEnabledFor(orchestration.logger.DEBUG5):
                self.__logger.debug5("Pushing transport to queue: transport=%s queue=%s", transport, queue)
            
            queue.push(transport)
        
        if not busIds:
            self.__logger.warning("Failed finding bus identifier for event transport: transport=%s", transport)
        
#-----------------------------------------------------------------------------------------------------------------------
    def receive(self):
        """ Receive an event transport from this bus
        @return: An event transport or None if event transport was not found
        """
        
        # First try from local queue
        queue = self.__localQueue
        transport = queue.pop()
        
        # Local queue is empty, try ingress
        if transport is None:
            queue = self.__ingressQueue
            transport = queue.pop()
           
        if transport is not None:

            # Ugly but rather quick and currently necessary (Rather than adding a Messagepack type) in order to unpack 
            # the _Transport named tuple
            transport = _Transport(Header(HandlerIdentifier(*transport[0][0]), HandlerIdentifier(*transport[0][1])), 
                                   Event(*transport[1]))
            
            if self.__logger.isEnabledFor(orchestration.logger.DEBUG5):
                self.__logger.debug5("Popped transport from queue: event=%s queue=%s", transport, queue)            

        return transport

#=======================================================================================================================
class _HandlersMap:

#-----------------------------------------------------------------------------------------------------------------------         
    def __init__(self):
        self.__typeToInstanceToValue = {}

#-----------------------------------------------------------------------------------------------------------------------         
    def put(self, id_, value):
        """ Map a handler identifier to a value
        @param id_: Handler identifier
        @param value: Value to map to above identifier
        """
        
        # Try getting instance to values list for the given handler type
        instancesToValue = self.__typeToInstanceToValue.get(id_.type, None)
        
        # Don't have it? Add an empty list
        if instancesToValue is None:
            instancesToValue = []
            self.__typeToInstanceToValue[id_.type] = instancesToValue

        # Get the instance to values list length            
        instancesToValueLength = len(instancesToValue)          
        
        # If the instance to values list length is smaller than given instance, extend the list 
        if instancesToValueLength < id_.instance + 1:
            instancesToValue.extend( [None] * (id_.instance - instancesToValueLength + 1))

        # Now place it - We are expecting an empty place since instances should be unique      
        assert instancesToValue[id_.instance] is None
        instancesToValue[id_.instance] = value
            
#-----------------------------------------------------------------------------------------------------------------------                     
    def getIter(self, id_, hashValue = 0):
        """ Get an iterable to mapped values for a given handler identifier
        @param id_: Handler identifier to get mapped value for
        @param hashValue: If identifier is an anycast identifier use hashValue to select an instance
        @return: An iterable to mapped values for given identifier or an empty iterable if not found
        """
        
        assert id_.type >= _HandlerIdentifierTypeGroupE.LAST
        assert id_.instance >= _HandlerIdentifierInstanceGroupE.LAST
        
        # Broadcast - Return everyone
        if id_.type == _HandlerIdentifierTypeGroupE.ALL:
            return self.iterValues()
        
        # Getting the instance value list
        instancesToValues = self.__typeToInstanceToValue.get(id_.type, ())
        instancesToValuesNoNones = [x for x in instancesToValues if x is not None] 

        # Multicast - Return the whole list, if the list is empty, just return is as it fits all 
        # Empty tuple - All following types will return empty tuple when there are no instances so just return it here
        if id_.instance == _HandlerIdentifierInstanceGroupE.ALL or not instancesToValuesNoNones:
            return instancesToValuesNoNones
        
        instance = id_.instance
        instancesToValuesLength = len(instancesToValues)
        instancesToValuesNoNonesLength = len(instancesToValuesNoNones)
        
        # Anycast - Use context identifier to hash out an instance
        if id_.instance == _HandlerIdentifierInstanceGroupE.ANY:
            return (instancesToValuesNoNones[hashValue % instancesToValuesNoNonesLength],)
            
        # Unicast - check if we are out of bounds            
        if instance < instancesToValuesLength:
            value = instancesToValues[instance]
            
            if value is not None:
                return (value, )   
        
        return ()       
        
#-----------------------------------------------------------------------------------------------------------------------     
    def iterItems(self):
        """ Get an iterator over the (identifier, value) tuples
        @return: iterator over the (identifier, value) tuples
        """
        
        for type_, instancesToValues in self.__typeToInstanceToValue.iteritems():
            instance = 0
            
            for value in instancesToValues:

                if value is not None:
                  
                    yield (HandlerIdentifier(type_, instance), value)
                    
                instance += 1

#-----------------------------------------------------------------------------------------------------------------------                     
    def iterValues(self):
        """ Get an iterator over the map values
        @return: iterator over the map values
        """
        
        for instancesToValues in self.__typeToInstanceToValue.itervalues():

            for value in instancesToValues:

                if value is not None:

                    yield value    
