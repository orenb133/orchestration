import collections
import cPickle
import errno
import msgpack
import os
import zmq

#=======================================================================================================================
class _QueueBase:
    
#----------------------------------------------------------------------------------------------------------------------- 
    def __init__(self, logger, id_, name):   
        """ C'tor
        @param logger: Parent logger
        @param id_: Integer to identify the queue with
        @param name: Queue name
        """

        self.__id = id_
        self.__name = name                
        self._logger = logger.createAdapter(self)
        
#-----------------------------------------------------------------------------------------------------------------------    
    def __repr__(self): return "Queue(name=%s, id=%s)" % (self.__name, self.__id)        

#-----------------------------------------------------------------------------------------------------------------------    
    def push(self, element):
        """ Push an element to this queue
        @attention: To be implemented by derived
        @param element: Element to push into the queue
        """
        
        pass

#-----------------------------------------------------------------------------------------------------------------------    
    def pop(self):
        """ Pop an element from this queue
        @attention: To be implemented by derived        
        @return: Element or None if the queue is empty 
        """
        
        pass
    
#=======================================================================================================================
class _QueueLocal(_QueueBase):
    
#-----------------------------------------------------------------------------------------------------------------------    
    def __init__(self, logger, id_, name):
        """ C'tor
        @see: Base class
        """
         
        _QueueBase.__init__(self, logger, id_, name)
        self.__dequeue = collections.deque()    
    
#-----------------------------------------------------------------------------------------------------------------------    
    def push(self, element):
        """
        @see Base class
        """
             
        self.__dequeue.append(element)
    
#-----------------------------------------------------------------------------------------------------------------------    
    def pop(self):
        """
        @see Base class
        """        
        
        if self.__dequeue:
            return self.__dequeue.popleft()    
            
        return None
    
#=======================================================================================================================
class _QueueZmq(_QueueBase):
    
#-----------------------------------------------------------------------------------------------------------------------    
    def __init__(self, logger, id_, name, socketPath, serializer):
        """ C'tor
        @see: Base class
        @param socketPath: Path to locate Unix Domain Sockets within
        @param serializer: Serializer to use
        @see: _SerializerInterface 
        """
         
        _QueueBase.__init__(self, logger, id_, name)
        self.__location = "ipc://%s" % (socketPath,)
        self.__socket = None
        self.__socketPath = socketPath
        self.__serializer = serializer
        
#-----------------------------------------------------------------------------------------------------------------------        
    def bind(self):
        """ Bind to the queue as its single reader
        """
        
        context = zmq.Context.instance()
        self.__socket = context.socket(zmq.PULL)
        
        try:
            self._logger.debug2("Binding to socket: location=%s", self.__location)
            self.__socket.bind(self.__location)
        
        except:
            self._logger.error("Binding failed: location=%s", self.__location)
            
            raise  
    
#-----------------------------------------------------------------------------------------------------------------------    
    def connect(self):
        """ Connect to the queue as a writer
        @param timeout: Seconds to wait before declaring the queue unaccessible
        """
        
        context = zmq.Context.instance()
        self.__socket = context.socket(zmq.PUSH)
        
        self._logger.debug2("Connecting to socket: location=%s", self.__location)        
    
        try:
            self.__socket.connect(self.__location)
                   
        except:
            self._logger.error("Connection failed: location=%s", self.__location)      

            raise     

#----------------------------------------------------------------------------------------------------------------------- 
    def unbind(self):
        """ Unbind from this queue
        """
        
        self._logger.debug2("Closing socket: location=%s", self.__location)            
        self.__socket.close()
        
        try:
            self._logger.debug2("Deleting socket file: location=%s", self.__location)       
            os.remove(self.__socketPath)
        
        except OSError:
            pass

#----------------------------------------------------------------------------------------------------------------------- 
    def disconnect(self):
        """ Disconnect from this queue
        """
        
        self._logger.debug2("Closing socket: location=%s", self.__location)     
        self.__socket.close()
           
#-----------------------------------------------------------------------------------------------------------------------    
    def push(self, element):
        """
        @see Base class
        """
        
        try:
            
            try:
                self.__socket.send(self.__serializer.serialize(element))
                
            except zmq.ZMQError as e:
                
                # In case of a SIGINT we want to continue till gracefull exit
                if e.errno == errno.EINTR:  
                    
                    return    
                
                raise

        except:
            self._logger.error("Push failed: element=%s", element)
            
            raise  
    
#-----------------------------------------------------------------------------------------------------------------------    
    def pop(self):
        """
        @see Base class
        """
        
        try:
            
            try:
                string = self.__socket.recv(flags=zmq.NOBLOCK)
                
                if string is not None:
                 
                    return self.__serializer.deserialize(string)
                    
                return None
            
            except zmq.Again:
                
                return None
            
            except zmq.ZMQError as e:
                
                # In case of a SIGINT we want to continue till gracefull exit
                if e.errno == errno.EINTR:
                    
                    return None
                
                raise 
            
        
        except:
            self._logger.error("Pop failed")
            
            raise
     
#=======================================================================================================================
class _SerializerInterface:
    
#-----------------------------------------------------------------------------------------------------------------------    
    def serialize(self, object_):
        """ Serialize an object
        @attention: To be implemented by derived
        @param object_: Object to serialize
        @return A serialized string
        """
        
        pass

#-----------------------------------------------------------------------------------------------------------------------    
    def deserialize(self, string):
        """ Deserialize an object from a string 
        @attention: To be implemented by derived    
        @param string: String to deserialize from    
        @return: Deserialized object
        """
        
        pass    
    
#=======================================================================================================================
class _SerializerPickle(_SerializerInterface):
    
#-----------------------------------------------------------------------------------------------------------------------    
    def serialize(self, object_):
        """ 
        @see: Base class
        """
        
        return cPickle.dumps(object_, cPickle.HIGHEST_PROTOCOL)

#-----------------------------------------------------------------------------------------------------------------------    
    def deserialize(self, string):
        """ 
        @see: Base class
        """
        
        return cPickle.loads(string)  
    
#=======================================================================================================================
class _SerializerMessagePack(_SerializerInterface):
    
#-----------------------------------------------------------------------------------------------------------------------    
    def serialize(self, object_):
        """ 
        @see: Base class
        """
        
        return msgpack.dumps(object_, use_bin_type = True)

#-----------------------------------------------------------------------------------------------------------------------    
    def deserialize(self, string):
        """ 
        @see: Base class
        """
        
        return msgpack.loads(string, use_list = False)            