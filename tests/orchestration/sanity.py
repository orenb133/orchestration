import aenum
import multiprocessing
import orchestration.events
import orchestration.unittest
import os
import unittest

#=======================================================================================================================
class TestSanity(unittest.TestCase):
            
#-----------------------------------------------------------------------------------------------------------------------            
    def setUp(self):
        
        self.services = orchestration.unittest.Services(self)
       
#---------------------------------------------------------------------------------------------------------------------    
    def test_sanity(self):
         
        class JobSanity(orchestration.JobBase):
            
                def prepareOrchestrator(self, configuration, orchestrator, stopRun):
         
                    def onStart(channel):
                        # Sending 1 broadcast
                        channel.broadcast(orchestration.events.Event(_EventsTypesE.PING,
                                                                     _DataTypesE.BROADCAST.value, 13))
                        
                        # Sending 1 unicast
                        channel.unicast(orchestration.events.Event(_EventsTypesE.PING, 
                                                                   _DataTypesE.UNICAST.value, 13), 
                                                                   orchestration.events.HandlerIdentifier(
                                                                       _HandlersTypesE.HARD_WORKER, 1))
                        
                        # Sending 2 anycast
                        channel.anycast(orchestration.events.Event(_EventsTypesE.PING, 
                                                                   _DataTypesE.ANYCAST.value, 13), 
                                                                   _HandlersTypesE.HARD_WORKER)      
                        
                    
                        channel.anycast(orchestration.events.Event(_EventsTypesE.PING, 
                                                                   _DataTypesE.ANYCAST.value, 13), 
                                                                   _HandlersTypesE.HARD_WORKER)  
                        
                        # Sending 1 multicast
                        channel.multicast(orchestration.events.Event(_EventsTypesE.PING, 
                                                                     _DataTypesE.MULTICAST.value, 13), 
                                                                     _HandlersTypesE.HARD_WORKER)           
                    
                    
                    executer1 = orchestrator.createExecuter()
                    executer1.registerEventsHandler(_HandlerHardWorker("hard-worker-0"))
                    executer1.registerEventsHandler(_HandlerLightWorker("light-worker-0"))
                    
                    executer2 = orchestrator.createExecuter()
                    executer2.registerEventsHandler(_HandlerHardWorker("hard-worker-1"))
                    executer2.registerEventsHandler(_HandlerLightWorker("light-worker-1")) 
                    
                    manager = multiprocessing.Manager()
                    self.masterReceivedEvents = manager.dict()
                    masterHandler = _HandlerMaster("master", onStart, stopRun, self.masterReceivedEvents) 
                    executer3 = orchestrator.createExecuter()
                    executer3.registerEventsHandler(masterHandler)
                    
        configFile = os.path.join(self.services.testCasePath, "sanity.conf")
        job = JobSanity("sanity")
        self.assertTrue(self.services.run(job, [configFile]))
        
        # Expecting response from all workers for broadcast
        self.assertTrue(orchestration.events.HandlerIdentifier(_HandlersTypesE.HARD_WORKER, 0) in 
                    job.masterReceivedEvents[_DataTypesE.BROADCAST.value])
        
        self.assertTrue(orchestration.events.HandlerIdentifier(_HandlersTypesE.HARD_WORKER, 1) in 
                    job.masterReceivedEvents[_DataTypesE.BROADCAST.value])
        
        self.assertTrue(orchestration.events.HandlerIdentifier(_HandlersTypesE.LIGHT_WORKER, 0) in 
                    job.masterReceivedEvents[_DataTypesE.BROADCAST.value])
        
        self.assertTrue(orchestration.events.HandlerIdentifier(_HandlersTypesE.LIGHT_WORKER, 1) in 
                    job.masterReceivedEvents[_DataTypesE.BROADCAST.value]) 
        
        self.assertEqual(len(job.masterReceivedEvents[_DataTypesE.BROADCAST.value]), 4)
        
        # Expecting single response from specific worker in unicast
        self.assertTrue(orchestration.events.HandlerIdentifier(_HandlersTypesE.HARD_WORKER, 1) in 
                    job.masterReceivedEvents[_DataTypesE.UNICAST.value])
        
        self.assertEqual(len(job.masterReceivedEvents[_DataTypesE.UNICAST.value]), 1)
        
        # Expecting 2 response from same worker in anycast
        self.assertTrue(orchestration.events.HandlerIdentifier(_HandlersTypesE.HARD_WORKER, 1) in 
                    job.masterReceivedEvents[_DataTypesE.ANYCAST.value])     
        
        self.assertEqual(len(job.masterReceivedEvents[_DataTypesE.ANYCAST.value]), 2)  
        
        # Expecting response from all hard workers in multicast
        self.assertTrue(orchestration.events.HandlerIdentifier(_HandlersTypesE.HARD_WORKER, 0) in 
                    job.masterReceivedEvents[_DataTypesE.MULTICAST.value])
        
        self.assertTrue(orchestration.events.HandlerIdentifier(_HandlersTypesE.HARD_WORKER, 1) in 
                    job.masterReceivedEvents[_DataTypesE.MULTICAST.value])
        
        self.assertEqual(len(job.masterReceivedEvents[_DataTypesE.MULTICAST.value]), 2)                 

#=======================================================================================================================
class _HandlersTypesE(aenum.IntEnum):
    MASTER = 0
    HARD_WORKER = 1
    LIGHT_WORKER = 2 

#=======================================================================================================================
class _EventsTypesE(aenum.IntEnum):
    PING = 0
    PONG = 1  

#=======================================================================================================================    
class _DataTypesE(aenum.IntEnum):
    UNICAST = 0
    BROADCAST = 1
    ANYCAST = 2
    MULTICAST = 3
    
#=======================================================================================================================    
class _HandlerMaster(orchestration.events.HandlerBase):

#---------------------------------------------------------------------------------------------------------------------    
    def __init__(self, name, onStart, stopRun, receivedEvents):
        orchestration.events.HandlerBase.__init__(self, _HandlersTypesE.MASTER, name)
        self.__onStart = onStart
        self.__receivedEvents = receivedEvents
        self.__numReceivedEvents = 0
        self.__stopRun = stopRun
        
#---------------------------------------------------------------------------------------------------------------------            
    @property            
    def receivedEvents(self): return self.__receivedEvents
    
#---------------------------------------------------------------------------------------------------------------------        
    @property
    def numReceivedEvents(self): return self.__numReceivedEvents

#---------------------------------------------------------------------------------------------------------------------    
    def start(self, channel):
        self.__onStart(channel)

#---------------------------------------------------------------------------------------------------------------------            
    def handleEvent(self, event, header, channel):
        self.logger.info("Received event with data: data=%s" , event.data)

        if event.data not in self.__receivedEvents:
            self.__receivedEvents[event.data] = []
            
        # Need to assign local list to shared dict for change to propagate
        localList = self.__receivedEvents[event.data]
        localList.append(header.sourceHandlerId)
        self.__receivedEvents[event.data] = localList
       
        self.__numReceivedEvents += 1
        
        if self.__numReceivedEvents >= 9:
            self.__stopRun()

#=======================================================================================================================        
class _HandlerHardWorker(orchestration.events.HandlerBase):
    
    def __init__(self, name):
        orchestration.events.HandlerBase.__init__(self, _HandlersTypesE.HARD_WORKER, name)

    def handleEvent(self, event, header, channel):
        self.logger.info("Received event with data: data=%s", event.data)
        
        channel.unicast(orchestration.events.Event(_EventsTypesE.PONG, event.data, event.contextId), 
                        header.sourceHandlerId)

#=======================================================================================================================        
class _HandlerLightWorker(orchestration.events.HandlerBase):
    
    def __init__(self, name):
        orchestration.events.HandlerBase.__init__(self, _HandlersTypesE.LIGHT_WORKER, name)

    def handleEvent(self, event, header, channel):
        self.logger.info("Received event with data: data=%s", event.data)
        channel.unicast(orchestration.events.Event(_EventsTypesE.PONG, event.data, event.contextId), 
                        header.sourceHandlerId)     

#=======================================================================================================================
if __name__ == '__main__':
    unittest.main()   