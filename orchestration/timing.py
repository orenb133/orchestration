import os
import shutil
import time

#=======================================================================================================================
class _Poller(object):

#-----------------------------------------------------------------------------------------------------------------------    
    def __init__(self, func, interval):
        """ C'tor
        @param func: Callable to call when reaching interval
        @param interval: Interval in seconds
        """
         
        self.__func = func
        self.__interval = interval
        self.__nextTime = time.time() + interval
        
#-----------------------------------------------------------------------------------------------------------------------        
    def poll(self):
        """ Poll and execute callable if interval was reached
        @return: Seconds left till next execution
        """
        
        currentTime = time.time()
        
        if currentTime >= self.__nextTime:
            self.__func()
            self.__nextTime = self.__nextTime + self.__interval
            
        return max(self.__nextTime - currentTime, 0)               