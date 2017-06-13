import orchestration
import inspect 
import os
import sys

#=======================================================================================================================
class Services(object):
    
#-----------------------------------------------------------------------------------------------------------------------    
    def __init__(self, testCase):
        """ C'tor
        @param testCase: Test case which is using services 
        """
        
        # Top level logger
        self.__logger = orchestration.logger._getTopLogger().createAdapter("UnitTestServices")
        
        # Get the test case path
        self.__testCasePath = os.path.relpath(os.path.dirname(inspect.getfile(testCase.__class__)), '.')
        
#-----------------------------------------------------------------------------------------------------------------------    
    @property
    def logger(self):
        """ Get the services logger
        """
        
        return self.__logger
        
#-----------------------------------------------------------------------------------------------------------------------    
    @property
    def testCasePath(self):
        """ Get the test case path
        """
        
        return self.__testCasePath 
    
#-----------------------------------------------------------------------------------------------------------------------    
    def run(self, job, defaultConfigurationFilesPaths, args = []):
        """ Run an orchestration job within a unit test
        @param: job: Job to run
        @see: orchestration.JobBase
        @param defaultConfigurationFilesPath: List of default configurations files paths
        @param args: Optional command line arguments
        @return True on success 
        """    
        
        # Remove command line arguments added by pydev unit test runner 
        for i in xrange(0, len(sys.argv)):
            if "--port" == sys.argv[i]:
                del sys.argv[i+1]
                del sys.argv[i]
                
                break
        
        return orchestration.run(job, defaultConfigurationFilesPaths, args)
