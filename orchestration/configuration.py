import collections
import ConfigParser
import orchestration
import os

#=======================================================================================================================
Field = collections.namedtuple("Field", ['name', 'argParseShortSwitch','argParseLongSwitch', 'configFileField', 'type',
                               'help','metavar'])
""" C'tor
@param: name Configuration field name, will also be the property name within the configuration object
@param: argParseShortSwitch Short switch for option parser (e.g. -s)
@param: argParseLongSwitch Long switch for option parser (e.g. --long)
@param: configFileField Name of configuration file field
@param: type One of 'int' 'float' 'string' 'bool'
@param: help Help string for this field
@param: metavar Metavar as used in ArgParser
@see ArgParser
"""

#=======================================================================================================================
class Configuration(object):
    
#-----------------------------------------------------------------------------------------------------------------------    
    def __init__(self, rawDict):
        """ C'tor
        @param rawDict: Raw configuration dict, which its keys and values will become this object's properties
        """
        
        self.__dict__.update(rawDict)
        
#=======================================================================================================================        
class ValidatorBase(object):
    
#-----------------------------------------------------------------------------------------------------------------------
    def __init__(self):
        
        # Top object, create logger directly from main logger
        self.__logger = orchestration.logger._getTopLogger().createAdapter("ConfigurationValidator")

#-----------------------------------------------------------------------------------------------------------------------    
    @property
    def logger(self): 
        """ Validator's logger
        """
        
        return self.__logger
    
#-----------------------------------------------------------------------------------------------------------------------    
    def validate(self, configuration):
        """ Validate configuration
        @attention: To be implemented 
        @param configuration: Orchestration configuration object
        @return true if validated
        """
        
        return True
         
#=======================================================================================================================
class Builder(object):
    
    __DEFAULT_CONFIGURATION_FILE = os.path.join(os.path.realpath(os.path.dirname(__file__)), 'default.conf')

#-----------------------------------------------------------------------------------------------------------------------    
    def __init__(self, argParseGroup, configFileSection, validator, fields):
        """ C'tor
        @param argParseGroup: Option parser group name
        @see: argparse.ArgumentParser
        @param fields: List of fields
        @see Field
        @param validator: Validator to validate given configuration
        @see ValidatorBase
        """
        
        self.__argParseGroup = argParseGroup
        self.__configFileSection = configFileSection
        self.__fields = fields
        self.__validator = validator
        
#-----------------------------------------------------------------------------------------------------------------------             
    def _addArgParseArgumentGroup(self, argParser):
        """ Add an argument parser group to a given option parser
        @param argParser: Arguments parser to add an argument parser group for
        @see: argparse.ArgumentParser
        """
        
        group = argParser.add_argument_group(title = self.__argParseGroup)
        
        for field in self.__fields:
            
            if field.argParseShortSwitch:
                group.add_argument(field.argParseShortSwitch, field.argParseLongSwitch, dest = field.configFileField, 
                                   help = field.help, metavar = field.metavar, type = field.type)
                
            else:
                group.add_argument(field.argParseLongSwitch, dest = field.configFileField, help = field.help, 
                                   metavar = field.metavar, type = field.type)                
            
#-----------------------------------------------------------------------------------------------------------------------             
    def _buildConfiguration(self, argParseNamespace, defaultConfigurationFiles):
        """ Build a configuration object from a raw configuration dict
        @param argParseNamespace: Argument parser namespace results
        @param defaultConfigurationFiles: List of default configuration files
        @return: Orchestration configuration object
        """ 
        
        rawDict = {}
        
        for field in self.__fields:
            value = vars(argParseNamespace).get(field.configFileField, None)
            
            if isinstance(value, str):
                value = os.path.expandvars(str(value))
                
            rawDict[field.name] = value
            
        userConfigurationFile =  rawDict.get('userConfigurationFile', None)

        if userConfigurationFile is not None:
            defaultConfigurationFiles = [userConfigurationFile] + defaultConfigurationFiles
            
        defaultConfigurationFiles.append(self.__DEFAULT_CONFIGURATION_FILE)
        
        for configurationFile in defaultConfigurationFiles:
        
            configFileParser = ConfigParser.SafeConfigParser()
            configFileParser.read(configurationFile)
            
            for field in self.__fields:
                
                if rawDict[field.name] is None and configFileParser.has_option(self.__configFileSection, 
                                                                               field.configFileField):
                
                    if field.type == int:
                        value = configFileParser.getint(self.__configFileSection, field.configFileField)
                        
                    elif field.type == float:
                        value = configFileParser.getfloat(self.__configFileSection, field.configFileField)    
                        
                    elif field.type == bool:
                        value = configFileParser.getboolean(self.__configFileSection, field.configFileField)     
                        
                    else:
                        value = configFileParser.get(self.__configFileSection, field.configFileField)         
                        value = os.path.expandvars(str(value)) if value is not None else None          
                        
                    rawDict[field.name] = value           
            
        config = Configuration(rawDict)
      
        rc = self.__validator.validate(config)
        assert(rc != None)
      
        if not rc:
            raise orchestration.QuietError()
        
        return config
