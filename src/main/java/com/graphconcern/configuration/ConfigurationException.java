package com.graphconcern.configuration;

public class ConfigurationException extends Exception
{

  
    private static final long serialVersionUID = 1L;
    
    public ConfigurationException(String expectedKey, String configFile)
    {
        super("Could not find key:{" + expectedKey +"} in " + configFile + ".");    
    }
    
}
