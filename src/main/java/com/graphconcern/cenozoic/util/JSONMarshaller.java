package com.graphconcern.cenozoic.util;
import java.io.File;
import java.io.InputStream;
import java.net.URL;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class JSONMarshaller
{
    private static volatile JSONMarshaller instance = null;
    private ObjectMapper mapper;
    private static Logger LOG = LoggerFactory.getLogger(JSONMarshaller.class);
    
    private JSONMarshaller()
    {
        mapper = new ObjectMapper();
    }
    
   public void writeNulls(boolean flag) {
	   if (!flag){
		   mapper.setSerializationInclusion(Inclusion.NON_NULL);  
	   } else {
		   mapper.setSerializationInclusion(Inclusion.ALWAYS);
	   }
	   
	 
   }
    
    public static JSONMarshaller getInstance()
    {
        if (instance == null)
        {
            synchronized (JSONMarshaller.class)
            {
                if (instance == null)
                {
                    instance = new JSONMarshaller();
                }
            }
        }
        return instance;
    }
    public String marshall(Object value)
    {
        try
        {
            return mapper.writeValueAsString(value);
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            LOG.error("Unabled to marshall.", e);
            e.printStackTrace();
        }
        return null;
    }
    
    public <T> T unMarshall(Class<T> valueType, String content)
    {
        try
        {
            return mapper.readValue(content, valueType);
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            LOG.error("Unabled to un-marshall.", e);
            e.printStackTrace();
        }
        return null;
    }
    
    public <T> T unMarshall(Class<T> valueType, File file)
    {
        try
        {
            return mapper.readValue(file, valueType);
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            LOG.error("Unabled to un-marshall.", e);
            e.printStackTrace();
        }
        return null;
    }
    
    public <T> T unMarshall(Class<T> valueType, InputStream in)
    {
        try
        {
            return mapper.readValue(in, valueType);
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            LOG.error("Unabled to un-marshall.", e);
            e.printStackTrace();
        }
        return null;
    }
    
    public <T> T unMarshallFromURLString(Class<T> valueType, String strUrl)
    {
        try
        {
            URL url = new URL(strUrl);
            return mapper.readValue(url, valueType);
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            LOG.error("Unabled to un-marshall.", e);
            e.printStackTrace();
        }
        return null;
    }
    
    public Object unMarshall(TypeReference valueTypeRef, String content)
    {
        try
        {
            return mapper.readValue(content, valueTypeRef);
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            LOG.error("Unabled to un-marshall.", e);
            e.printStackTrace();
        }
        return null;
    }
    
    public Object unMarshall(TypeReference valueTypeRef, InputStream in)
    {
        try
        {
            return mapper.readValue(in, valueTypeRef);
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            LOG.error("Unabled to un-marshall.", e);
            e.printStackTrace();
        }
        return null;
    }
    
    
}