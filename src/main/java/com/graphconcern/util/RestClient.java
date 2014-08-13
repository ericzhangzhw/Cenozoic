package com.graphconcern.util;



import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;



public class RestClient
{

 

    public static <T> T getClient(Class<T> serviceInterface, String url)
    {
    	  ResteasyClient client = new ResteasyClientBuilder().build();
          ResteasyWebTarget target = client.target(url);
          return target.proxy(serviceInterface);
    }
    
   

   
    
}