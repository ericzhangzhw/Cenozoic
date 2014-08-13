package com.graphconcern.web.exception;

import java.util.HashMap;

import javax.validation.ValidationException;
import javax.ws.rs.core.Response;

import org.jboss.resteasy.spi.MethodNotAllowedException;
import org.jboss.resteasy.spi.NotFoundException;

public class ExceptionToHttpStatusMap
{
    private static HashMap<Class<?>, Response.Status> map = null;

    public static Response.Status getStatusCode(Class<?> clazz)
    {
        // JIT map population
        if (map == null)
        {
            populateMap();
        }

        // Translate the exception to an HTTP Status Code
        Response.Status statusCode = map.get(clazz);
        if (statusCode == null)
        {
            statusCode = Response.Status.INTERNAL_SERVER_ERROR;
        }
        return statusCode;
    }

    private static void populateMap()
    {
        map = new HashMap<Class<?>, Response.Status>();

        map.put(NotFoundException.class, Response.Status.NOT_FOUND);
        map.put(ValidationException.class, Response.Status.FORBIDDEN);
        map.put(IllegalArgumentException.class, Response.Status.BAD_REQUEST);
        
        /*
         * We are returning NOT_FOUND instead of METHOD_NOT_ALLOWED because we do not wish 
         * (at this time at least) to return a list of allowed methods.  Also, when I tried
         * returning METHOD_NOT_ALLOWED (405), something else translated it into a Forbidden
         * (403) status.  Is 405 a deprecated status code?  It is not in the Response.Status 
         * enumeration.
         */
        map.put(MethodNotAllowedException.class, Response.Status.NOT_FOUND);
    }
}
