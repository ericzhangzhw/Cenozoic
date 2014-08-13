package com.graphconcern.cenozoic.status;

import java.util.Date;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/status")
public class ServiceStatusResource
{
    public static final int mb = 1024*1024;
    
    @GET
    @Path("/")
    @Produces({ MediaType.APPLICATION_JSON })
    public ServiceStatus getStatus() {
        Runtime runtime = Runtime.getRuntime();
        ServiceStatus status = new ServiceStatus();
        status.setActiveThreads(Thread.activeCount());
        status.setMaxMemory(runtime.maxMemory() / mb);
        status.setServerTime(new Date());
        long totalMem = runtime.totalMemory();
        long freeMem = runtime.freeMemory();
        status.setTotalMemory(totalMem / mb);
        status.setFreeMemory(freeMem / mb);
        status.setUsedMemory((totalMem - freeMem) / mb);
        return status;
    }
}
