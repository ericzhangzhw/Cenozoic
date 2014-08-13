package com.graphconcern.status;

import java.io.Serializable;
import java.util.Date;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ServiceStatus implements Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private Date serverTime;
    private long usedMemory;
    private long totalMemory;
    private long maxMemory;
    private long freeMemory;
    private long activeThreads;

    public Date getServerTime()
    {
        return serverTime;
    }

    public void setServerTime(Date serverTime)
    {
        this.serverTime = serverTime;
    }

    public long getUsedMemory()
    {
        return usedMemory;
    }

    public void setUsedMemory(long usedMemory)
    {
        this.usedMemory = usedMemory;
    }

    public long getTotalMemory()
    {
        return totalMemory;
    }

    public void setTotalMemory(long totalMemory)
    {
        this.totalMemory = totalMemory;
    }

    public long getMaxMemory()
    {
        return maxMemory;
    }

    public void setMaxMemory(long maxMemory)
    {
        this.maxMemory = maxMemory;
    }

    public long getActiveThreads()
    {
        return activeThreads;
    }

    public void setActiveThreads(long activeThreads)
    {
        this.activeThreads = activeThreads;
    }

    public long getFreeMemory()
    {
        return freeMemory;
    }

    public void setFreeMemory(long freeMemory)
    {
        this.freeMemory = freeMemory;
    }

   

}
