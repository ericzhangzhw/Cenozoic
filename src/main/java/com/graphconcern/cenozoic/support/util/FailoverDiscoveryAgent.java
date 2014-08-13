/** 
/*Graph Concern Inc Confidential Information.
/* TM and (c) 2013 Graph Concern Inc.  All Rights Reserved.
/* Reproduction in whole or in part without prior written permission of a
/* duly authorized representative is prohibited.
 * 
 * Author: Felix Chiu
 * Date: 3/20/2014
 */
package com.graphconcern.cenozoic.support.util;

import java.net.URI;
import java.util.List;

import org.apache.activemq.transport.discovery.simple.SimpleDiscoveryAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A static DiscoveryAgent that supports connecting to a Master / Slave tuple
 * of brokers.
 */
public class FailoverDiscoveryAgent extends SimpleDiscoveryAgent {

    private final static Logger log = LoggerFactory.getLogger(FailoverDiscoveryAgent.class);

    private String[] msServices = new String[]{};
    private boolean failover = true;

    @Override
    public String[] getServices() {
        return msServices;
    }

    @Override
    public void setServices(String services) {
        this.msServices = services.split(",");
        configureServices();
    }

    @Override
    public void setServices(String services[]) {
        this.msServices = services;
        configureServices();
    }
    
    public void setServices(List<String> serviceList, boolean failover) {
    	this.failover = failover;
    	if (!serviceList.isEmpty()) {
    		String[] services = new String[serviceList.size()];
    		for (int i=0; i < serviceList.size(); i++) {
    			services[i] = serviceList.get(i);
    		}
    		setServices(services);
    	}
    }

    @Override
    public void setServices(URI services[]) {
        this.msServices = new String[services.length];
        for (int i = 0; i < services.length; i++) {
            this.msServices[i] = services[i].toString();
        }
        configureServices();
    }

    protected void configureServices() {
    	if (msServices == null || msServices.length < 2) {
            log.error("You must specify as least 2 failover URIs");
            msServices = new String[]{};
            throw new IllegalArgumentException("You must specify as least 2 failover URIs");
    	}
        StringBuffer buf = new StringBuffer();
        buf.append("failover:(");
        for (int i = 0; i < (msServices.length - 1); i++) {
        	buf.append("tcp://");
            buf.append(msServices[i]);
            buf.append(',');
        }
        buf.append("tcp://");
        buf.append(msServices[msServices.length - 1]);
        buf.append(")?randomize=");
        buf.append(failover? "true":"false");
        buf.append("&maxReconnectAttempts=0");
            
        super.setServices(new String[]{buf.toString()});
    }

}
