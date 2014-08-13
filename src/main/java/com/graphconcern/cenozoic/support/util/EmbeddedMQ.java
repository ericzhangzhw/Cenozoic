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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.network.DiscoveryNetworkConnector;

import javax.jms.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.graphconcern.cenozoic.support.config.SystemConfig;

public class EmbeddedMQ {
	private static final Logger log = LoggerFactory.getLogger(EmbeddedMQ.class);
	private static Utility util = new Utility();
	
	private static ActiveMQDestination allQueues = new ActiveMQQueue(">");
	private static ActiveMQDestination internalQueues = new ActiveMQQueue(Constants.PRIVATE_MQ_PREFIX+">");
	private static ActiveMQDestination allTopics = new ActiveMQTopic(">");
	private static ActiveMQDestination internalTopics = new ActiveMQTopic(Constants.PRIVATE_MQ_PREFIX+">");

	private static int TIMEOUT = 10000; // 10 seconds
	private static String mqConnector, clientConnector;
	private static Connection mqConnection = null;
	private static BrokerService localBroker = null;
	private static DiscoveryNetworkConnector networkConnector = null, topicConnector = null;
	private static String brokerName;
	private static SystemConfig config;
	private static MqUtil mqUtil;
	
	public EmbeddedMQ() { 
		config = SystemConfig.getInstance();
		int mqPort = config.getInt("mqConfiguration.port");
		if (mqPort < 1024) {
			log.error("Unable to start because MQ port ("+mqPort+") < 1024");
			return;
		}
		/*
		 * Embedded MQ listens to localhost so other platform apps on the same VM can share the Embedded MQ
		 */
		mqConnector = "nio://127.0.0.1:"+mqPort;
		/*
		 * Use failover transport to connect to an existing Embedded MQ
		 */
		clientConnector = "failover:(tcp://127.0.0.1:"+mqPort+",tcp://127.0.0.1:"+mqPort+")";
		/*
		 * Test if there is an existing Embedded MQ
		 */
		if (util.portReady("127.0.0.1", mqPort, TIMEOUT)) {
			log.info("Local MQ already loaded");
		} else {
			startBroker();
		}
		getMQConnection();
	}
	
	private void startBroker() {
		/*
		 * brokerName must be unique for proper connectivity to Network MQ
		 */
		brokerName = config.getHostId();
		
		String mqDirOffset = config.getString("mqConfiguration.directoryOffset");
		if (mqDirOffset == null) mqDirOffset = "mq";
		File mqDir = new File(config.getString("workDirectory"), mqDirOffset);
    	/*
    	 * Create local MQ broker
    	 * setDedicatedTaskRunner(false) may be used to reduce memory consumption at the price of slower performance.
    	 */
    	localBroker = new BrokerService();
		localBroker.setUseJmx(false);	
		localBroker.setBrokerName(brokerName);
		localBroker.setDataDirectoryFile(mqDir);
        try {
    	    /*
             * setup local connector
             */
        	localBroker.addConnector(mqConnector);
        	
        	int consolePort = config.getInt("mqConfiguration.consolePort");
        	if (consolePort < 1024) {
        		consolePort = config.getInt("mqConfiguration.port") + 1000;
        		log.warn("mqConfiguration.consolePort < 1024 so it is reset to "+consolePort + "(i.e. mqConfiguration.port + 1000)");
        	}
        	localBroker.setUseJmx(true);
        	localBroker.getManagementContext().setConnectorPort(consolePort);
        	/*
        	 * Connect to network brokers if any
        	 */
        	setupConnector(true);
        	setupConnector(false);
        	/*
        	 * start local broker and connection
        	 */
        	log.info("Starting MQ broker");
        	localBroker.start();
        	    	
        } catch (Exception e) {
        	log.error("Unable to start MQ ("+mqConnector+") "+e.getMessage());
        	return;
    	};
		
	}
	
	private void setupConnector(boolean optimizedForTopic) throws Exception {
		DiscoveryNetworkConnector connector = optimizedForTopic? topicConnector : networkConnector;
    	/*
    	 * Connect to network brokers if any
    	 */
    	boolean networkMq = false;
		List<String> mqDiscoveryURIs = config.getHostPortList("mqConfiguration.discoveryURI");
    	if (mqDiscoveryURIs != null) {
			if (!mqDiscoveryURIs.isEmpty()) {
				/*
				 * If there is only one URI, duplicate it so MQ thinks there are two URIs.
				 */
				if (mqDiscoveryURIs.size() == 1) mqDiscoveryURIs.add(mqDiscoveryURIs.get(0));
				boolean failover = config.getBoolean("mqConfiguration.failover");
				String username = config.getString("mqConfiguration.username");
				String password = config.getString("mqConfiguration.password");
				int networkTTL = config.getInt("mqConfiguration.networkTTL");
				if (networkTTL < 1) {
					log.warn("MQ networkTTL ("+networkTTL+") is invalid - reset to 2");
					networkTTL = 2;
				}
        		/*
        		 * Add connections to network brokers
        		 * Set conduitSubscriptions must be set to false to enable load balancing between message consumers
        		 */
        		log.info("Starting network connector "+mqDiscoveryURIs+", networkTTL="+networkTTL+", failover? "+failover+", "+(optimizedForTopic ? "optimized for topics" : "optimized for queues"));
        		
        		/*
        		 * Setup discovery agent
        		 * FailoverDiscoveryAgent will connect to one MQ node only.
        		 */
        		FailoverDiscoveryAgent agent = new FailoverDiscoveryAgent();
        		agent.setServices(mqDiscoveryURIs, failover);
        		/*
        		 * Setup and configure network connector
        		 */
        		connector = new DiscoveryNetworkConnector();
        		connector.setName((optimizedForTopic? "t_" : "q_") +brokerName);
        		connector.setDiscoveryAgent(agent);
        		connector.setNetworkTTL(networkTTL);
        		connector.setDuplex(true);
        		connector.setConduitSubscriptions(optimizedForTopic);
        		/*
        		 * Skip local queues and topics
        		 */
        		List<ActiveMQDestination> exclusion = new ArrayList<ActiveMQDestination>();
        		exclusion.add(optimizedForTopic? allQueues : allTopics);
        		exclusion.add(internalTopics);
        		exclusion.add(internalQueues);
        		connector.setExcludedDestinations(exclusion);
        		connector.setBridgeTempDestinations(optimizedForTopic);
        		
        		log.info("Excludes: "+exclusion);
        	
        		if (username != null && password != null) {
        			connector.setUserName(username);
        			connector.setPassword(password);
            		log.info("Network connector authenticates as \""+username+"\"");
        		} else {
        			log.info("Network connector authenticates as anonymous user");
        		}
        		localBroker.addNetworkConnector(connector);
        		networkMq = true;
			}
    	}
    	if (!networkMq) log.warn("Not connecting to Network MQ ("+(optimizedForTopic ? "topics" : "queues")+") properly - please check mqConfiguration");
	}
	
	public Connection getMQConnection() {
		if (mqConnection != null) return mqConnection;
		String target = localBroker == null? clientConnector : "vm://"+brokerName+"?create=false";
		try {				
			ActiveMQConnectionFactory connFactory = new ActiveMQConnectionFactory(target);
            mqConnection = connFactory.createConnection();
            mqConnection.start();
            log.info("MQ connection started at "+target);
			return mqConnection;
	    } catch (Exception e) {
	    	log.error("Unable to connect to MQ ("+target+") "+e.getMessage());
	    	return null;
		}	
	}
	public MqUtil getMqUtil() {
		if (mqUtil == null) mqUtil = new MqUtil(getMQConnection());
		return mqUtil;
	}
	
	/*
	 * Orderly shutdown
	 */
	public void stopBroker() {
		try {
			if (mqUtil != null) mqUtil.shutdown();
			if (mqConnection != null) mqConnection.stop();
			if (networkConnector != null) networkConnector.stop();
			if (topicConnector != null) topicConnector.stop();
			if (localBroker != null) localBroker.stop();
			
			log.info("MQ stopped successfully");
		} catch (Exception e) {
        	log.error("Unable to stop MQ ("+mqConnector+") "+e.getMessage());
        	return;
		}
	}


}
