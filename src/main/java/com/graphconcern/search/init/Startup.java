package com.graphconcern.search.init;

import java.io.File;
import java.util.List;

import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.WebApplicationContext;

import com.graphconcern.search.util.UtilConstants;
import com.graphconcern.search.index.IndexController;
import com.graphconcern.search.worker.IndexWorker;
import com.graphconcern.search.worker.LockMaster;
import com.graphconcern.search.worker.OptimizeWorker;
import com.graphconcern.search.worker.SearchWorker;
import com.graphconcern.search.init.Startup;
import com.graphconcern.support.config.SystemConfig;
import com.graphconcern.support.util.EmbeddedMQ;
import com.graphconcern.support.util.MqUtil;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster.Builder;

public class Startup {

	private static final Logger log = LoggerFactory.getLogger(Startup.class);

	private static EmbeddedMQ mq;

	private static Cluster cassandraCluster;
	
	private static Session cassandraSession;
	
	public static File rootWorkDir;
	
	@Autowired
	private ApplicationContext applicationContext;

	private static IndexController indexController;
	
	private Thread controller;
	
	private static SystemConfig config;

	
	public static Session getCassandraSession() {
		return cassandraSession;
	}
	
	public static EmbeddedMQ getMq() {
		return mq;
	}
	
	public static SystemConfig getSystemConfig() {
		return config;
	}
	
	public void init() {
		log.info("Starting service @" + applicationContext);

		config = new SystemConfig((WebApplicationContext) applicationContext);

		String wd = config.getString("workDirectory");
		if (wd == null) {
			log.error("Unable to start because workDirectory is missing from config");
			return;
		}
		rootWorkDir = new File(wd);
		if (!rootWorkDir.exists()) {	
			log.error("Unable to start because workDirectory is not found - " + rootWorkDir.getPath());
			return;
		}
		
		/*
		 * Setup database
		 */
		if (!setupDatabase()) {
			log.error("Unable to start because database is not ready");
			return;
		}
		
		
		String domainId = config.getString("domainId");
		if (domainId == null) {
			log.error("Unable to start because domainId is missing in config");
			return;
		}


		/*
		 * cLucene setup goes here
		 */
		startIndexController();

		if (indexController==null || !this.controller.isAlive()) {
			log.error("Unable to start bucket manager.");
			return;
		}
		
		
		try {
			mq = new EmbeddedMQ();
			MqUtil mqUtil = mq.getMqUtil();
			int indexInstance = config.getInt(UtilConstants.CONCURRENT_INDEX_WORKER);
			if (indexInstance < 3) indexInstance = 3; //Make it at least 3 instances to serve.
			for (int i=0; i < indexInstance; i++) {
				mqUtil.createQueueMessageConsumer(UtilConstants.QUEUE_INDEX_WORKER, new IndexWorker());
			}
			
			int searchInstance = config.getInt(UtilConstants.CONCURRENT_SEARCH_WORKER);
			if (searchInstance < 3) searchInstance = 3; //Make it at least 3 instances to serve.
			for (int i=0; i < searchInstance; i++) {
				mqUtil.createQueueMessageConsumer(UtilConstants.QUEUE_SEARCH_WORKER, new SearchWorker());
			}
			/*
			 * Create lock master listener on the exclusive queue of LOCK_MASTER
			 */
			mqUtil.createExclusiveConsumer(UtilConstants.LOCK_MASTER, new LockMaster());
			
			/*
			 * Create Optimize Listener for processing the "read lock" during optimization on indexes 
			 */
			mqUtil.createTopicMessageConsumer(OptimizeWorker.getTopicName(), new OptimizeWorker());
			
		} catch (JMSException je) {
			je.printStackTrace();
		}
		config.setReady(true);

	}

	public void shutdown() {
		/*
		 * cLucene shutdown goes here
		 */
		stopIndexController();
		
		if (mq != null) {
			log.info("Closing MQ broker");
			mq.stopBroker();
		}
		if (config != null ) {
			config.shutdown();
		}
		if (cassandraSession != null) {
			log.info("Closing Cassandra session");
			cassandraSession.shutdown();
			if (cassandraCluster != null) cassandraCluster.shutdown();
		}
		log.info("Service stopped @"+applicationContext);

	}
	
	private boolean setupDatabase() {
		/*
		 * Setup connection to Cassandra
		 */
		try {
			
			
			int port = config.getInt("cassandra.clucene.port");
			if (port < 1024) {
				log.error("Unable to start because cassandra.clucene.port ("+port+") < 1024");
				return false;
			}
			List<String> hosts = config.getStringList("cassandra.clucene.hosts");
			if (hosts == null) {
				log.error("Unable to start because cassandra.clucene.server is missing in config");
				return false;
			}
			Builder builder = Cluster.builder().withPort(port);
			for (String h: hosts) builder.addContactPoint(h);
			cassandraCluster = builder.build();

			Metadata metadata = cassandraCluster.getMetadata();
			log.info("Connected to Cassandra cluster: "+metadata.getClusterName());

			for ( Host host : metadata.getAllHosts() ) {
				log.info("Datatacenter: " +
	                host.getDatacenter() + ", Host: " + host.getAddress() + ", Rack: "+ host.getRack());
			}
			String keyspace = config.getString("cassandra.clucene.keySpace");
			if (keyspace == null) {
				log.error("Unable to start because cassandra.clucene.keySpace is missing in config");
				return false;
			}
			cassandraSession = cassandraCluster.connect(keyspace);
			log.info("Cassandra ready "+hosts+" @"+port+", keyspace: "+keyspace);
			return true;

		} catch (Exception e1) {
			log.error("Cassandra service is NOT available - "+config.getStringList("cassandra.clucene.server")+" @"+
						config.getInt("cassandra.clucene.port")+", keyspace: "+config.getString("cassandra.clucene.keyspace"));
			return false;
		}
	}
	
	
	public static IndexController getIndexController() {
		return indexController;
	}
	private void startIndexController() {
		if (controller == null || !controller.isAlive()) {
			log.info("Starting Management Thread");
			indexController = new IndexController();
			indexController.init();
			controller = new Thread(indexController);
			controller.start();
		}
	}
	
	private void stopIndexController() {
		indexController.shutdown();
		
	}

}
