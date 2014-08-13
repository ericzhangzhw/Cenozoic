package com.graphconcern.cenozoic.connector.init;

import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.WebApplicationContext;

import com.graphconcern.cenozoic.connector.dispatcher.Dispatcher;
import com.graphconcern.cenozoic.connector.dispatcher.DispatchLoader;
import com.graphconcern.cenozoic.support.config.SystemConfig;

public class Startup {
	private static final Logger log = LoggerFactory.getLogger(Startup.class);

	@Autowired
	private ApplicationContext applicationContext;

	private static SystemConfig config;
	
	public void init() {
		log.info("Starting service @" + applicationContext);

		config = new SystemConfig((WebApplicationContext) applicationContext);
		
		String wd = config.getString("workDirectory");
		if (wd == null) {
			log.error("Unable to start because workDirectory is missing from config");
			return;
		}
		File rootWorkDir = new File(wd);
		if (!rootWorkDir.exists()) {
			log.error("Unable to start because workDirectory is not found - " + rootWorkDir.getPath());
			return;
		}
		String domainId = config.getString("domainId");
		if (domainId == null) {
			log.error("Unable to start because domainId is missing in config");
			return;
		}
		/*
		 * Set limit to number of connector instances
		 */
		int n = config.getInt("concurrent.storageConnector");
		if (n < 3) n = 3;
		if (n > 10) n = 10;
		/*
		 * Start connector instances
		 */
		for (int i=0; i < n; i++) {
			DispatchLoader dispatcher = new DispatchLoader();
			dispatcher.start();
		}

		config.setReady(true);
	}
	
	public void shutdown() {
		/*
		 * Close dispatcher instances
		 */
		Dispatcher.shutdown();
		
		if (config != null ) config.shutdown();
		
		log.info("Service stopped @"+applicationContext);

	}
	
}
