package com.graphconcern.search.engine;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.graphconcern.search.init.Startup;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class KeepAlive extends Thread {
	private static final Logger log = LoggerFactory.getLogger(KeepAlive.class);
	
	private Session session;
	private PreparedStatement prepareUpdateLock;
	private String keepAlivePath;
	private String hostname;
	
	public KeepAlive(Session session, PreparedStatement prepareUpdateLock, String keepAlivePath) {
		this.session = session;
		this.prepareUpdateLock = prepareUpdateLock;
		this.keepAlivePath = keepAlivePath;
		this.hostname = Startup.getSystemConfig().getHostId();
	}
	
	private void keepAlive() {
		session.execute(prepareUpdateLock.bind(keepAlivePath, hostname, hostname, new Date(System.currentTimeMillis()), "-", false));
		log.debug(hostname+" keep-alive");
	}

	@Override
	public void run() {
		log.info("Started");
		
		long t0 = 0;
		while (true) {
			if (System.currentTimeMillis() - t0  > 30000) {
				t0 = System.currentTimeMillis();
				keepAlive();
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {}
			
		}
	}
}
