package com.graphconcern.connector.dispatcher;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;

import javax.websocket.ContainerProvider;
import javax.websocket.DeploymentException;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.graphconcern.support.config.SystemConfig;

public class DispatchLoader extends Thread {
	private static final Logger log = LoggerFactory.getLogger(DispatchLoader.class);
	
	private static SystemConfig config = SystemConfig.getInstance();

	private String uri;
	
	private static int totalInstance = 0;
	private int instance;
	private static boolean connectionError = false;
	
	public DispatchLoader() {
		instance = ++totalInstance;
		this.uri = config.getString("storageConnector.gatewayUrl");
	}

	public String getUri() {
		return uri;
	}

	@Override
	public void run() {
		if (uri == null) {
			log.error("Instance #"+instance+" - Unable to connect because Virtual Storage Gateway URL is undefined");
			return;
		}
		log.info("Instance #"+instance+" started");
		
		try {
			/*
			 * Keep connecting to virtual storage gateway until it is done
			 * because VSG may not be available when the storage connector is started.
			 */
			while (Dispatcher.isEnabled()) {
				Dispatcher.init();
				Random r = new Random();
				/*
				 * Randomly start process so that multiple web socket connections will be happen at once
				 * Range: 1 to 6 seconds
				 */
				try {
					Thread.sleep(r.nextInt(50) * 100 + 1000);
				} catch (InterruptedException e1) {}
				/*
				 * Try setting up a web socket connection
				 */
				try {
					WebSocketContainer container =  ContainerProvider.getWebSocketContainer();
					Session session = container.connectToServer(Dispatcher.class, new URI(uri));
					log.info("Instance #"+instance+", ws#"+session.getId()+" - connected to " + uri);
					connectionError = false;
					break;
				} catch (DeploymentException | IOException e) {
					if (!connectionError) {
						log.warn("Instance #"+instance+" - "+uri+" not responding - "+e.getMessage());
						connectionError = true;	// show only once
					}
				}
			}
			if (!Dispatcher.isEnabled()) log.info("Instance #"+instance+" - Loader aborted");

		} catch (URISyntaxException e1) {
			log.info("Instance #"+instance+" - URI exception - "+e1.getMessage());
		}
	}


}
