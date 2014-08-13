package com.graphconcern.cenozoic.search.engine;

import java.util.concurrent.ConcurrentHashMap;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.graphconcern.cenozoic.support.util.Constants;

public class LockListener implements MessageListener {
	private static final Logger log = LoggerFactory.getLogger(LockListener.class);
	
	private static ConcurrentHashMap<String, CqlLock> allLocks;
	
	private JSONParser json = new JSONParser();
	
	public LockListener() {
		allLocks = CqlLock.getLocks();
		log.info("Initialized");
	}

	@Override
	public void onMessage(Message message) {
		if (message instanceof TextMessage) {
			TextMessage text = (TextMessage) message;
			
			try {
				JSONObject o = (JSONObject) json.parse(text.getText());
				if (o.containsKey(Constants.REQUEST_ID) && o.containsKey(Constants.TYPE) && o.containsKey("path") && o.containsKey("name") && o.containsKey("result")) {
					String requestId = (String) o.get(Constants.REQUEST_ID);
					String type = (String) o.get(Constants.TYPE);
					String path = (String) o.get("path");
					String name = (String) o.get("name");
					boolean result = (boolean) o.get("result");
					String compositeKey = path+"/"+name+"/"+requestId;
					
					if (requestId != null && type != null && allLocks.containsKey(compositeKey)) {
						if (type.equals("status") || type.equals("obtain")) {
							CqlLock lock = allLocks.get(compositeKey);
							lock.setLocked(result);
							lock.wakeUp();
							log.debug("--------->"+o.toString());
						}
					}
				}
			} catch (JMSException e) {
				e.printStackTrace();
			} catch (ParseException e1) {
				// ignore invalid json string
			}
		}
		
	}
	
}
