package com.graphconcern.cenozoic.search.worker;

import java.io.IOException;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.graphconcern.cenozoic.search.engine.CqlLockDao;
import com.graphconcern.cenozoic.search.init.Startup;
import com.graphconcern.cenozoic.support.util.Constants;
import com.graphconcern.cenozoic.support.util.MqUtil;
import com.graphconcern.cenozoic.support.util.Utility;

public class LockMaster implements MessageListener {
	private static final Logger log = LoggerFactory.getLogger(LockMaster.class);
	
	private static Utility util = new Utility();
	
	private JSONParser json = new JSONParser();
	private CqlLockDao dao;
	private MqUtil mqUtil;
	
	public LockMaster() {
		dao = new CqlLockDao(Startup.getCassandraSession());
		mqUtil = Startup.getMq().getMqUtil();
		log.info("Initialized");
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onMessage(Message message) {
		
		if (message instanceof TextMessage) {
			TextMessage text = (TextMessage) message;
			
			try {
				JSONObject o = (JSONObject) json.parse(text.getText());
				if (o.containsKey(Constants.REQUEST_ID) && o.containsKey(Constants.TYPE) && o.containsKey("path") && o.containsKey("name") && o.containsKey("hostname")) {
					
					log.debug(util.json2str(o));
					
					String requestId = (String) o.get(Constants.REQUEST_ID);
					String type = (String) o.get(Constants.TYPE);
					String path = (String) o.get("path");
					String name = (String) o.get("name");
					String hostname = (String) o.get("hostname");
					if (requestId != null && type != null) {
						if (type.equals("status")) {
							Destination dest = message.getJMSReplyTo();
							if (dest != null) {
								boolean locked = dao.isLocked(path, name);
								o.put("result", locked);
								mqUtil.sendResponse(dest, util.json2str(o));
							}
						}
						if (type.equals("obtain")) {
							Destination dest = message.getJMSReplyTo();
							if (dest != null) {
								boolean obtained;
								try {
									obtained = dao.obtainLock(hostname, path, name, requestId);
								} catch (IOException e) {
									obtained = false;
								}
								o.put("result", obtained);
								mqUtil.sendResponse(dest, util.json2str(o));
							}
						}
						if (type.equals("close")) {
							try {
								dao.releaseLock(hostname, path, name, requestId);
							} catch (IOException e) {}
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
