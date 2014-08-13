package com.graphconcern.search.worker;

import java.util.concurrent.ConcurrentHashMap;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import com.graphconcern.search.util.UtilConstants;
import com.graphconcern.support.util.Utility;

public class OptimizeWorker implements MessageListener {

	private enum Status {Request, Release};

	private String accessCode;
	
	private static final Logger logger = LoggerFactory.getLogger(OptimizeWorker.class);

	private final static JSONParser parser = new JSONParser();

	private static ConcurrentHashMap<String, String> optimizeRequest = new ConcurrentHashMap<String, String>();

	public OptimizeWorker() {
		this.accessCode = util.generateUUID();
		logger.info("Optimize worker created with access code: " + accessCode);
	}
	
	public static String getTopicName() {
		return UtilConstants.QUEUE_OPTIMIZE_WORKER;
	}
	
	private Utility util = new Utility();

	@SuppressWarnings("unchecked")
	public static String getRequestMessage(String accessCode, String space) {
		JSONObject json = new JSONObject();
		json.put("accessCode", accessCode);
		json.put("space", space);
		json.put("action", Status.Request.toString());
		logger.info("Generate text for optimize "+ Status.Request.toString() +" from " + accessCode + " for space " + space);
		return json.toJSONString();
	}

	@SuppressWarnings("unchecked")
	public static String getReleaseMessage(String accessCode, String space) {
		JSONObject json = new JSONObject();
		json.put("accessCode", accessCode);
		json.put("space", space);
		json.put("action", Status.Release.toString());
		logger.info("Generate text for optimize "+ Status.Release.toString() +" from " + accessCode + " for space " + space);
		return json.toJSONString();
	}
	
	public static boolean isAvailable(String space, String accessCode) {
		boolean available = false;
		if (optimizeRequest.get(space) == null) { 
			available = true;
		} else {
			if (accessCode.equals(optimizeRequest.get(space))) {
				available = true;
			}
		}
		return available;
	}

	@Override
	public void onMessage(Message message) {
		if (message instanceof TextMessage) {
			TextMessage text = (TextMessage) message;
			try {
				JSONObject content = (JSONObject) parser.parse(text.getText());
				String space = (String) content.get("space");
				String accessCode = (String) content.get("accessCode");
				Status status = Status.valueOf((String) content.get("action"));
				if (status.equals(Status.Request)) {
					if (optimizeRequest.get(space)!=null) {
						logger.warn("Found existing unexpected request without release. The new request will override the old one");
					} 
					optimizeRequest.put(space, accessCode);
					logger.info("Optimize request "+ Status.Request.toString() +" has been added by " + accessCode + " for space " + space);
				} else if (status.equals(Status.Release)) {
					optimizeRequest.remove(space);
					logger.info("Optimize request "+ Status.Release.toString() +" has been added by " + accessCode + " for space " + space);
				}
			} catch (ParseException | JMSException e) {
				e.printStackTrace();
			}

		}

	}
}
