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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Felixlaw
 *
 */
public class MqUtil {
	private static final Logger log = LoggerFactory.getLogger(MqUtil.class);
	private Utility util = new Utility();
	
	private Connection connection = null;
	private static ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<String, Session>();
	private static ConcurrentHashMap<String, MessageConsumer> consumers = new ConcurrentHashMap<String, MessageConsumer>();
	
	public MqUtil(Connection connection) {
		this.connection = connection;
	}
	
	public Connection getConnection() {
		return connection;
	}
	
	public void postMessageToQueue(String queue, String text) throws JMSException {
		if (connection == null) throw new JMSException("Unable to send because there is no MQ connection");
		
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		// Create the destination (Topic or Queue)
	    Destination destination = session.createQueue(queue);

	    // Create a MessageProducer from the Session to the Topic or Queue
	    MessageProducer producer = session.createProducer(destination);
	    producer.setDeliveryMode(DeliveryMode.PERSISTENT);

	    // Create a messages
	    TextMessage textMessage = session.createTextMessage(text);

	    // Tell the producer to send the message
	    log.debug("To: "+queue+", Message: "+text);
	    producer.send(textMessage);
	    session.close();
	}
	
	public void postMessageToQueue(String queue, String command, Object obj) throws JMSException {
		if (connection == null) throw new JMSException("Unable to send because there is no MQ connection");
		
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		// Create the destination (Topic or Queue)
	    Destination destination = session.createQueue(queue);
	    
	    // Create a MessageProducer from the Session to the Topic or Queue
	    MessageProducer producer = session.createProducer(destination);
	    producer.setDeliveryMode(DeliveryMode.PERSISTENT);
	    
	    MapMessage mapMessage = session.createMapMessage();
	    mapMessage.setString(Constants.MSG_COMMAND, command);
	    mapMessage.setObject(Constants.MSG_OBJECT, obj);

	    // Tell the producer to send the message
	    log.debug("To: "+queue+", Message: "+obj);
	    producer.send(mapMessage);
	    session.close();
	} 
	
	public void postMessageToQueue(String queue, Message message) throws JMSException {
		if (connection == null) throw new JMSException("Unable to send because there is no MQ connection");
		
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		// Create the destination (Topic or Queue)
	    Destination destination = session.createQueue(queue);

	    // Create a MessageProducer from the Session to the Topic or Queue
	    MessageProducer producer = session.createProducer(destination);
	    producer.setDeliveryMode(DeliveryMode.PERSISTENT);

	    // Tell the producer to send the message
	    log.debug("To: "+queue+", Message: "+message);
	    producer.send(message);
	    session.close();
	} 
	
	public void postMessageToTopic(String topic, String message) throws JMSException {
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		// Create the destination (Topic or Queue)
	    Destination destination = session.createTopic(topic);
	
	    // Create a MessageProducer from the Session to the Topic or Queue
	    MessageProducer producer = session.createProducer(destination);
	    producer.setDeliveryMode(DeliveryMode.PERSISTENT);

	    // Create a messages
	    TextMessage text = session.createTextMessage(message);

	    // Tell the producer to send the message
	    log.debug("To: "+topic+", Message: "+message);
	    producer.send(text);
	    session.close();
	}
	/*
	 * Listeners' sessions are kept in a map so that they can be destroyed when needed
	 */
	public String createExclusiveConsumer(String queue, MessageListener listener) throws JMSException {
		String mqId = util.generateUUID();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		sessions.put(mqId, session);
		ActiveMQQueue exclusiveQueue = new ActiveMQQueue(queue+"?consumer.exclusive=true");
		MessageConsumer consumer = session.createConsumer(exclusiveQueue);
		consumers.put(mqId, consumer);
		consumer.setMessageListener(listener);
		return mqId;
	}
	public String createQueueMessageConsumer(String queue, MessageListener listener) throws JMSException {
		String mqId = util.generateUUID();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		sessions.put(mqId, session);
		MessageConsumer consumer = session.createConsumer(session.createQueue(queue));
		consumers.put(mqId, consumer);
		consumer.setMessageListener(listener);
		return mqId;
	}
	
	public String createQueueMessageConsumer(Destination queue, MessageListener listener) throws JMSException {
		String mqId = util.generateUUID();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		sessions.put(mqId, session);
		MessageConsumer consumer = session.createConsumer(queue);
		consumers.put(mqId, consumer);
		consumer.setMessageListener(listener);
		return mqId;
	}

	public String createTopicMessageConsumer(String topic, MessageListener listener) throws JMSException {
		String mqId = util.generateUUID();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		sessions.put(mqId, session);
		MessageConsumer consumer = session.createConsumer(session.createTopic(topic));
		consumers.put(mqId, consumer);
		consumer.setMessageListener(listener);
		return mqId;
	}
	
	public String createTopicMessageConsumer(Destination topic, MessageListener listener) throws JMSException {
		String mqId = util.generateUUID();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		sessions.put(mqId, session);
		MessageConsumer consumer = session.createConsumer(topic);
		consumers.put(mqId, consumer);
		consumer.setMessageListener(listener);
		return mqId;
	}
	
	public void clearListenerSession(String key) {
		Session session = sessions.get(key);
		MessageConsumer consumer = consumers.get(key);
		if (session != null && consumer != null) {
			try {
				consumer.close();
				session.close();
				sessions.remove(key);
				consumers.remove(key);
				log.info(key+" consumer closed");
			} catch (JMSException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void shutdown() {
		Iterator<Map.Entry<String, Session>> it = sessions.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Session> kv = (Map.Entry<String, Session>) it.next();
			clearListenerSession(kv.getKey());
		}
	}
	
	/*
	 * For request-response usage pattern:
	 * Create temp queue when servlet context starts
	 * Destroy temp queue when servlet context closes
	 * 
	 * Assume requester uses a temporary queue and responder uses a permanent queue.
	 */
	public Destination createTemporaryQueue() throws JMSException {
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination temp = session.createTemporaryQueue();
		log.info("Created "+temp);
		return temp;
	}
	
	public Destination createTemporaryTopic() throws JMSException {
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination temp = session.createTemporaryTopic();
		log.info("Created "+temp);
		return temp;
	}
	
	public void sendRequest(String dest, String text, Destination tempQueue) throws JMSException {
		if (connection == null) throw new JMSException("Unable to send because there is no MQ connection");
		
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		// Create the destination (Topic or Queue)
	    Destination destination = session.createQueue(dest);

	    // Create a MessageProducer from the Session to the Topic or Queue
	    MessageProducer producer = session.createProducer(destination);
	    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

	    // Create a messages
	    TextMessage textMessage = session.createTextMessage(text);
	    textMessage.setJMSReplyTo(tempQueue);

	    // Tell the producer to send the message
	    log.debug("Request from "+tempQueue+" to "+dest+", Message: "+text);
	    producer.send(textMessage);
	    session.close();
	}
	
	public void sendResponse(Destination tempQueue, String text) throws JMSException {
		if (connection == null) throw new JMSException("Unable to send because there is no MQ connection");
		
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

	    // Create a MessageProducer from the Session to the Topic or Queue
	    MessageProducer producer = session.createProducer(tempQueue);
	    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

	    // Create a messages
	    TextMessage textMessage = session.createTextMessage(text);

	    // Tell the producer to send the message
	    log.debug("Reply to "+tempQueue+", Message: "+text);
	    producer.send(textMessage);
	    session.close();
	}
	
	public String createTempQueueMessageConsumer(Destination tempQueue, MessageListener listener) throws JMSException {
		return createTempDestConsumer(tempQueue, listener);
	}
	
	public String createTempTopicMessageConsumer(Destination tempTopic, MessageListener listener) throws JMSException {
		return createTempDestConsumer(tempTopic, listener);
	}
	
	private String createTempDestConsumer(Destination tempDest, MessageListener listener) throws JMSException {
		String mqId = util.generateUUID();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		sessions.put(mqId, session);
		MessageConsumer consumer = session.createConsumer(tempDest);
		consumers.put(mqId, consumer);
		consumer.setMessageListener(listener);
		return mqId;
	}

}
