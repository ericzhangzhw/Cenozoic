package com.graphconcern.cenozoic.search.engine;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.lucene.store.Lock;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.graphconcern.cenozoic.search.init.Startup;
import com.graphconcern.cenozoic.search.util.UtilConstants;
import com.graphconcern.cenozoic.support.util.Constants;
import com.graphconcern.cenozoic.support.util.MqUtil;
import com.graphconcern.cenozoic.support.util.Utility;

public class CqlLock extends Lock {
	private static final Logger log = LoggerFactory.getLogger(CqlLock.class);
	
	private static ConcurrentHashMap<String, CqlLock> allLocks = new ConcurrentHashMap<String, CqlLock>();
	
	private static AtomicInteger onceOff = new AtomicInteger();
	private static Destination myTempQueue;
	private static Utility util = new Utility();
	
	private String path, lockName, instanceId;
	
	private boolean locked = false;
	private MqUtil mqUtil;
	private boolean timeout = false;
	private Thread myThread;
	private String hostname;
	
	public CqlLock(MqUtil mqUtil, String path, String lockName, String instanceId) {
		this.mqUtil = mqUtil;
		this.path = path;
		this.lockName = lockName;
		this.instanceId = instanceId;
		this.hostname = Startup.getSystemConfig().getHostId();
		if (onceOff.getAndIncrement() == 0) {
			/*
			 * Start Lock listener
			 */
			try {
				myTempQueue = mqUtil.createTemporaryQueue();
				mqUtil.createTempQueueMessageConsumer(myTempQueue, new LockListener());
				log.info("Starting listener at "+myTempQueue);
			} catch (JMSException e) {
				log.error("Unable to start LockListener due to "+e.getMessage());
			}
		} else {
			onceOff.set(10); // Guarantee this atomic counter never overflows
		}
	}
	
	public static ConcurrentHashMap<String, CqlLock> getLocks() {
		return allLocks;
	}
	
	public void wakeUp() {
		if (myThread != null) {
			timeout = false;
			LockSupport.unpark(myThread);
			myThread = null;
		}
	}
	
	public void setLocked(boolean locked) {
		this.locked = locked;
	}

	@Override
	public boolean obtain() throws IOException {
		return lockRequest("obtain");
	}

	@Override
	public boolean isLocked() {
		return lockRequest("status");
	}
	
	@SuppressWarnings("unchecked")
	private boolean lockRequest(String type) {
		/*
		 * Save this thread into map
		 */
		String compositeKey = path+"/"+lockName+"/"+instanceId;
		myThread = Thread.currentThread();
		allLocks.put(compositeKey, this);
		/*
		 * Send MQ request, park thread and wait for the request to complete
		 */
		JSONObject request = new JSONObject();
		request.put("path", path);
		request.put("hostname", hostname);
		request.put("name", lockName);
		request.put(Constants.REQUEST_ID, instanceId);
		request.put(Constants.TYPE, type);
		
		log.debug("sending request to MQ ----->"+type+" "+compositeKey);
		
		try {
			mqUtil.sendRequest(UtilConstants.LOCK_MASTER, util.json2str(request), myTempQueue);
		} catch (JMSException e) {
			log.error("Unable to send MQ message to "+UtilConstants.LOCK_MASTER);
			return false;
		}
		
		timeout = true;
		LockSupport.parkUntil(System.currentTimeMillis() + Constants.INTRANET_TIMEOUT);
		allLocks.remove(compositeKey);
		
		if (timeout) {
			log.debug("Unable to obtain lock "+lockName+" because MQ timeout");
			return false;
		}		
		
		return locked;
	}
	
	@Override
	public String toString() {
	    return "Lock@" + path+"/"+lockName+"/"+instanceId;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void close() throws IOException {
		/*
		 * Guarantee to do it only once
		 */
		if (locked) {
			locked = false;
			JSONObject request = new JSONObject();
			request.put("path", path);
			request.put("name", lockName);
			request.put("hostname", hostname);
			request.put(Constants.REQUEST_ID, instanceId);
			request.put(Constants.TYPE, "close");
			
			try {
				mqUtil.postMessageToQueue(UtilConstants.LOCK_MASTER, util.json2str(request));
			} catch (JMSException e) {
				log.error("Unable to send MQ message to "+UtilConstants.LOCK_MASTER);
			}
			/*
			 * Sleep for a few random milliseconds to avoid engaging the lock exclusively
			 */
			Random r = new Random();
			int sleepTime = r.nextInt(10);
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {}
		}
	}
	

}
