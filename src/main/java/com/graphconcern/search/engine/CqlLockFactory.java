package com.graphconcern.search.engine;

import java.io.IOException;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.graphconcern.support.util.MqUtil;

public class CqlLockFactory extends LockFactory {
	private static final Logger log = LoggerFactory.getLogger(CqlLockFactory.class);
	
	private String path;
	private String id;
	private MqUtil mqUtil;
	private ConcurrentHashMap<String, CqlLock> myLocks = new ConcurrentHashMap<String, CqlLock>();
	
	public CqlLockFactory(MqUtil mqUtil, String path, String id) {
		this.mqUtil = mqUtil;
		this.path = path;
		this.id = id;
	}

	@Override
	public Lock makeLock(String lockName) {
		if (myLocks.containsKey(lockName)) return myLocks.get(lockName);
		CqlLock lock = new CqlLock(mqUtil, path, lockName, id);
		myLocks.put(lockName, lock);
		log.debug("making lock, total locks = "+myLocks.size());
		return lock;
	}

	@Override
	public void clearLock(String lockName) throws IOException {
		if (myLocks.containsKey(lockName)) {
			myLocks.get(lockName).close();
			myLocks.remove(lockName);
			log.debug("clearing lock, total locks = "+myLocks.size());
		}
	}
	/*
	 * Clear all locks
	 */
	public void close() {
		
		log.debug("closing remaining locks = "+myLocks.size());
		Enumeration<String> e = myLocks.keys();
		while (e.hasMoreElements()) {
			String lockName = e.nextElement();
			log.debug("closing "+lockName);
			CqlLock lock = myLocks.get(lockName);
			try {
				lock.close();
			} catch (IOException e1) {}
		}
		/*
		 * Clear map
		 */
		if (!myLocks.isEmpty()) myLocks.clear();
	}

}

