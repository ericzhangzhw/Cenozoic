package com.graphconcern.search.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.graphconcern.support.util.Constants;
import com.graphconcern.support.util.Utility;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CqlLockDao {
	private static final Logger log = LoggerFactory.getLogger(CqlLockDao.class);
	
	private static Utility util = new Utility();
	private static AtomicInteger onceOff = new AtomicInteger();
	private static String busySignature;
	private static String KEEP_ALIVE = ":://alive";
	/*
	 * DO NOT CHANGE this timeout value
	 */
	private static final long BLOCK_AFTER_RELEASE = 8000;
	/*
	 * Simple expiring cache for locks
	 */
	private static ConcurrentHashMap<String, HashMap<String, Object>> cache = new ConcurrentHashMap<String, HashMap<String, Object>>();
	
	private static String GET_LOCK = "SELECT locked, modified, instance, server FROM locks WHERE path = ? and name = ?";
	private static String UPDATE_LOCK = "INSERT INTO locks (path, name, server, modified, instance, locked) VALUES (?, ?, ?, ?, ?, ?) ";
	private static String IS_ALIVE = "SELECT modified FROM locks WHERE path = ? and name = ? and locked = False";
	
	private static PreparedStatement prepareGetLock, prepareUpdateLock, prepareIsAlive;
	
	private Session session;

		
	public CqlLockDao(Session session) {
		this.session = session;
		
		if (onceOff.getAndIncrement() == 0) {
			prepareGetLock = session.prepare(GET_LOCK);
			prepareUpdateLock = session.prepare(UPDATE_LOCK);
			prepareIsAlive = session.prepare(IS_ALIVE);
			busySignature = "busy-"+util.getRandomDigits(16);
			HouseKeeping housekeeper = new HouseKeeping();
			housekeeper.start();
			KeepAlive alive = new KeepAlive(session, prepareUpdateLock, KEEP_ALIVE);
			alive.start();
		} else {
			onceOff.set(10); // Guarantee this atomic counter never overflows
		}
	}

	public boolean obtainLock(String hostname, String path, String lockName, String instanceId) throws IOException {
		if (path.equals(KEEP_ALIVE)) return false;
		
		String owner = lockOwner(path, lockName);
		if (owner != null) return instanceId.equals(owner) ? true : false;
		
		try {
			/*
			 * To lock, set the LOCKED record by setting time to current time
			 */
			long now = System.currentTimeMillis();
			session.execute(prepareUpdateLock.bind(path, lockName, hostname, new Date(now), instanceId, true));
			
			saveCache(path, lockName, instanceId, true);
			return true;
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException("Unable to create lock "+path+"/"+lockName+" - "+e.getMessage());
		}
	}
	
	public boolean isLocked(String path, String lockName) {
		if (path.equals(KEEP_ALIVE)) return true;
		
		return lockOwner(path, lockName) != null;
	}
	
	private String lockOwner(String path, String lockName) {
		/*
		 * Available from cache?
		 */
		String composite = path+"/"+lockName;
		if (cache.containsKey(composite)) {
			HashMap<String, Object> map = cache.get(composite);
			long time = (long) map.get("time");
			String owner = (String) map.get("id");
			boolean lock = (boolean) map.get("lock");
			long diff = System.currentTimeMillis() - time;
			if (diff > Constants.ONE_MINUTE) {
				log.warn("Cache "+composite+" expired");
				cache.remove(composite);
			} else {
				if (lock) {
					log.debug("cache says "+composite+" belongs to "+owner);
					return owner;
				}
				/*
				 * IMPORTANT:
				 * 
				 * If the lock is recently released, defer the IndexWriter for the same space for a few seconds
				 * to ensure that Cassandra has sufficient time to commit to the database.
				 * 
				 */
				if (diff < BLOCK_AFTER_RELEASE) {
					log.debug("cache says "+composite+" is "+busySignature);
					return busySignature;
				}
				log.debug("cache says "+composite+" is free");
				return null;
			}
		}
		long lockedTime = 0, unlockedTime = 0;
		String lockedId = null, unlockedId = null;
		String lockedServer = null;
		/*
		 * There should only be maximum two records because of boolean value in the column "locked".
		 * Natural database sorting order is False then True.
		 */
		ResultSet result = session.execute(prepareGetLock.bind(path, lockName));
		Iterator<Row> records = result.iterator();
		while (records.hasNext()) {
			Row row = records.next();
			long modified = row.getDate("modified").getTime();
			String id = row.getString("instance");
			boolean locked = row.getBool("locked");
			String server = row.getString("server");
			
			if (locked) {
				lockedTime = modified;
				lockedId = id;
				lockedServer = server;
			} else {
				unlockedTime = modified;
				unlockedId = id;
			}
		}
		/*
		 * Normal unlock case
		 */
		if (unlockedTime > 0 && unlockedTime >= lockedTime) return null;
		/*
		 * Normal lock case
		 */
		if (lockedId != null && unlockedId != null && lockedServer != null && lockedId.equals(unlockedId)) {
			if (lockedTime > unlockedTime) 
				return lockedAndLive(lockedServer, path, lockName, lockedId, lockedTime)? lockedId: null;
		}
		/*
		 * If we see only the locked record, it is a new lock.
		 */
		if (lockedId != null && unlockedId == null) {
			return lockedAndLive(lockedServer, path, lockName, lockedId, lockedTime)? lockedId: null;
		}
		/*
		 * All other cases = unlocked
		 */
		return null;
	}
	
	private boolean lockedAndLive(String hostname, String path, String lockName, String lockedId, long lockedTime) {
		/*
		 * If the indexing task takes more than one minute, check if the host is still alive.
		 */
		if (System.currentTimeMillis() - lockedTime > Constants.ONE_MINUTE) {
			if (alive(hostname)) {
				saveCache(path, lockName, lockedId, true);
				log.info("--------->"+lockedId+" is alive");
				return true;
			} else {
				log.info(hostname+" is offline");
				return false;
			}
		} else {
			saveCache(path, lockName, lockedId, true);
			log.info("--------->"+lockedId+" is alive? "+alive(hostname));
			return true;
		}
	}
	
	private void saveCache(String path, String lockName, String instanceId, boolean lock) {
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("id", instanceId);
		map.put("lock", lock);
		map.put("time", System.currentTimeMillis());
		cache.put(path+"/"+lockName, map);
		log.debug("Saving cache "+path+" "+lockName+" "+instanceId+"? "+lock);
	}

	public void releaseLock(String hostname, String path, String lockName, String instanceId) throws IOException {
		/*
		 * You can only release your own lock
		 */
		try {
			String owner = lockOwner(path, lockName);
			if (owner != null && owner.equals(instanceId)) {
				session.execute(prepareUpdateLock.bind(path, lockName, hostname, new Date(System.currentTimeMillis()), instanceId, false));
				log.info(path+" lock released by "+instanceId);
				
				saveCache(path, lockName, instanceId, false);		
			}
		} catch (Exception e) {
			throw new IOException("Unable to delete lock "+path+"/"+lockName+" - "+e.getMessage());
		}
	}
	
	public boolean alive(String hostname) {
		ResultSet result = session.execute(prepareIsAlive.bind(KEEP_ALIVE, hostname));
		Iterator<Row> records = result.iterator();
		if (records.hasNext()) {
			Row row = records.next();
			long modified = row.getDate("modified").getTime();
			if (System.currentTimeMillis() - modified < Constants.ONE_MINUTE) return true;
		}
		return false;		
	}
	
	private class HouseKeeping extends Thread {
		
		@Override
		public void run() {
			log.info("Housekeeper started");
			
			while (true) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {}
				long now = System.currentTimeMillis();
				
				List<String> removalList = new ArrayList<String>();
				Enumeration<String> keys = cache.keys();
				while (keys.hasMoreElements()) {
					String k = keys.nextElement();
					HashMap<String, Object> map = cache.get(k);
					if (map != null) {
						long t0 = (long) map.get("time");
						/*
						 * Expire in 30 seconds
						 */
						if (now - t0 > 30000) {
							removalList.add(k);
						}
					}
				}
				if (!removalList.isEmpty()) {
					for (String k: removalList) {
						cache.remove(k);
						log.info("Expiring cache "+k);
					}
				}
			}
		}
		
	}



}
