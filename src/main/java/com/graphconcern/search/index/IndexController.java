package com.graphconcern.search.index;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.graphconcern.search.index.request.IndexManager;
import com.graphconcern.search.index.request.PhysicalStore;
import com.graphconcern.search.index.token.TokenMaker;
import com.graphconcern.search.init.Startup;
import com.graphconcern.support.util.Utility;

public class IndexController implements Runnable {
	
	private static Utility util = new Utility();
	
	public enum Directory {Physical, Token, Batch}; 
	
	private final static Logger logger = LoggerFactory.getLogger(IndexController.class);

	private boolean running;
	
	private File storage;
	
	private int maxSize;
	
	private int minSize;
	
	private int timeout;
	
	private File tokenFolder;

	private File batchFolder;
	
	public File getTokenFolder() {
		return tokenFolder;
	}

	public File getBatchFolder() {
		return batchFolder;
	}
	
	public File getStorageFolder() {
		return storageFolder;
	}

	
	private File storageFolder;
	
	private Thread manager;
	
	private TokenMaker tokenMaker;
	
	private IndexManager indexManager;
	
	private PhysicalStore physicalStore;
	
	private final static String STATIC_DISPLAY_DATE_FORMAT = "MM/dd/yyyy HH:mm:ss";

	public static SimpleDateFormat getDateFormatter() {
		return new SimpleDateFormat(STATIC_DISPLAY_DATE_FORMAT);		
	}
	
	public void shutdown() {
		this.running = false;
		indexManager.shutdown();
	}
	
	public void init() {
		running = true;
		
		logger.info("Prepare values from System Configuraion");
		maxSize = Startup.getSystemConfig().getInt("clucene.index.max");
		minSize = Startup.getSystemConfig().getInt("clucene.index.min");
		timeout = Startup.getSystemConfig().getInt("clucene.index.seconds");
		storage = new File(Startup.getSystemConfig().getString("clucene.storage"));
		
		logger.info("Construct folder list");
		
		tokenFolder = new File(storage, Directory.Token.toString());
		batchFolder = new File(storage, Directory.Batch.toString());
		storageFolder = new File(storage, Directory.Physical.toString());
		
		physicalStore = new PhysicalStore(storageFolder);
		
		logger.info("Prepare the Helper classes");
		tokenMaker = new TokenMaker(this);
		indexManager = new IndexManager();
		
		logger.info("Initialize the Helper classes");
		indexManager.init(this);
		
		logger.info("Construct Helper processing thread");
		manager = new Thread(indexManager, "Index Manager");

		logger.info("Start the Index Manager");
		manager.start();	
	}
	
	public void createDocument(String space, JSONObject document) {
		/*
		 * Create a new document with reference name is "name". Also know as the file name
		 */
		String name = this.physicalStore.store(space, document);
		/*
		 * Create the token with the token maker object
		 */
		this.tokenMaker.make(space, name);
	}
	
	@Override
	public void run() {
		while(running) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
	}
	
	public void removeDocument(String space, String batchNumber) {
		logger.info("Release Document with Space " + space + " and Batch Number " + batchNumber);
		File batchInstanceFolder = new File(new File(this.batchFolder, space), batchNumber);
		String[] names = batchInstanceFolder.list();
		util.cleanupDir(batchInstanceFolder);
		logger.info(batchInstanceFolder.getPath());
		
		for (String name : names) {
			logger.info("Purge Physical File: " + name);
			this.physicalStore.remove(space, name);
		}
		
		
	}
		
	public int getMaxSize() {
		return maxSize;
	}

	public int getMinSize() {
		return minSize;
	}
	
	public int getTimeout() {
		return timeout;
	}
	
	public JSONObject getPhysicalData(String space, String name) throws ParseException, IOException {
		return this.physicalStore.retrieve(space, name);
	}
	
	public IndexManager getIndexManager() {
		return this.indexManager;
	}
}
