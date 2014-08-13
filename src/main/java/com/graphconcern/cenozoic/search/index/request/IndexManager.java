package com.graphconcern.cenozoic.search.index.request;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.DateTime;
import org.joda.time.Seconds;

import com.graphconcern.cenozoic.search.init.Startup;
import com.graphconcern.cenozoic.search.util.UtilConstants;
import com.graphconcern.cenozoic.support.util.Utility;
import com.graphconcern.cenozoic.search.index.IndexController;

public class IndexManager implements Runnable {

	private ConcurrentHashMap<String, String> batch = new ConcurrentHashMap<String, String>();

	public boolean associate(String space, String batchNumber) {
		if (!batch.containsKey(space)) {
			batch.put(space, batchNumber);
			return true;
		}
		return false;
	}

	public String getBatchNumber(String space) {
		return this.batch.get(space);
	}

	public void resume(String space) {
		if (space!=null) {
			batch.remove(space);	
		}
	}

	private boolean running;

	private File tokenFolder;

	private File batchFolder;

	private int maxSize;

	private int minSize;

	private int timeout;

	private static final Logger logger = LoggerFactory.getLogger(IndexManager.class);

	private static final Utility util = new Utility();

	public void init(IndexController controller) {
		logger.info("Starting up " + this.getClass().getName());
		running = true;
		this.tokenFolder = controller.getTokenFolder();
		this.batchFolder = controller.getBatchFolder();
		this.timeout = controller.getTimeout();
		this.maxSize = controller.getMaxSize();
		this.minSize = controller.getMinSize();

		this.batchFolder.mkdirs();
	}

	public void shutdown() {
		logger.info("Shutting down " + this.getClass().getName());
		running = false;
	}

	@Override
	public void run() {
		while(running) {

			/*
			 * Printing out the  batch information.  
			 */

			if (batch.size() > 0) {
				String msg = "\n***************************************************\n";
				for (String key : this.batch.keySet()) {
					msg += "\t";
					msg += "Key : " + key;
					msg += "\n";
					msg += "\t";
					msg += "Value : " + this.batch.get(key);
					msg += "\n";
				}
				msg += "***************************************************\n";
				logger.debug(msg);
			}
			

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			/*
			 * Create batch instance into the batch folder and allocate token item into it. 
			 */
			File[] spaceFolders = tokenFolder.listFiles();
			logger.debug("Monitoring Token Folder: " + this.tokenFolder.getPath());
			for (File spaceFolder : spaceFolders) {
				String space = spaceFolder.getName();
				logger.debug("Found space: " + space);

				List<String> names = Arrays.asList(spaceFolder.list()); 
				Collections.sort(names);

				int currentSize = names.size();

				boolean passthru = false;
				if (currentSize > 0) {
					if (currentSize >= minSize) {
						passthru = true;
					} else if (currentSize < minSize) {
						logger.info("Current size is " + currentSize + ". And the minimum size is: " + minSize);
						for (String filename : names) {
							File file = new File(spaceFolder, filename);
							DateTime fileDateTime = new DateTime(new Date(util.file2long(file)));
							DateTime currentDateTime = new DateTime(new Date());
							int secondDiff = Seconds.secondsBetween(fileDateTime, currentDateTime).getSeconds();
							if (secondDiff > this.timeout) {
								passthru = true;
							}
						}

					}

					if (passthru) {

						// Prepare the batch folder environment. 
						String batchNumber = util.long2uniqueTime(new Date().getTime()); 
						File batchInstanceFolder = new File(new File(this.batchFolder, space), batchNumber);
						batchInstanceFolder.mkdirs();

						if (currentSize > maxSize) {
							logger.debug("There are "+ currentSize +" items, and there ["+(currentSize - maxSize)+"] will be left over as the max size is " + maxSize);
						}
						for (int i=0; i<Math.min(currentSize, maxSize); i++) {
							String filename = names.get(i);
							File tokenFile = new File(spaceFolder, filename);
							logger.info("Process file name: " + filename);
							try {
								new File(batchInstanceFolder, filename).createNewFile();
								tokenFile.delete();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				}

			}

			/*
			 * Submitting batch instance to worker
			 */
			File[] batchInstanceSpaceFolders = this.batchFolder.listFiles();
			for (File batchInstanceSpaceFolder : batchInstanceSpaceFolders ) {
				String space = batchInstanceSpaceFolder.getName();
				if (!batch.containsKey(space)) {
					String[] batchNumbers = batchInstanceSpaceFolder.list();
					if (batchNumbers != null && batchNumbers.length > 0) {
						String batchNumber = batchNumbers[0];
						try {
							if (associate(space, batchNumber)) {
								Startup.getMq().getMqUtil().postMessageToQueue(UtilConstants.QUEUE_INDEX_WORKER, space);
							}
						} catch (JMSException e) {
							logger.error("Unable to send the message for indexing: " + e.getMessage());
							e.printStackTrace();
						}
					}

				}


			}



		}

	}
}