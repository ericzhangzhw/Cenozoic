package com.graphconcern.search.index.request;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.graphconcern.search.index.IndexController;
import com.graphconcern.support.util.Utility;

public class PhysicalStore {

	private static final Logger logger = LoggerFactory.getLogger(PhysicalStore.class);
	
	private final static Utility util = new Utility();
	
	private static JSONParser parser = new JSONParser();
	
	private File storage;
	
	public PhysicalStore(File storage) {
		this.storage = storage;
		storage.mkdirs();
	}
	
	public String store(String space, JSONObject document) {
		File spaceFolder = new File(storage, space);
		
		/*
		 * Space folder checking. If space folder does not exist in the physical folder area, we create it first. 
		 */
		if (!spaceFolder.exists()) {
			spaceFolder.mkdirs();
		}
		
		/*
		 * Generate the physical data file by putting the JSON content into it. 
		 * The file name is a unique value with a time stamp 
		 */
		Date storeDate = new Date();
		String name = util.long2uniqueTime(storeDate.getTime());
		
		logger.info("UUID Generate for Space ["+space+"] is [" + name+"] "
				+ "at " + IndexController.getDateFormatter().format(storeDate));
		
		File file = new File(spaceFolder, name);
		util.bytes2file(file, util.json2bytes(document));
		
		return name;
	}
	
	public JSONObject retrieve(String space, String name) throws ParseException, IOException {
		return (JSONObject) parser.parse(util.file2str(new File(new File(this.storage, space), name)));
	}
	
	public boolean remove(String space, String name) {
		return new File(new File(this.storage, space), name).delete();
	}
}
