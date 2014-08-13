package com.graphconcern.cenozoic.search.index.token;

import java.util.Date;
import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.graphconcern.cenozoic.support.util.Utility;
import com.graphconcern.cenozoic.search.index.IndexController;

public class TokenMaker {
	
	private static Utility util = new Utility();

	private File tokenFolder;
	
	private static final Logger logger = LoggerFactory.getLogger(TokenMaker.class);

	public TokenMaker(IndexController controller) {
		this.tokenFolder = controller.getTokenFolder();
		this.tokenFolder.mkdirs();
	}


	
	public void make(String space, String name) {
		File spaceFolder = new File(tokenFolder, space);
		
		/*
		 * Checking if the space folder existence. We create the space folder if it has not been existed. 
		 */
		if (!spaceFolder.exists()) {
			logger.info("Create Space folder : " + spaceFolder.getPath() + " if not exist.");
			spaceFolder.mkdirs();
		} 
		
		/*
		 * Prepare for creating the token file with the provided name. 
		 */
		logger.debug("Create the token file : " + name);
		Date currentDate = new Date();
		logger.info("Create date time: " + IndexController.getDateFormatter().format(currentDate));
		/*
		 * We also write the long value into the file with the current date time in long value.
		 */
		util.long2file(new File(spaceFolder, name), currentDate.getTime());
		/*
		 * Up to this moment, the file is a token to driven the index. Token maker's job has been done. 
		 */
	}
	

	
	
}
