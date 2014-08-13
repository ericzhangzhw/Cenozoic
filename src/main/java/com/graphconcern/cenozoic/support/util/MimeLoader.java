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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MimeLoader {
	private static final Logger log = LoggerFactory.getLogger(MimeLoader.class);
	
	private HashMap<String, String> ext2mime = new HashMap<String, String>();
	private HashMap<String, List<String>> mime2ext = new HashMap<String, List<String>>();
	private Utility util;
	
	public MimeLoader(File f, Utility util) {
		this.util = util;
		if (f.exists()) loadTypes(f);
	}
	
	private void loadTypes(File f) {
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			while(true) {
				String line = in.readLine();
				if (line == null) break;
				/*
				 * Enforce lower case
				 */
				line = line.trim().toLowerCase();
				if (line.length() == 0) continue;
				if (line.startsWith("#")) continue;
				List<String> s = util.split(line, "\t ");	// detect tab or space as separator
				if (s == null) continue;
				if (s.size() < 2) continue;
				List<String> values = new ArrayList<String>();
				for (int i=1; i < s.size(); i++) {
					values.add(s.get(i));
					/*
					 * extension to contentType mapping
					 */
					ext2mime.put(s.get(i), s.get(0));
				}
				/*
				 * contentType to extensions mapping
				 */
				mime2ext.put(s.get(0), values);
			}
			in.close();
			in = null;
			log.info("Total "+ext2mime.size()+" extensions and "+mime2ext.size()+" MimeTypes registered");
		} catch (IOException e) {
			e.printStackTrace();
			log.warn("Unable to load mime types - "+e.getMessage());
    	} finally {
    		if (in != null)
				try {
					in.close();
				} catch (IOException e) {}
    	}
	}
	
	public String getMimeFromExt(String ext) {
		return ext2mime.get(ext);
	}
	
	public List<String> getExtFromMime(String mime) {
		return mime2ext.get(mime);
	}

}
