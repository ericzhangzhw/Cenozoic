package com.graphconcern.cenozoic.search.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Hashtable;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.graphconcern.cenozoic.support.util.Utility;

public class CollectionHelper {

	private static Utility util = new Utility();
	
	public static Hashtable<String, String> getHashtable(JSONObject json, String part) {
		Hashtable<String, String> result = new Hashtable<String, String>();
		JSONObject json2 = (JSONObject) json.get(part);
		for (Object oKey : json2.keySet()) {
			String value = (String) json2.get(oKey);
	 		result.put((String) oKey, value);
		}
		return result;
	}
	
	public static JSONObject getJSONObject(HttpServletRequest request) throws ParseException, IOException {
		JSONParser parser = new JSONParser();	// parser is not thread safe so we need to start a new instance
		StringBuffer jb = new StringBuffer();
		String line = null;
		BufferedReader reader = request.getReader();
		while ((line = reader.readLine()) != null) {
			jb.append(line);
		}		
		return (JSONObject) parser.parse(jb.toString());
	}
	
	public static String getSpaceArea(HttpServletRequest request, HttpServletResponse response) throws UnsupportedEncodingException {
		request.setCharacterEncoding("UTF-8");
		response.setHeader("Cache-Control", "no-cache");
		response.setDateHeader("Expires", 0);

		String path = request.getPathInfo();
		if (path == null || path.trim().length() <= 1 || path.charAt(0) != '/') {
			util.sendJsonError(response, 400, "Path should contain space area");
			return null;
		}
		return path.substring(1);
	}
}
