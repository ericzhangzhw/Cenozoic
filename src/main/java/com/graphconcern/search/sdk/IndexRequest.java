package com.graphconcern.search.sdk;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.graphconcern.search.sdk.exception.CLuceneException;
import com.graphconcern.search.sdk.util.Constants;

public class IndexRequest {

	private JSONParser parser = new JSONParser();
	
	private Map<String, String> body;
	
	private Map<String, String> header;

	private String space;

	private String url;
	
	protected IndexRequest (String url, String space) {
		this.url = url;
		this.space = space;
	}

	public void setBody(Map<String, String> body) {
		this.body = body;
	}

	public void setHeader(Map<String, String> header) {
		this.header = header;
	}
	
	public void validate() throws CLuceneException {
		if (header==null || body==null || header.size() == 0 || body.size()==0) {
			throw new CLuceneException("Found invalid schema or content");
		} else if (!header.containsValue(Constants.TYPE_KEY_ID)) {
			throw new CLuceneException("Missing [" + Constants.TYPE_KEY_ID + "] entry in content");
		} else {
			for(String key : body.keySet()) {
				if (header.get(key) == null) {
					throw new CLuceneException("Found invalid key from content not exist in schema");
				}
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public boolean submit() throws CLuceneException {
		// Verifying the header and body entry
		validate();
		
		int status = Constants.REPLY_HTTP_STATUS_FNF;
		
		JSONObject content = new JSONObject();
		content.put(Constants.PARAM_HEADER, this.header);
		content.put(Constants.PARAM_BODY, this.body);

		HttpPost httpPost = new HttpPost(this.url + "/" + space);
		StringEntity entity = new StringEntity(content.toJSONString(), 
				ContentType.create("application/json", "UTF-8"));
		httpPost.setEntity(entity);
		
		CloseableHttpClient httpclient = HttpClients.createDefault();
		
		try {
			CloseableHttpResponse response = httpclient.execute(httpPost);
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			
			response.getEntity().writeTo(baos);
			status = ((Long) ((JSONObject) parser.parse(baos.toString())).get(Constants.REPLY_HTTP_STATUS_KEY)).intValue();
			
			response.close();
			
		} catch (ParseException | IOException ex) {
			ex.printStackTrace();
			throw new CLuceneException(ex.getMessage());
		}
		return status == Constants.REPLY_HTTP_STATUS_OK;
		
	}
	
	/*
	 * Define 
	 * */
	public IndexRequest define(String key, String type, String value) {
		init();
		header.put(key, type);
		body.put(key, value);
		return this;
	}
	
	/*
	 * Initiate Implementer 
	 * */
	private void init() {
		if (header==null) {
			header = new HashMap<String, String>();
		}
		
		if (body==null) {
			body = new HashMap<String, String>();
		}
	}
	
}
