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

public class SearchRequest {

	private static JSONParser parser = new JSONParser();

	private Map<String, String> queryString;

	private String url;
	
	private String space;


	protected SearchRequest(String url, String space) {
		this.url = url;
		this.space = space;
	}

	public void setQueryString(Map<String, String> queryString) {
		this.queryString = queryString;
	}

	public void validate() throws CLuceneException {
		for (String key : queryString.keySet()) {
			if (queryString.get(key) == null) {
				throw new CLuceneException("Invalid value from key [" + key + "]");
			}
		}
	}

	/*
	 * Query String Implementer
	 */
	public void addQueryString(String key, String value) {
		init();
		queryString.put(key, value);
	}

	/*
	 * Initiate Implementer
	 */
	private void init() {
		if (queryString == null) {
			queryString = new HashMap<String, String>();
		}
	}

	@SuppressWarnings("unchecked")
	public HashMap<String, HashMap<String, String>> submit() throws CLuceneException {
		// Verifying the query string entry
		validate();

		JSONObject content = new JSONObject();
		content.put(Constants.PARAM_QUERY_STRING, this.queryString);

		HttpPost httpPost = new HttpPost(this.url + "/" + space);
		StringEntity entity = new StringEntity(content.toJSONString(), 
				ContentType.create("application/json", "UTF-8"));
		httpPost.setEntity(entity);
		
		CloseableHttpClient httpclient = HttpClients.createDefault();
		try {
			CloseableHttpResponse response = httpclient.execute(httpPost);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();

			response.getEntity().writeTo(baos);

			HashMap<String, HashMap<String, String>> resultSet = null;

			try {
				JSONObject result = (JSONObject) parser.parse(baos.toString());

				resultSet = (HashMap<String, HashMap<String, String>>) result
						.get(Constants.REPLY_RESULT_SET_KEY);

			} catch (ClassCastException ex) {
				ex.printStackTrace();
				throw new CLuceneException(ex.getMessage(), ex);
			}
			return resultSet;

		} catch (ParseException | IOException ex) {
			ex.printStackTrace();
			throw new CLuceneException(ex.getMessage());
		}

	}
}
