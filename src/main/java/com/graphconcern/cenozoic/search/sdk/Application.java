package com.graphconcern.cenozoic.search.sdk;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.graphconcern.cenozoic.search.sdk.exception.CLuceneException;
import com.graphconcern.cenozoic.search.sdk.util.Constants;
import com.graphconcern.cenozoic.search.sdk.util.Utility;

public class Application {

	public static void main(String args[]) throws ClientProtocolException, IOException {

		if (args==null || args.length < 2) {
			System.out.println("Usage: [url] [file path]");
			System.exit(1);
		}

		String url = args[0];
		String filePath = args[1];

		System.out.println("url : " + url);
		System.out.println("file path : " + filePath);
		
		Utility util = new Utility();

		HttpPost httpPost = new HttpPost(url);
		
		File file = new File(filePath);
		String content = util.file2str(file);
		System.out.println("Content : " + content);
		StringEntity entity = new StringEntity(content, 
				ContentType.create("application/json", "UTF-8"));
		httpPost.setEntity(entity);

		CloseableHttpClient httpclient = HttpClients.createDefault();
		CloseableHttpResponse response = httpclient.execute(httpPost);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		response.getEntity().writeTo(baos);
		System.out.println(baos.toString());
	}

	/****************************************
	 * Search Example
	 ****************************************/
	public static void searchExample(String url, String space) {

		SearchRequest searchRequest = new SearchRequest(url, space);

		searchRequest.addQueryString("name", "J*");

		try {
			HashMap<String, HashMap<String, String>> resultSet = searchRequest.submit();

			if (resultSet == null) {
				System.out.println("There is no result with your submission query string");
			} else {
				System.out.println("Total Number of Records : " + resultSet.size());
				for (String key : resultSet.keySet()) {
					System.out.println("\tDocument Number : " + key);
					HashMap<String, String> item = resultSet.get(key);
					for (String k : item.keySet()) {
						System.out.println("\t\t" + k + " : " + item.get(k));
					}
				}
			}

		} catch (CLuceneException e) {
			e.printStackTrace();
		}
	}

	/****************************************
	 * Index Example
	 ****************************************/
	public static void indexExample(String url, String space) {

		IndexRequest indexRequest = new IndexRequest(url, space);

		indexRequest.define("staffid", Constants.TYPE_KEY_ID, "230")
		.define("name", Constants.TYPE_KEY_INDEX_STORE, "Jerry Harry")
		.define("address", Constants.TYPE_KEY_INDEX_STORE, "1800 Heyon Street");

		try {	
			boolean result = indexRequest.submit();

			System.out.println("Result is: " + result);

		} catch (CLuceneException e) {
			e.printStackTrace();
		}
	}


}
