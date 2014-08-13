package com.graphconcern.search.sdk.util;

public class Constants {

	public final static String PARAM_SERVICE = "service";
	public final static String PARAM_SERVICE_SEARCH = "search";
	public final static String PARAM_SERVICE_INDEX = "index";
	
	public final static String PARAM_SPACE = "space";
	public final static String PARAM_HEADER = "header";
	public final static String PARAM_BODY = "body";
	public final static String PARAM_QUERY_STRING = "query_string";
	public final static String PARAM_CONTENT = "content";

	public final static String TYPE_KEY_ID = "id";
	public final static String TYPE_KEY_INDEX = "index";
	public final static String TYPE_KEY_STORE = "store";
	public final static String TYPE_KEY_INDEX_STORE = "index_store";
	
	public final static String TYPE_KEY_UNTOKENIZED = "untokenized";
	
	public final static String TYPE_KEY_SEPARATOR = ":";
	
	public final static String TYPE_KEY_INDEX_UNTOKENIZED = TYPE_KEY_INDEX + TYPE_KEY_SEPARATOR + TYPE_KEY_UNTOKENIZED; 
	public final static String TYPE_KEY_STORE_UNTOKENIZED = TYPE_KEY_STORE + TYPE_KEY_SEPARATOR + TYPE_KEY_UNTOKENIZED; 
	public final static String TYPE_KEY_INDEX_STORE_UNTOKENIZED = TYPE_KEY_INDEX_STORE + TYPE_KEY_SEPARATOR + TYPE_KEY_UNTOKENIZED; 

	
	
	// Reply Message
	public final static String REPLY_HTTP_STATUS_KEY = "http_status";
	public final static int REPLY_HTTP_STATUS_OK = 200;
	public final static int REPLY_HTTP_STATUS_FNF = 404;
	public final static String REPLY_MESSAGE_KEY = "reply_message";
	public final static String REPLY_RESULT_SET_KEY = "result_set";
}
