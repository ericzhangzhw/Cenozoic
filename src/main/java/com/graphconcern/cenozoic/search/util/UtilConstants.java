package com.graphconcern.cenozoic.search.util;

import org.apache.lucene.util.Version;

public class UtilConstants {
	
	public final static String QUEUE_INDEX_WORKER = "private.clucene.index.worker";
	public final static String QUEUE_SEARCH_WORKER = "private.clucene.search.worker";
	public final static String QUEUE_OPTIMIZE_WORKER = "private.clucene.optimize.worker";
	
	public final static String LOCK_MASTER = "clucene.lock.master";
	
	public final static String SERVICE_KEY_INDEX = "index";
	public final static String SERVICE_KEY_SEARCH = "search";

	public final static String TYPE_KEY_ID = "id";
	public final static String TYPE_KEY_INDEX = "index";
	public final static String TYPE_KEY_STORE = "store";
	public final static String TYPE_KEY_INDEX_STORE = "index_store";
	
	public final static String TYPE_KEY_UNTOKENIZED = "untokenized";
	
	public final static String TYPE_KEY_SEPARATOR = ":";
	
	public final static String TYPE_KEY_INDEX_UNTOKENIZED = TYPE_KEY_INDEX + TYPE_KEY_SEPARATOR + TYPE_KEY_UNTOKENIZED; 
	public final static String TYPE_KEY_STORE_UNTOKENIZED = TYPE_KEY_STORE + TYPE_KEY_SEPARATOR + TYPE_KEY_UNTOKENIZED; 
	public final static String TYPE_KEY_INDEX_STORE_UNTOKENIZED = TYPE_KEY_INDEX_STORE + TYPE_KEY_SEPARATOR + TYPE_KEY_UNTOKENIZED; 

	
	public final static String CONTENT_KEY_HEADER = "header";
	public final static String CONTENT_KEY_BODY = "body";
	public final static String CONTENT_KEY_QUERY_STRING = "query_string";
	
	public final static String PARAM_KEY_REQUEST_SPACE = "space";
	public final static String PARAM_KEY_REQUEST_CONTENT = "content";
	public final static String PARAM_KEY_REQUEST_SERVICE = "service";
	
	public final static String CONCURRENT_INDEX_WORKER = "concurrent.indexer";
	public final static String CONCURRENT_SEARCH_WORKER = "concurrent.searcher";
	 
	public final static String REPLY_HTTP_STATUS_KEY = "http_status";
	public final static int REPLY_HTTP_STATUS_OK = 200;
	public final static int REPLY_HTTP_STATUS_FNF = 404;
	public final static String REPLY_MESSAGE_KEY = "reply_message";
	
	public final static String REPLY_KEY_TOTAL_HITS = "total_hits";
	public final static String REPLY_KEY_RESULT_SET = "result_set";
	
	public final static String STATUS_KEY = "status";
	public final static String STATUS_OK = "ok";
	public final static String STATUS_FOLLOWUP = "followup";
	public final static String STATUS_ERROR = "error";
	
	
	public static final long INTRANET_TIMEOUT = 15000;
	
	public static final int OPTIMIZATION_WAIT = 100;
	
	public static final int OPTIMIZATION_MAX_NUM_OF_SEGMENTS = 50;
	
	public static final Version LUCENE_VERSION = Version.LUCENE_48;

}
