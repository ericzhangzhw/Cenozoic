package com.graphconcern.cenozoic.search.servlets;

import java.io.IOException;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.graphconcern.cenozoic.search.util.UtilConstants;
import com.graphconcern.cenozoic.support.util.Utility;

public class SearchAsyncListener implements AsyncListener {
	
	private static final Logger log = LoggerFactory.getLogger(SearchAsyncListener.class);
	
	private static Utility util = new Utility();

	@Override
	public void onComplete(AsyncEvent event) throws IOException {
		String sid = (String) event.getSuppliedRequest().getAttribute(UtilConstants.TYPE_KEY_ID);
		log.debug("onComplete Method with SID: " + sid);
    	if (sid != null) {
    		if (SearchService.asyncCtx.containsKey(sid)) {
    			log.debug("Removing completed "+sid+" from AsyncContext");
    			SearchService.asyncCtx.remove(sid);
    		}
    	}
//    	statusUpdate("onComplete", event.toString());
	}

	@Override
	public void onTimeout(AsyncEvent event) throws IOException {
		String sid = (String) event.getSuppliedRequest().getAttribute(UtilConstants.TYPE_KEY_ID);
		log.info("onTimeout Method with SID: " + sid);
    	if (sid != null) {
    		AsyncContext c = event.getAsyncContext();
    		util.sendJsonError((HttpServletResponse) c.getResponse(), 408, "Timeout");
    		SearchService.asyncCtx.remove(sid);
    		log.debug("Removing expired "+sid+" from AsyncContext");
    		c.complete();
    	}
//    	statusUpdate("onTimeout", event.toString());
	}

	@Override
	public void onError(AsyncEvent event) throws IOException {
		String sid = (String) event.getSuppliedRequest().getAttribute(UtilConstants.TYPE_KEY_ID);
		log.debug("onError Method with SID: " + sid);
    	if (sid != null) {
    		SearchService.asyncCtx.remove(sid);
    	}
//    	statusUpdate("onError", event.toString());
	}

	@Override
	public void onStartAsync(AsyncEvent event) throws IOException {
		// Do nothing
//		statusUpdate("onStartAsync", event.toString());
	}
	
//	private void statusUpdate(String method, String message) {
//		log.info("\t"
//				+ "Method: " + method + "\n" + "\t"
//						+ "Message: " + message);
//	}

}
