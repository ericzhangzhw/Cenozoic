package com.graphconcern.cenozoic.search.sdk.exception;

public class CLuceneException extends Exception {
	
	private static final long serialVersionUID = 5156367814772633416L;

	public CLuceneException(String msg) {
		super(msg);
	}
	
	public CLuceneException(String msg, Throwable throwable) {
		super(msg, throwable);
	}
	
}
