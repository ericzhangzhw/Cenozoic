package com.graphconcern.cenozoic.resources.exception;

public class ResourceNotFoundException extends Exception {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public ResourceNotFoundException(String msg) {
	super(msg);
    }

}
