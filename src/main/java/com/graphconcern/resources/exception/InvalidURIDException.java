package com.graphconcern.resources.exception;

public class InvalidURIDException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    public InvalidURIDException(String urid) {
	super("Unable to prase : " + urid);
    }

}
