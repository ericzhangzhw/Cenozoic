package com.graphconcern.cenozoic.datastore;

public class CassandraException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    CassandraException(Exception ex) {
	super(ex);
    }

}
