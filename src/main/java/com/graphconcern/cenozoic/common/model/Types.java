package com.graphconcern.cenozoic.common.model;

import java.util.HashMap;
import java.util.Map;

import com.graphconcern.cenozoic.resources.exception.InvalidURIDException;

public enum Types {
    ACCOUNT("accounts"),
    CONTEXT("contexts"),
    DOCUMENT_SERIES("documentseries"),
    DOCUMENT_VERSION("documentversion"),
    EVENT("events");
    
    private static Map<String, Types> namesLookup = new HashMap<String, Types>();
    
    static {
	namesLookup = new HashMap<String, Types>();
	for (Types type : Types.values()) {
	    namesLookup.put(type.getTypeName(), type);
	}
    }
    
    private final String typeName;
    
    public String getTypeName() {
	return typeName;
    }
    
    public static Types getTypeByName(String key) throws InvalidURIDException {
	if (namesLookup.containsKey(key)) {
	    return namesLookup.get(key);
	}
	throw new InvalidURIDException("Invalid data type specified: " + key);
    }
    
    private Types(final String typeName){
	this.typeName = typeName;
    }
    
}
