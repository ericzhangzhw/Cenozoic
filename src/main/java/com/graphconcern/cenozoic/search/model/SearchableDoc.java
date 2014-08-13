package com.graphconcern.cenozoic.search.model;

import java.util.Hashtable;
import java.util.Set;

public class SearchableDoc {
	
	private Hashtable<String, String> attributes;
	
	public SearchableDoc(Hashtable<String, String> attributes) {
		this.attributes = attributes;
	}
	
	public int size() {
		if (this.attributes==null) return 0;
		else return this.attributes.size();
	}
	
	public String get(String key) {
		if (this.attributes==null) return null;
		return attributes.get(key);
	}
	
	public Set<String> keySet() {
		if (this.attributes==null) return null;
		return attributes.keySet();
	}
	
	
}
