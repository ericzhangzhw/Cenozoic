package com.graphconcern.cenozoic.support.util;

import java.util.BitSet;

/**
 * BitMap adds data serialization to java.util.BitSet
 * 
 * @author Felix Chiu
 *
 */
public class BitMap {
	private BitSet bset;
	private int size;
	
	/**
	 * Avoid using default constructor
	 * @throws Exception 
	 */
	public BitMap() throws Exception {
		size = Integer.MAX_VALUE;
		bset = new BitSet();
		throw new Exception("Default constructor not recommended");
	}
	
	public BitMap(int size) {
		this.size = size;
		bset = new BitSet(size);
	}
	
	public BitMap(byte[] b) {
		size = b.length * 8;
		bset = new BitSet(size);
		int k=0;
		for (int i=0; i < b.length; i++) {
			for (int j=0; j < 8; j++) bset.set(k++, (b[i] & (1 << j)) > 0);
		}
	}
	public String toString() {
		if (bset.length() == 0) return "0";
		StringBuffer sb = new StringBuffer();
		for (int i=0; i < bset.length(); i++) {
			if (i > 0) sb.append(' ');
			sb.append(bset.get(i)? '1':'0');
		}
		return sb.toString();
	}
	
	public void set(int n) {
		if (n < size) bset.set(n);
	}
	public void set(int n, boolean state) {
		if (n < size) bset.set(n, state);
	}
	
	public void clear(int n) {
		if (n < size) bset.clear(n);
	}
	
	public void clear() {
		bset.clear();
	}
	
	public boolean get(int n) {
		return (n < size) ? bset.get(n) : false;
	}
	
	public int length() {
		return bset.length();
	}	
	
	public byte[] toByteArray() {
		int len = bset.length();
		if (len == 0) {
			/*
			 * EMPTY DATASET is represented by a single byte of zeros
			 */
			byte[] empty = new byte[1];
			empty[0] = 0;
			return empty;
		}
		int n = (len+7)/8;
		byte[] b = new byte[n];
		int k = 0;
		for (int i=0; i < n; i++) {
			for (int j=0; j < 8; j++) {
				if (bset.get(k++)) b[i] |= (1 << j);
				if (k > len) break;
			}
		}
		return b;			
	}	
}


