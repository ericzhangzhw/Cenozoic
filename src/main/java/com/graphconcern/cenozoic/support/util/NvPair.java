package com.graphconcern.cenozoic.support.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * NvPair is a Name-Value Pair designed for fast serialization and very compact storage.<br/></br/>
 * 
 * @author Felix Chiu
 *
 */
public class NvPair {
    private static final int MAX_BUFFER = 50 * 1024 * 1024;	// 50 MB
    private static final int MAX_LABELS = 50000;			// Maximum number of items in a map
    private static final int COMPRESSED_SIGNATURE = -2010;	// signature
    private static final int THRESHOLD = 2000;				// Threshold for compression decision
    private static final String TRUE = "y";
    private static final String FALSE = "n";
    
    private Map<String, byte[]> thismap = new HashMap<String, byte[]>();
    private int origSize = -1;
    
    public NvPair() {}

    public NvPair(byte[] b) {
    	if (b != null) {
    		try {
				if (!load(b)) thismap.clear();
			} catch (IOException e) {
				thismap.clear();
			} catch (DataFormatException e) {	// incorrect data compression format
				thismap.clear();
			} 
    	}
    }  
    /**
     * @return -1 if uncompressed. Otherwise return original data block length
     */
    public int length() {
    	return origSize;
    }

    public void put(String name, String value) {
    	if (name != null && name.length() < 255) {
        	if (value == null) {
        		thismap.remove(name);
        	} else {
				try {
					if (thismap.size() < MAX_LABELS) thismap.put(name, value.getBytes("UTF-8"));
				} catch (UnsupportedEncodingException e) {}
        	}
		}
    }

    public void put(String name, byte[] value) {
        if (name != null && name.length() < 255) {
        	if (value == null) {
        		thismap.remove(name);
        	} else {
            	if (thismap.size() < MAX_LABELS) thismap.put(name, value);
        	}
        }
    }
    
    public void put(String name, long value) {
    	put(name, value+"");
    }
    
    public void put(String name, int value) {
    	put(name, value+"");
    }
    
    public void put(String name) {
    	put(name, TRUE);
    }
    
    public void put(String name, boolean value) {
    	if (name != null) put(name, value?TRUE:FALSE);
    }
    
	public void remove(String name) {
		thismap.remove(name);
	}
	
	public void clear() {		
		thismap.clear();
	}

	public byte[] get(String name) {
		return thismap.get(name);
	}
	
	public String getString(String name) {
		byte[] entry = thismap.get(name);
		if (entry != null) {
			try {
				return new String(entry, "UTF-8");
			} catch (UnsupportedEncodingException e) { }
		}
		return null;
	}
	
	public boolean isInteger(String name) {
		byte[] entry = thismap.get(name);
		if (entry != null) {
			try {
				Integer.parseInt(new String(entry));
				return true;
			} catch (NumberFormatException e) {
				return false;
			}
		}
		return false;		
	}
	
	public boolean isLong(String name) {
		byte[] entry = thismap.get(name);
		if (entry != null) {
			try {
				Long.parseLong(new String(entry));
				return true;
			} catch (NumberFormatException e) {
				return false;
			}
		}
		return false;		
	}	
	
	public boolean isFloat(String name) {
		byte[] entry = thismap.get(name);
		if (entry != null) {
			try {
				Float.parseFloat(new String(entry));
				return true;
			} catch (NumberFormatException e) {
				return false;
			}
		}
		return false;		
	}		
	
	public int getInt(String name) {
		byte[] entry = thismap.get(name);
		if (entry != null) {
			try {
				return Integer.parseInt(new String(entry));
			} catch (NumberFormatException e) {}
		}
		return -1;
	}
	
	public long getLong(String name) {
		byte[] entry = thismap.get(name);
		if (entry != null) {
			try {
				return Long.parseLong(new String(entry));
			} catch (NumberFormatException e) {}
		}
		return -1;
	}
	
	public float getFloat(String name) {
		byte[] entry = thismap.get(name);
		if (entry != null) {
			try {
				return Float.parseFloat(new String(entry));
			} catch (NumberFormatException e) {}
		}
		return -1.0f;
	}
	
	public boolean getBoolean(String name) {
		String s = getString(name);
		if (s == null) return false;
		return s.equalsIgnoreCase(TRUE) || s.equalsIgnoreCase("true");
	}	

	public boolean exists(String name) {
		return thismap.containsKey(name);
	}

	public int size() {
		return thismap.size();
	}
	public boolean isEmpty() {
		return thismap.isEmpty();
	}
	
	public List<String> getKeys() {
		List<String> rv = new ArrayList<String>();
		if (thismap.size() == 0) return rv;

		for (String key: thismap.keySet()) {
			rv.add(key);
		}
		return rv;
	}
	
	public List<String> getSortedKeys() {
		List<String> rv = getKeys();
		if (rv.size() > 1) Collections.sort(rv);
		return rv;
	}
	
	public List<String> getReverseKeys() {
		List<String> rv = getKeys();
		if (rv.size() > 1) Collections.sort(rv, Collections.reverseOrder());
		return rv;
	}
	
	public byte[] toCompressedBytes() {
		byte[] data = toByteArray();
		origSize = data.length;
		/*
		 * Compress data if larger than a threshold
		 */
		if (data.length > THRESHOLD) {
		    Deflater compressor = new Deflater(Deflater.BEST_SPEED);
		    compressor.setInput(data);
		    compressor.finish();
		    int m = compressor.deflate(data);
		    compressor.end();
		    ByteArrayOutputStream out = new ByteArrayOutputStream();
		    try {
				out.write(int2bytes(COMPRESSED_SIGNATURE));
				out.write(int2bytes(m));
				out.write(int2bytes(data.length));
				out.write(data, 0, m);
				return out.toByteArray();
			} catch (IOException e) {}
		}
	    return data;
	}
	
	public String toString() {
		return getKeys().toString();
	}
	
	public byte[] toByteArray() {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		int cols = thismap.size();
		if (cols == 0) {
			/*
			 * EMPTY DATASET is represented by a single byte of zeros
			 */
	        out.write(0);
	        return out.toByteArray();
		}
		int maxBuffer = 0;
        List<String> keys = getKeys();
        byte[][] elements = new byte[cols][];
        
        BitMap bs = new BitMap(keys.size());   
        for (int i=0; i < keys.size(); i++) {
			elements[i] = thismap.get(keys.get(i));
			if (elements[i].length > maxBuffer) maxBuffer = elements[i].length;
			bs.set(i, elements[i].length > 254);
        }
        byte[] bsBytes = bs.toByteArray();
        
        try {
            /*
             * First, number of labels
             */
			out.write(int2bytes(cols));
			/*
			 * Second, max buffer for each element
			 */
	        out.write(int2bytes(maxBuffer));
	        /*
	         * Then, save element-length Bit set
	         */
	        out.write(int2bytes(bsBytes.length));
	        out.write(bsBytes);
			out.write(100);
	        byte[] label;
	        int len;
	        /*
	         * Save labels
	         */
	        for (String k: keys) {
	        	label = k.getBytes("UTF-8");
	        	len = label.length > 254 ? 254 : label.length;
	        	out.write(len);
	        	out.write(label, 0, len);        	
	        }
	        out.write(101);
	        /*
	         * Save elements
	         */
			for (int i=0; i < elements.length; i++) {
				len = elements[i].length;
				if (len > 254) {
					out.write(int2bytes(len));	// 4 bytes for large element
				} else {
					out.write(len);				// 1 byte for small element
				}
				out.write(elements[i]);
			} 
			/*
			 * End of file
			 */
			out.write(0);
			
		} catch (IOException e) {
			out.reset();
			try {
				out.write(int2bytes(0));
			} catch (IOException e1) {}
		}
		return out.toByteArray();        
	}
	
	protected boolean load(byte[] b) throws IOException, DataFormatException {
		ByteArrayInputStream in = new ByteArrayInputStream(b);
		
		byte[] lenbytes = new byte[4];
		if (in.read(lenbytes) < 4) return false;
		int cols = bytes2int(lenbytes);
		if (cols == COMPRESSED_SIGNATURE) {
			if (in.read(lenbytes) < 4) return false;
			int compressed = bytes2int(lenbytes);
			if (compressed != b.length - 12) return false;
			if (in.read(lenbytes) < 4) return false;
			int original = bytes2int(lenbytes);
			if (original <= 0 || original > MAX_BUFFER) return false;			
		    Inflater decompresser = new Inflater();
		    decompresser.setInput(b, 12, compressed);
		    byte[] data = new byte[original];
		    if (decompresser.inflate(data) != original) return false;
		    origSize = original;
		    decompresser.end();
		    return load(data);
		}		
		if (cols <= 0 || cols > MAX_LABELS) return false;
		
		if (in.read(lenbytes) < 4) return false;
		int maxBuffer = bytes2int(lenbytes);
		if (maxBuffer <=0 || maxBuffer > MAX_BUFFER) return false;
		
		if (in.read(lenbytes) < 4) return false;
		int n = bytes2int(lenbytes);
		if (n <=0 || n > MAX_LABELS) return false;		
		byte[] bsBytes = new byte[n];
		if (in.read(bsBytes) != n) return false;
		BitMap bs = new BitMap(bsBytes);
		if (bs.length() > cols) return false;		
		/*
		 * Marker #1
		 */
		if (in.read() != 100) return false;

		String[] labels = new String[cols];
		/*
		 * Read label
		 */
		for (int i=0; i < cols; i++) {
			n = in.read();
			if (n < 0) return false;
			byte[] labelbytes = new byte[n];
			if (in.read(labelbytes) != n) return false;
			labels[i] = new String(labelbytes, "UTF-8");
		}
		/*
		 * Marker #2
		 */
		if (in.read() != 101) return false;
		/*
		 * Read elements
		 */
		for (int i=0; i < cols; i++) {
			if (bs.get(i)) {
				if (in.read(lenbytes) < 4) return false;
				n = bytes2int(lenbytes);
				if (n <=0 || n > maxBuffer) return false;
			} else {
				n = in.read();
				if (n < 0) return false;
			}
			byte[] element = new byte[n];
			if (in.read(element) != n) return false;
			thismap.put(labels[i], element);			
		}
		/*
		 * Marker #3
		 */
		if (in.read() != 0) return false;
		
		return true;		
	}
	
	protected static int bytes2int(byte[] b) {
		int val = 0;
		for (int i = 0 ; i < 4 && i < b.length ; ++i) {
			val *= 256 ;
			if ((b[i] & 0x80) == 0x80) {
				val += 128 ;
			}
			val += (b[i] & 0x7F) ;
		}
		return val;
	}

	protected static byte[] int2bytes(int v) {
		byte[] results = new byte[4];

		for (int idx = 3; idx >=0; --idx) {
			results[idx] = (byte) (v & 0xFF);
			v = v >> 8;
		}
		return results;
	}
	


}
