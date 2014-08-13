package com.graphconcern.cenozoic.search.sdk.util;



import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URLEncoder;
import java.security.SecureRandom;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;

public class Utility {
	public enum OS {WINDOWS, OSX, LINUX, SOLARIS, UNKNOWN};
	private String[] officeExt = {"pdf", "ppt", "pptx", "xls", "xlsx", "doc", "docx", "txt", "rtf", "odt", "ods", "odp", "odg"};
	private String[] pptExt = {"ppt", "pptx"};
	private static final String JSON_BACKSLASH = "\\/";
	
	public String escapeHTML(String html) {
		String result = html;
		if (result.contains("&")) result = result.replace(">", "&amp;");
		if (result.contains("\"")) result = result.replace(">", "&quot;");
		if (result.contains("\'")) result = result.replace(">", "&#39;");
		if (result.contains(">")) result = result.replace(">", "&gt;");
		if (result.contains("<")) result = result.replace("<", "&lt;");
		return result;
	}
	
	public String generateUUID() {
		return UUID.randomUUID().toString();
	}
	
	public byte[] getRandomBytes(int n) {
		SecureRandom r = new SecureRandom();
		byte[] b = new byte[n];
		r.nextBytes(b);
		return b;
	}
	/*
	 * Return a random string of base62 characters (0=9, a-z, A-Z)
	 */
	public String getRandomString(int n) {
		SecureRandom r = new SecureRandom();
		StringBuffer sb = new StringBuffer();
		for (int i=0; i < n; i++) {
			int x = r.nextInt(62);
			if (x < 10) {
				sb.append((char) ('0' + x));
			} else if (x < 36) {
				sb.append((char) ('a' + x - 10));
			} else {
				sb.append((char) ('A' + x - 36));
			}
		}
		return sb.toString();
	}
	
	public String getRandomDigits(int n) {
		SecureRandom r = new SecureRandom();
		StringBuffer sb = new StringBuffer();
		for (int i=0; i < n; i++) {
			sb.append((char) ('0' + r.nextInt(10)));
		}
		return sb.toString();
	}
	
	public String long2uniqueTime(long t) {
		return long2sortableTime(t) + "_"+ getRandomString(10);
	}
	
	public String long2sortableTime(long t) {
		return new SimpleDateFormat("yyyyMMddHHmmss.SSS").format(new Date(t));
	}
	
	public String long2rfc3339(long t, boolean milliseconds) {
		String s = milliseconds ? 	new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").format(new Date(t)) :
									new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(new Date(t));
		return s.substring(0, s.length()-2)+":"+s.substring(s.length()-2);
	}
	
	public long rfc3339ToLong(String s) {
		SimpleDateFormat formatter = null;
		if (s.endsWith("Z")) {
			if (s.contains(".")) {
				formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");
			} else {
				formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			}
		} else {
			if (s.contains(".")) {
				formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSX");
			} else {
				formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
			}
		}
		if (formatter != null) {
			formatter.setLenient(true);
			try {
				return formatter.parse(s).getTime();
			} catch (ParseException e) {
				// just ignore
			}
		}
		return 0;
	}
	
	public String formatNumber(long m) {
		boolean negative = m < 0;
		long n = negative? m * -1 : m;	// absolute value		
		if (n < 1000) return negative? "-"+n : ""+n;
		
		String number = n+"";
		StringBuffer sb = new StringBuffer();
		int c = 0;
		for (int i=number.length()-1; i >= 0; i--) {
			c++;
			sb.append(number.charAt(i));
			if (c == 3) {
				sb.append(',');
				c = 0;
			}
		}
		if (sb.charAt(sb.length()-1) == ',') {
			sb.setLength(sb.length()-1);
		}
		String s = sb.reverse().toString();
		return negative? "-"+s : s;
	}
	
	public boolean isHex(String s) {
		int len = s.length();
		if (len > 0 && len % 2 == 0) {
			for (int i=0; i < len; i++) {
				if (s.charAt(i) >= '0' && s.charAt(i) <= '9') continue;
				if (s.charAt(i) >= 'a' && s.charAt(i) <= 'z') continue;
				if (s.charAt(i) >= 'A' && s.charAt(i) <= 'Z') continue;
				return false;
			}
			return true;
		} else {
			return false;
		}
	}
	
	public boolean isNumFelix(String s) {
		for (int i=0; i < s.length(); i++) {
			if (s.charAt(i) >= '0' && s.charAt(i) <= '9') continue;
			return false;
		}
		return true;
	}
	
	public String file2str(File f) {
		return getUTF(file2bytes(f));
	}
	public long file2long(File file) {
		byte[] number = file2bytes(file);
		if (number.length != 8) return -1;
		return bytes2long(number);
	}	
	public boolean long2file(File file, long number) {
		createParent(file);
		FileOutputStream out = null;
		try {
			out = new FileOutputStream(file);
			out.write(long2bytes(number));
			out.close();
			out = null;
			return true;
		} catch (IOException e) {
			return false;
		} finally { 
			if (out != null)
				try {
					out.close();
				} catch (IOException e) {}
		}
	}
	public boolean bytes2file(File file, byte[] b) {
		createParent(file);
		FileOutputStream out = null;
		try {
			out = new FileOutputStream(file);
			out.write(b);	
			out.close();
			out = null;
			return true;
		} catch (IOException e) {
			return false;
		} finally {
			if (out != null)
				try {
					out.close();
				} catch (IOException e) {
				}
		}		
	}	
	public boolean str2file(File file, String s) {
		createParent(file);
		FileOutputStream out = null;
		try {
			out = new FileOutputStream(file);
			out.write(getUTF(s));	
			out.close();
			out = null;
			return true;
		} catch (IOException e) {
			return false;
		} finally {
			if (out != null)
				try {
					out.close();
				} catch (IOException e) {
				}
		}		
	}
	
	public byte[] file2bytes(File f) {
		try {
			return stream2bytes(new FileInputStream(f), true);
		} catch (FileNotFoundException e1) {
			return new byte[0];
		}		
	}	
	public byte[] stream2bytes(InputStream stream) {
		return stream2bytes(stream, true);
	}	
	public byte[] stream2bytes(InputStream stream, boolean closeStream) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		int len = 0;
		byte[] buffer = new byte[1024];		
		if (stream instanceof ByteArrayInputStream) {
			ByteArrayInputStream in = (ByteArrayInputStream) stream;
			while ((len = in.read(buffer, 0, buffer.length)) != -1) {
				out.write(buffer, 0, len) ;
			}
		} else {
			BufferedInputStream bin = (stream instanceof BufferedInputStream) ? (BufferedInputStream) stream : new BufferedInputStream(stream);
			try {
				while ((len = bin.read(buffer, 0, buffer.length)) != -1) {
					out.write(buffer, 0, len) ;
				}
				if (closeStream) bin.close();
				bin = null;
			} catch (IOException e) {
			} finally {
				if (bin != null && closeStream)
					try {
						bin.close();
					} catch (IOException e) {}
			}
		}
		return out.toByteArray();
	}
	
	public void copyfile(File input, File output) {
		if (!input.exists()) return;
		File parent = output.getParentFile();
		if (!parent.exists()) parent.mkdirs();
		BufferedInputStream in = null;
		BufferedOutputStream out = null;
		try {
			in = new BufferedInputStream(new FileInputStream(input));
			out = new BufferedOutputStream(new FileOutputStream(output));			
			int len = 0;
			byte[] buffer = new byte[1024];
			while ((len = in.read(buffer, 0, buffer.length)) != -1) {
				out.write(buffer, 0, len) ;
			}
			out.close();
			out = null;
			in.close();
			in = null;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (out != null)
				try {
					out.close();
				} catch (IOException e) {}
			if (in != null)
				try {
					in.close();
				} catch (IOException e) {}
		}
	}
	
	public byte[] getUTF(String s) {
		if (s == null) return new byte[0];
		if (s.length() == 0) return new byte[0];
		try {
			return s.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			return s.getBytes();
		}		
	}
	public String getUTF(byte[] b) {
		if (b == null) return "";
		if (b.length == 0) return "";
		try {
			return new String(b, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return new String(b);
		}
	}
	
	public long str2long(String s) {
		try {
			return Long.parseLong(s);
		} catch (NumberFormatException e) {
			return -1;
		}
	}
	
	public int str2int(String s) {
		try {
			return Integer.parseInt(s);
		} catch (NumberFormatException e) {
			return -1;
		}
	}
	
	public byte[] long2bytes(long val) {
		byte[] results = new byte[8];

		for (int idx =7; idx >=0; --idx) {
			results[idx] = (byte) (val & 0xFF);
			val = val >> 8;
		}
		return results;
	}
	
	public long bytes2long(byte[] b) {
		if (b == null) return -1;
		if (b.length != 8) return -1;
		long val = 0;
		for (int i = 0 ; i < 8 && i < b.length; ++i) {
			val *= 256L ;
			if ((b[i] & 0x80) == 0x80) {
				val += 128L ;
			}
			val += (b[i] & 0x7F) ;
		}
		return val;
	}
	
	private void createParent(File f) {
		File parent = f.getParentFile();
		if (parent != null && !parent.exists()) parent.mkdirs();
	}
	
	public String getPrefix(String s) {
		String ext = getExt(s);
		if (ext == null) {
			return s;
		} else {
			return s.substring(0, s.length()-ext.length()-1);
		}
	}
	
	public String getExt(String s) {
		if (s == null) return null;
	    int i = s.lastIndexOf('.');
	    if(i>0 && i < s.length()-1) return s.substring(i+1).toLowerCase();
		return null;
	}
	
	public boolean isOfficeDoc(List<String> extensions) {
		if (extensions == null) return false;
		
		for (String ext: extensions) {
			for (String s: officeExt) {
				if (s.equalsIgnoreCase(ext)) return true;
			}
		}
		return false;
	}
	
	public boolean isOfficeDoc(String filename) {
		String ext = getExt(filename);
		if (ext == null) return false;
		
		for (String s: officeExt) {
			if (s.equalsIgnoreCase(ext)) return true;
		}
		return false;
	}
	
	public boolean isPPT(String filename) {
		String ext = getExt(filename);
		if (ext == null) return false;
		
		for (String s: pptExt) {
			if (s.equalsIgnoreCase(ext)) return true;
		}
		return false;
	}
	
	public void cleanupDir(File dir) {
		cleanupDir(dir, false);
	}
	
	public void cleanupDir(File dir, boolean keep) {
		if (dir != null && dir.exists() && dir.isDirectory()) {
			File[] files = dir.listFiles();
			if (files != null) {
				for (File f: files) {
					if (f.isDirectory()) {
						cleanupDir(f, false);
					} else {
						f.delete();
					}				
				}
			}
			if (!keep) dir.delete();
		}
	}
	
  	public boolean portReady(String host, int port, int timeout) {
  		InetSocketAddress target = new InetSocketAddress(host, port);
  		Socket s = null;
  		try {
  			s = new Socket();
  			s.setReuseAddress(true);
  			s.bind(null);
  			s.connect(target, timeout);
  			s.close();
  			s = null;
  			return true;
  		} catch (SocketTimeoutException te) {
  			try {
  				s.close();
  	  			s = null;
  	  			return false;
  			} catch (IOException e) {}
  		} catch (IOException ie) {
  			try {
  				s.close();
  	  			s = null;			
  			} catch (IOException e) {}
  		} finally {
  			if (s != null) {
  				try {
  					s.close();
  				} catch (IOException e) {}
  			}
  		}
  		return false;
  	}
	
	public OS getOS()
	{
		String osValue = System.getProperty("os.name").toLowerCase();
		if (osValue.indexOf("win") >= 0)
		{
			return OS.WINDOWS;
		}
		else if (osValue.indexOf("mac") >= 0)
		{
			return OS.OSX;
		}
		else if((osValue.indexOf("nix") >= 0 || osValue.indexOf("nux") >= 0 || osValue.indexOf("aix") > 0 ))
		{
			return OS.LINUX;
		}
		else if(osValue.indexOf("sunos") >= 0)
		{
			return OS.SOLARIS;
		}
		
		return OS.UNKNOWN;
	}
	
	/*
	 * Find element using the dot convention.
	 * Assume key name does not have "."
	 * e.g. "profile.name.firstname"
	 */
	public Object getJsonElement(String path, JSONObject obj) {
		if (path == null || obj == null) return null;
		
		List<String> list = split(path, "."); 
		JSONObject o = obj;
		int len = list.size();
		int n = 0;
		for (String p: list) {
			n++;
			if (o.containsKey(p)) {
				Object x = o.get(p);
				if (n == len) {
					return x;
				} else {
					if (x instanceof JSONObject) {
						o = (JSONObject) x;
					} else {
						return null;
					}
				}
			} else {
				return null;
			}
		}
		return null;
	}
	/*
	 * Find element using the dot and bracket convention.
	 * e.g. 
	 * key.element
	 * key[2].element[3].sub_element
	 * 
	 */
	@SuppressWarnings("rawtypes")
	public Object getMapElement(String path, Map obj) {
		if (path == null || obj == null) return null;
		
		if (!path.contains(".") && !path.contains("[")) return obj.get(path);
		
		List<String> list = split(path, "."); 
		Map o = obj;
		int len = list.size();
		int n = 0;
		for (String p: list) {
			n++;
			/*
			 * key is an array list?
			 */
			if (p.contains("[") && p.endsWith("]") && !p.startsWith("[")) {
				int bracketStart = p.indexOf('[');
				int bracketEnd = p.lastIndexOf(']');
				if (bracketStart > bracketEnd) return null;
				
				String key = p.substring(0, bracketStart);
				String index = p.substring(bracketStart+1, bracketEnd).trim();
				
				if (index.length() == 0) return null;
				if (!isNumFelix(index)) return null;
				
				int i = str2int(index);
				if (i < 0) return null;
								
				if (o.containsKey(key)) {
					Object x = o.get(key);
					
					if (x instanceof List) {
						List y = (List) x;
						if (i >= y.size()) {
							return null;
						} else {
							if (n == len) {
								return y.get(i);
							} else if (y.get(i) instanceof Map) {
								o = (Map) y.get(i);
								continue;
							}
						}
					}
				}
				return null;
			}
			/*
			 * Target found?
			 */
			if (o.containsKey(p)) {
				Object x = o.get(p);
				if (n == len) {
					return x;
				} else if (x instanceof Map) {
					o = (Map) x;
					continue;
				}
			}
			return null;
		}
		return null;
	}
	
	public String urlEncode(String s) {
		try {
			return URLEncoder.encode(s, "UTF-8").replace("+", "%20");
		} catch (UnsupportedEncodingException e) {
			// impossible case as UTF-8 is guaranteed
			return null;
		}
	}
	/*
	 * For security, return portion of the sessionId
	 */
	public String partialSessionId(Object sessionId) {
		String s = (sessionId instanceof String) ? (String) sessionId : sessionId.toString();
		int slash = s.lastIndexOf('-');
		if (slash > 10) {
			return s.substring(0, slash);
		} else {
			if (s.length() > 2) {
				return s.substring(0, s.length()/2);
			} else {
				return s;
			}
		}
	}
	
	public String list2str(List<String> list) {
		StringBuffer sb = new StringBuffer();
		for (String s: list) {
			sb.append(s);
			sb.append(", ");
		}
		return sb.length() == 0? "" : sb.substring(0, sb.length()-2);
	}
	
	public String json2str(JSONObject json) {
		String s = json.toJSONString();
		return s.contains(JSON_BACKSLASH) ? s.replace(JSON_BACKSLASH, "/") : s;
	}
	
	public byte[] json2bytes(JSONObject json) {
		return getUTF(json2str(json));
	}
	/*
	 * Detect if an IP address is behind a firewall
	 * 
	 * 127.0.0.1 - localhost
	 * 
	 * 10.0.0.0    - 10.255.255.255
 	 * 172.16.0.0  - 172.31.255.255
 	 * 192.168.0.0 - 192.168.255.255
	 */
	public boolean isIntranet(String ipv4) {
		if (ipv4.equals("127.0.0.1")) return true;
		List<String> parts = split(ipv4, ".");
		int[] numbers = new int[4];
		if (parts.size() != 4) return false;
		for (int i=0; i < 4; i++) {
			numbers[i] = str2int(parts.get(i));
		}
		if (numbers[0] == 10 && withinNumberRange(numbers[1], 0, 255) && withinNumberRange(numbers[2], 0, 255) && withinNumberRange(numbers[3], 0, 255)) return true;
		if (numbers[0] == 172 && withinNumberRange(numbers[1], 16, 31) && withinNumberRange(numbers[2], 0, 255) && withinNumberRange(numbers[3], 0, 255)) return true;
		if (numbers[0] == 192 && numbers[1] == 168 && withinNumberRange(numbers[2], 0, 255) && withinNumberRange(numbers[3], 0, 255)) return true;
		return false;
	}
	
	private boolean withinNumberRange(int n, int low, int high) {
		return (low <= n && n <= high);
	}
	
	public List<String> getMyIntranetIP() {
		List<String> list = new ArrayList<String>();
		Enumeration<NetworkInterface> interfaces;
		try {
			interfaces = NetworkInterface.getNetworkInterfaces();
			while (interfaces.hasMoreElements()){
			    NetworkInterface current = interfaces.nextElement();
			    if (!current.isUp() || current.isLoopback() || current.isVirtual()) continue;
			    Enumeration<InetAddress> addresses = current.getInetAddresses();
			    while (addresses.hasMoreElements()){
			        InetAddress current_addr = addresses.nextElement();
			        if (current_addr.isLoopbackAddress()) continue;
			        if (current_addr instanceof Inet4Address)
			        	list.add(current_addr.getHostAddress());
			    }
			}
		} catch (SocketException e) {}
		if (list.size() > 1) Collections.sort(list);
		return list;
	}
	/*
	 * Generate a unique host IP name for use with localized MQ
	 */
	public String getMyIntranetIpName() {
		List<String> list = getMyIntranetIP();
		if (list.isEmpty()) return null;
		return list.get(0).replace('.', '_');
	}
	
	public String getNormalizedWebAppPath(String path) {
		List<String> list = getUriPathElement(path);
		if (list.isEmpty()) return "/";
		StringBuffer sb = new StringBuffer();
		for (String p: list) {
			sb.append('/');
			sb.append(p);
		}
		return sb.toString();
	}
	
	public List<String> getUriPathElement(String path) {
		String[] elements = path.split("/");
		List<String> list = new ArrayList<String>();
		for (String e: elements) {
			if (e.length() > 0) list.add(e);			
		}
		return list;
	}
	
	public void downloadFile(String url, CloseableHttpClient httpclient, File dataFile) {
		/*
		 * Just in case it is a local file
		 */
		if (url.startsWith("file://")) {
			String fileUrl = url.substring(7);
			File inFile = new File(fileUrl);
			if (inFile.exists()) copyfile(inFile, dataFile);
			return;
		}		
		HttpGet httpget = new HttpGet(url);
		FileOutputStream out = null;
		try {
			CloseableHttpResponse response = httpclient.execute(httpget);
			try {
				StatusLine status = response.getStatusLine();
				if (status.getStatusCode() == 200) {
					out = new FileOutputStream(dataFile);
					HttpEntity entity = response.getEntity();
					InputStream in = entity.getContent();
					int len = 0;
					byte[] buffer = new byte[1024];
					
					while ((len = in.read(buffer, 0, buffer.length)) != -1) {
						out.write(buffer, 0, len) ;
					}
					out.flush();
					out.close();
					out = null;
				}
			} finally {
				response.close();
				if (out != null) out.close();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String getHttpPage(String url, CloseableHttpClient httpclient) {
		HttpGet httpget = new HttpGet(url);
		try {
			CloseableHttpResponse response = httpclient.execute(httpget);
			try {
				StatusLine status = response.getStatusLine();
				if (status.getStatusCode() != 200) return null;
				
				HttpEntity entity = response.getEntity();
				byte[] bytes = EntityUtils.toByteArray(entity);
				if (bytes == null) return null;
				return getUTF(bytes);
				
			} finally {
				response.close();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;	
	}
	
	
    public List<String> split(String s, String chars) {
    	return split(s, chars, false);
    }
    
	public List<String> split(String s, String chars, boolean empty) {
		List<String> rv = new ArrayList<String>();
		if (s == null) return rv;
		StringBuffer sb = new StringBuffer();
		boolean found;
		for (int i=0; i < s.length(); i++) {
			found = false;
			for (int j=0; j < chars.length(); j++) {
				if (s.charAt(i) == chars.charAt(j)) {
					if (sb.length() > 0) {
						rv.add(sb.toString());
					} else if (empty) {
						rv.add("");
					}
					sb.setLength(0);	// reset buffer
					found = true;
					break;
				}
			}
			if (!found) sb.append(s.charAt(i));
		}
		if (sb.length() > 0) rv.add(sb.toString());
		return rv;
	}
	


	
}
