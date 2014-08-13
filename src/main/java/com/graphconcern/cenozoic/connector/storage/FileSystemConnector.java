package com.graphconcern.cenozoic.connector.storage;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.bouncycastle.crypto.digests.SHA256Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.graphconcern.cenozoic.connector.exception.ConnectorException;
import com.graphconcern.cenozoic.connector.model.Connector;
import com.graphconcern.cenozoic.support.config.SystemConfig;
import com.graphconcern.cenozoic.support.util.Utility;

/*
 * This is a reference implementation of the CIFS connector plug-in
 * 
 * It assumes a simple mount-point to a customer supplied CIFS system.
 * A 3-level hierarchical file folder system is used to distribute the files.
 * 
 * e.g. /mountpoint/aaa/bbb/ccc/filename
 * 
 * External URL for the virtual storage system is file://filename
 * where filename = sha256hex+"-"+fileLen
 */
@Component
public class FileSystemConnector implements Connector {
	private static final Logger log = LoggerFactory.getLogger(FileSystemConnector.class);
	
	private static Utility util = new Utility();
	
	private static String protocol, inboxPath;
	private static File inbox;
	
	private String instanceId;
	private OutputStream outStream;
	private InputStream inStream;

	@Override
	public void init(SystemConfig config, String instanceId) {
		this.instanceId = instanceId;
		
		if (protocol == null) {
			protocol = config.getString("storageConnector.protocol");
			String path = config.getString("storageConnector.repository");
			inbox = new File(path);
			if (!inbox.exists()) inbox.mkdirs();
			inboxPath = inbox.getPath();
		}
		log.info(instanceId+" loaded, protocol="+protocol+", inbox="+inbox.getPath());
	}
	
	@Override
	public void setInstance(String instanceId) {
		this.instanceId = instanceId;
	}	
	
	@Override
	public void shutdown() {
		if (outStream != null)
			try {
				outStream.close();
			} catch (IOException e) {}
		if (inStream != null)
			try {
				inStream.close();
			} catch (IOException e) {}
		log.info(instanceId+" stopped");
	}

	@Override
	public boolean exists(String url, String fileType) throws ConnectorException {
		if (url == null) return false;
		if (!url.startsWith(protocol)) return false;
		
		return (new File(getPath(url), fileType)).exists();
	}
	
	@Override
	public Map<String, Object> getMetadata(String url) throws ConnectorException {
		if (!url.startsWith(protocol))
			throw new ConnectorException(this.getClass().getName(), "Protocol should be "+protocol);
		
		if (!validUrl(url)) 
			throw new ConnectorException(this.getClass().getName(), "Invalid URL");
		
		int slash = url.indexOf('-');
		String sha256 = url.substring(protocol.length(), slash);
		long fileLen = util.str2long(url.substring(slash+1));
		
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("sha256", sha256);
		result.put("fileLen", fileLen);
		return result;
	}

	@Override
	public OutputStream getOutputStream(String url, String fileType) throws ConnectorException {
		if (!url.startsWith(protocol))
			throw new ConnectorException(this.getClass().getName(), "Protocol should be "+protocol);
		
		if (!validUrl(url)) 
			throw new ConnectorException(this.getClass().getName(), "Invalid URL");
		
		if (fileType == null) 
			throw new ConnectorException(this.getClass().getName(), "file type should not be NULL");
		
		if (fileType.equals("original") || fileType.equals("preview")) {
			int slash = url.indexOf('-');
			String sha256 = url.substring(protocol.length(), slash);
			long fileLen = util.str2long(url.substring(slash+1));
			
			try {
				File parent = getPath(url);
				if (!parent.exists()) parent.mkdirs();
				
				outStream = new ConnectorOutputStream(new File(parent, fileType), sha256, fileLen, fileType);
				return outStream;
				
			} catch (FileNotFoundException e) {
				throw new ConnectorException(this.getClass().getName(), "Unable to create output stream");
			}
		} else {
			throw new ConnectorException(this.getClass().getName(), "Invalid filetype. Must be original, preview-n or thumbnail-n");
		}

	}
	
	@Override
	public long getFileLength(String url, String fileType) throws ConnectorException {
		File parent = getPath(url);
		File file = new File(parent, fileType);
		if (file.exists()) {
			return file.length();
		} else {
			throw new ConnectorException(this.getClass().getName(), "File not found");
		}
	}

	@Override
	public InputStream getInputStream(String url, String fileType) throws ConnectorException {
		if (this.exists(url, fileType)) {
			try {
				File file = new File(getPath(url), fileType);
				inStream = new BufferedInputStream(new FileInputStream(file));
				return inStream;
				
			} catch (IOException e) {
				e.printStackTrace();
				throw new ConnectorException(this.getClass().getName(), e.getMessage());
			}
		} else {
			return null;
		}
	}

	@Override
	public boolean delete(String url) throws ConnectorException {
		File parent = getPath(url);
		if (!parent.exists()) return false;
		/*
		 * Delete all files under the parent path
		 */
		File[] files = parent.listFiles();
		for (File f: files) {
			f.delete();
		}
		/*
		 * Since all files under the parent directory have been deleted, 
		 * set the filename as "null" so the deleteFile() method can remove empty parent directories.
		 */
		deleteFile(new File(parent, "null"));
		return true;
	}
	
	@Override
	public String getUrl(String sha256, long fileLen) {
		/*
		 * Translate file checksum and length into an external URL
		 */
		return protocol + sha256+"-"+fileLen;
	}
	/*
	 * Internal file path - implementation specific
	 */
	private File getPath(String url) {
		String v = url.substring(protocol.length());		
		return new File(inbox, v.substring(0, 3)+"/"+v.substring(3, 6)+"/"+v.substring(6, 9)+"/"+v);
	}
	
	private boolean validUrl(String url) {
		if (url.length() < protocol.length()) return false;
		
		String v = url.substring(protocol.length());
		if (v.startsWith("-") || v.endsWith("-")) return false;
		
		int slash = v.indexOf('-');
		if (slash == -1) return false;
		
		String sha256 = v.substring(0, slash);
		String fileLen = v.substring(slash+1);
		
		return util.isHex(sha256) && util.isNumFelix(fileLen);		
	}
	/*
	 * Delete up to 3 levels of empty parents
	 */
	private void deleteFile(File file) {
		File parent = file.getParentFile();
		if (file.exists()) file.delete();
		clearEmptyFolder(parent, 3);
	}
	
	private boolean clearEmptyFolder(File dir, int level) {
		/*
		 * Guarantee not to go beyond the top inbox level
		 */
		if (inboxPath.equals(dir.getPath())) return false;
		
		log.info("checking directory "+dir.getPath()+" at level "+level);
		
		if (!dir.exists()) return false;
		if (!dir.isDirectory()) return false;
		
		int n = 0;
		File[] files = dir.listFiles();
		for (File f: files) {
			log.info("scanning "+f.getPath());
			if (f.getName().startsWith(".")) {
				f.delete();
			} else {
				n++;
			}
		}
		log.info("Number of files in directory = "+n+" "+dir.getPath());
		/*
		 * Stops when number of levels has reached or when the directory is not empty
		 */
		if (n == 0) {
			dir.delete();
			return level > 0 ? clearEmptyFolder(dir.getParentFile(), level - 1) : true;
		} else {
			return false;
		}
	}
	/*
	 * Implementation specific output stream
	 * 
	 * This streams the file to a temporary location and computes SHA256.
	 * Throws ConnectionException if checksums mismatch.
	 */
	private class ConnectorOutputStream extends FileOutputStream {
		
		private SHA256Digest hash;
		private String sha256;
		private long fileLen;
		private File tempFile, targetFile;
		private boolean fileOpened = false;
		
		public ConnectorOutputStream(File file, String sha256, long fileLen, String fileType) throws FileNotFoundException {
			super(new File(file.getPath()+".tmp"));
			targetFile = file;
			tempFile = new File(file.getPath()+".tmp");
			this.sha256 = sha256;
			this.fileLen = fileLen;
			fileOpened = true;
			hash = fileType.equals("original")? new SHA256Digest() : null;
		}
		
		@Override
		public void write(int b) throws IOException {
			super.write(b);
			if (hash != null) hash.update((byte) b);
		}
		
		@Override
		public void write(byte[] b) throws IOException {
			super.write(b);
			if (hash != null) hash.update(b, 0, b.length);
		}
		
		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			super.write(b, off, len);
			if (hash != null) hash.update(b, off, len);
		}
		
		@Override
		public void close() throws IOException {
			/*
			 * Guarantee that the output file is closed only once
			 */
			if (fileOpened) {
				fileOpened = false;
				super.flush();
				super.close();
				
				if (hash != null) {
					byte[] shaDigest = new byte[hash.getDigestSize()];
					hash.doFinal(shaDigest, 0);
					
					String checksum = util.bytes2hex(shaDigest);			
					if (!checksum.equals(sha256) || tempFile.length() != fileLen) {
						log.warn("Unable to save file, expected "+sha256+", "+fileLen+", actual "+checksum+", "+tempFile.length());
						deleteFile(tempFile);
						throw new ConnectorException(this.getClass().getName(), "Invalid checksum");
					}
				}
				if (targetFile.exists()) {
					log.info("Removing old file "+targetFile.getName());
					targetFile.delete();
				}
				/*
				 * Rename tempfile to target
				 */
				tempFile.renameTo(targetFile);
			}

		}

	}
	

}
