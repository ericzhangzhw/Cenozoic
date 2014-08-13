package com.graphconcern.cenozoic.search.engine;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.BufferedIndexOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import com.graphconcern.cenozoic.support.util.MqUtil;
import com.datastax.driver.core.Session;

public class CqlDirectory extends BaseDirectory {
	
	private CqlFileDao fileDao;
	private String path;
	private CqlLockFactory lockFactory;
	
	public CqlDirectory(MqUtil mqUtil, Session session, String path, String id) {
		this.path = path;
		this.fileDao = new CqlFileDao(session, path);
		this.lockFactory = new CqlLockFactory(mqUtil, path, id);
	}

	@Override
	public void clearLock(String lockName) throws IOException {
		this.lockFactory.clearLock(lockName);
	}

	@Override 
	public void close() throws IOException {
		this.lockFactory.close();
	}

	@Override
	public IndexOutput createOutput(String fileName, IOContext context)
			throws IOException {
		return new CqlIndexOutput(fileDao, fileName);
	}

	@Override
	public void deleteFile(String fileName) throws IOException {
		fileDao.deleteFile(fileName);
	}

	@Override
	public boolean fileExists(String fileName) throws IOException {
		return fileDao.exists(fileName);
	}

	@Override
	public long fileLength(String fileName) throws IOException {
		return fileDao.length(fileName);
	}

	@Override
	public LockFactory getLockFactory() {
		return lockFactory;
	}
	
	public boolean isEmpty() {
		return fileDao.isEmpty(path);
	}

	@Override
	public String[] listAll() {
		return fileDao.listFiles(path);
	}

	@Override
	public Lock makeLock(String lockName) {
		return lockFactory.makeLock(lockName);
	}

	@Override
	public IndexInput openInput(String fileName, IOContext context) {
		return new CqlIndexInput(fileDao, "CqlIndexInput(path=\"" + path+"/"+fileName + "\")", fileName, context);
	}

	@Override
	public void setLockFactory(LockFactory lockFactory) throws IOException {
		// No-OP
	}

	@Override
	public void sync(Collection<String> files) throws IOException {
		// No-OP
	}

}

class CqlIndexInput extends BufferedIndexInput {
	
	private String fileName;
	private CqlFileDao fileDao;
	private long filePointer = 0;
	private long fileLength = -1;
	
	public CqlIndexInput(CqlFileDao fileDao, String resourceDesc, String fileName, IOContext context) {
		super(resourceDesc, context);
		this.fileName = fileName;
		this.fileDao = fileDao;
	}

	@Override
	protected void readInternal(byte[] b, int offset, int length)
			throws IOException {
		
		if (b.length < offset + length) {
			throw new IOException("read buffer too small. Need "+(offset + length)+", given "+b.length +": "+ this);
		}
		try {
			byte[] data = fileDao.getBytes(fileName, filePointer, length);
			System.arraycopy(data, 0, b, offset, length);
			filePointer += length;
		} catch (EOFException e) {
			throw new EOFException("read past EOF: " + this);
		}
	}

	@Override
	protected void seekInternal(long pos) throws IOException {
		filePointer = pos;		
	}

	@Override
	public void close() throws IOException {
		// No-op
	}

	@Override
	public long length() {
		if (fileLength == -1) fileLength = fileDao.length(fileName);
		return fileLength;
	}
	
}

class CqlIndexOutput extends BufferedIndexOutput {
	
	private String fileName;
	private CqlFileDao fileDao;
	private long fileLength = 0;
	
	public CqlIndexOutput(CqlFileDao fileDao, String fileName) {
		this.fileName = fileName;
		this.fileDao = fileDao;
	}

	@Override
	protected void flushBuffer(byte[] b, int offset, int len)
			throws IOException {
		if (len > 0) {
			fileDao.saveBlock(fileName, fileLength, b, offset, len);
			fileLength += len;
		}
	}

	@Override
	public long length() throws IOException {
		return fileLength;
	}
	
	@Override
	public void close() throws IOException {
	    flush();
	    /*
	     * Update creation date and finalize file length meta-data
	     */
	    fileDao.finalizeFile(fileName);
	}
	
}
