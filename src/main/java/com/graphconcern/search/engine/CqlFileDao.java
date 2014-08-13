package com.graphconcern.search.engine;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CqlFileDao {
	private static final Logger log = LoggerFactory.getLogger(CqlFileDao.class);

	private static String SAVE_BLOCK = "INSERT INTO files (path, name, beginptr, endptr, data, length) VALUES (?, ?, ?, ?, ?, ?)";
	private static String GET_BLOCK = "SELECT * FROM files WHERE path = ? AND name = ? AND endptr >= ? LIMIT 1";
	private static String DELETE_FILE = "DELETE FROM files WHERE path = ? AND name = ?";
	private static String COMPUTE_LENGTH = "SELECT length FROM files WHERE path = ? AND name = ?";

	private static String ADD_FILE = "INSERT INTO dirs (path, name, created, length) VALUES (?, ?, ?, ?)";
	private static String REMOVE_FILE = "DELETE FROM dirs WHERE path = ? AND name = ?";
	private static String LIST_FILE = "SELECT name FROM dirs WHERE path = ?";
	private static String GET_FILELEN = "SELECT length FROM dirs WHERE path = ? AND name = ?";

	private static PreparedStatement prepareSaveBlock, prepareGetBlock, prepareDeleteFile, prepareComputeLength, prepareFileExist;
	private static PreparedStatement prepareAddFile, prepareRemoveFile, prepareListFile, prepareGetFileLen;

	private boolean started = false;
	private Session session;
	private String filePath;

	public CqlFileDao(Session session, String filePath) {
		this.session = session;
		this.filePath = filePath;
		this.init(session);
	}
	
	private synchronized void initOnce (Session session) {
		if (prepareSaveBlock == null) prepareSaveBlock = session.prepare(SAVE_BLOCK);
		if (prepareGetBlock == null) prepareGetBlock = session.prepare(GET_BLOCK);
		if (prepareDeleteFile == null) prepareDeleteFile = session.prepare(DELETE_FILE);
		if (prepareComputeLength == null) prepareComputeLength = session.prepare(COMPUTE_LENGTH);
		if (prepareFileExist == null) prepareFileExist = session.prepare(COMPUTE_LENGTH+" LIMIT 1");

		if (prepareAddFile == null) prepareAddFile = session.prepare(ADD_FILE);
		if (prepareRemoveFile == null) prepareRemoveFile = session.prepare(REMOVE_FILE);
		if (prepareListFile == null) prepareListFile = session.prepare(LIST_FILE);
		if (prepareGetFileLen == null) prepareGetFileLen = session.prepare(GET_FILELEN);
	}
	
	private void init(Session session) {
		if (!started) {
			started = true;
			this.initOnce(session);
		}
	}
	
	public boolean saveBlock(String fileName, long filePtr, byte[] data, int offset, int len) {
		try {
			if (!exists(fileName)) {
				/*
				 * When a file is created, the initial length is undefined.
				 * Therefore it is set to -1.
				 */
				session.execute(prepareAddFile.bind(filePath, fileName, new Date(System.currentTimeMillis()), -1L));
			}
			session.execute(prepareSaveBlock.bind(filePath, fileName, filePtr, (filePtr + len - 1), ByteBuffer.wrap(data, offset, len), len));
			return true;
		} catch (Exception e) {
			log.error("Unable to save file "+filePath+"/"+fileName+" - "+e.getMessage());
			return false;
		}
	}

	public boolean exists(String fileName) {
		ResultSet result = session.execute(prepareFileExist.bind(filePath, fileName));
		Iterator<Row> records = result.iterator();
		return records.hasNext();
	}

	public byte[] getBytes(String fileName, long pointer, int length) throws EOFException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		int size = length;
		long filePointer = pointer;
		while (size > 0) {
			Row row = getBlock(fileName, filePointer);
			if (row == null) throw new EOFException();

			int rowFileLen = row.getInt("length");
			long rowBeginPtr = row.getLong("beginptr");

			int offset = filePointer > rowBeginPtr ? (int) (filePointer - rowBeginPtr) : 0;			
			int remaining = rowFileLen - offset;
			byte[] data = getBytes(row);

			if (offset < 0 || remaining < 0) throw new EOFException();			
			if (size <= remaining) {
				out.write(data, offset, size);
				break;
			} else {
				out.write(data, offset, remaining);
				size -= remaining;
				filePointer += remaining;
			}
		}
		return out.toByteArray();
	}

	private Row getBlock(String fileName, long endPtr) {
		ResultSet result = session.execute(prepareGetBlock.bind(filePath, fileName, endPtr));
		Iterator<Row> records = result.iterator();
		return records.hasNext() ? records.next() : null;
	}

	private byte[] getBytes(Row row) {
		if (row == null) return null;
		ByteBuffer bb = row.getBytes("data");
		byte[] data = new byte[bb.remaining()];
		bb.get(data);
		return data;
	}

	public long length(String fileName) {
		ResultSet result = session.execute(prepareGetFileLen.bind(filePath, fileName));
		Iterator<Row> records = result.iterator();
		if (records.hasNext()) {
			Row row = records.next();
			long len = row.getLong("length");
			if (len == -1) {
				return computeLength(fileName);
			} else {
				return len;
			}
		}
		return 0;
	}
	
	public boolean isEmpty(String dir) {
		ResultSet result = session.execute(prepareListFile.bind(dir));
		Iterator<Row> records = result.iterator();
		return !records.hasNext();
	}

	public String[] listFiles(String dir) {
		List<String> list = new ArrayList<String>();
		ResultSet result = session.execute(prepareListFile.bind(dir));
		Iterator<Row> records = result.iterator();
		while (records.hasNext()) {
			Row row = records.next();
			list.add(row.getString("name"));
		}
		String[] files = new String[list.size()];
		for (int i=0; i < files.length; i++) {
			files[i] = list.get(i);
		}
		return files;
	}

	public void deleteFile(String fileName) throws IOException {
		try {
			if (exists(fileName)) {
				session.execute(prepareDeleteFile.bind(filePath, fileName));
				session.execute(prepareRemoveFile.bind(filePath, fileName));
			}
		} catch (Exception e) {
			throw new IOException("Unable to delete file "+fileName+" - "+e.getMessage());
		}
	}

	private long computeLength(String fileName) {
		long len = 0;
		ResultSet result = session.execute(prepareComputeLength.bind(filePath, fileName));
		Iterator<Row> records = result.iterator();
		while (records.hasNext()) {
			Row row = records.next();
			len += row.getInt("length");
		}
		return len;
	}	

	public void finalizeFile(String fileName) {
		ResultSet result = session.execute(prepareGetFileLen.bind(filePath, fileName));
		Iterator<Row> records = result.iterator();
		if (records.hasNext()) {
			Row row = records.next();
			long fileLen = row.getLong("length");
			long computedLen = computeLength(fileName);
			if (fileLen != computedLen) {
				session.execute(prepareAddFile.bind(filePath, fileName, new Date(System.currentTimeMillis()), computedLen));
			}
		}
	}

}
