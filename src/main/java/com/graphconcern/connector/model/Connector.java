package com.graphconcern.connector.model;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import com.graphconcern.connector.exception.ConnectorException;
import com.graphconcern.support.config.SystemConfig;

public interface Connector {
	
	public void init(SystemConfig config, String instanceId);
	public void setInstance(String instanceId);
	public void shutdown();
	
	public InputStream getInputStream(String url, String fileType) throws ConnectorException;

	public OutputStream getOutputStream(String url, String fileType) throws ConnectorException;
	
	public long getFileLength(String url, String fileType) throws ConnectorException;

	public boolean delete(String url) throws ConnectorException;

	public boolean exists(String url, String fileType) throws ConnectorException;
	
	public String getUrl(String sha256, long fileLen);
	
	public Map<String, Object> getMetadata(String url) throws ConnectorException;
	
}
