/** 
/*Graph Concern Inc Confidential Information.
/* TM and (c) 2013 Graph Concern Inc.  All Rights Reserved.
/* Reproduction in whole or in part without prior written permission of a
/* duly authorized representative is prohibited.
 * 
 * Author: Felix Chiu
 * Date: 3/20/2014
 */
package com.graphconcern.cenozoic.support.config;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.http.impl.client.CloseableHttpClient;
import org.eclipse.jetty.client.HttpClient;
import org.springframework.web.context.WebApplicationContext;

public interface SystemConfigBase {
	
	public WebApplicationContext getAppCtx();
	
	public String getHostname();
	
	public String getHostId();
	
	public String getIntranetName();
	
	public File getConfigDir();
	
	public String getWebApp();
	
	public boolean exists(String key);
	
	public String getString(String key);
	public List<String> getStringList(String key);
	
	public boolean getBoolean(String key);
	
	public long getLong(String key);
	
	public int getInt(String key);
	
	public String getUrl(String key);
	public List<String> getUrlList(String key);
	
	public String getHostPort(String key);
	public List<String> getHostPortList(String key);
	
	public Map<String, Object> getMap(String key);
	public List<Map<String, Object>> getMapList(String key);
	
	public int getListSize(String key);
	
	public CloseableHttpClient getHttpClient();
	
	public HttpClient getAsyncHttpClient();
	
	public void setReady(boolean ready);
	
	public boolean isReady();
	
	public long getAppTime();
	
	public long getDependencyTime();
	
	public boolean isDependencyResolved();
	
	public void shutdown();

}
