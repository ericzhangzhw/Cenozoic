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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.ServletContext;

import org.eclipse.jetty.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.WebApplicationContext;
import org.yaml.snakeyaml.Yaml;

import com.graphconcern.cenozoic.support.util.MimeLoader;
import com.graphconcern.cenozoic.support.util.Utility;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;


public class SystemConfig implements SystemConfigBase {
	private static final Logger log = LoggerFactory.getLogger(SystemConfig.class);
	
    private static Utility util = new Utility();
	private boolean logConfigured = false;
    private static MimeLoader mimeLoader;
    
	private File confDir, containerHome;
    private Map<String, Object> configMap = new HashMap<String, Object>();
    private static ConcurrentHashMap<String, String> errorMap = new ConcurrentHashMap<String, String>();
    private static ConcurrentHashMap<String, String[]> accessMap = new ConcurrentHashMap<String, String[]>(); 
    private static List<String> locations = new ArrayList<String>();
    
	private String hostname = "localhost";
	private String intranetName = hostname;
	private CloseableHttpClient httpclient;
	private HttpClient asyncHttpClient;
	private ExecutorService threadPoolExecutor;
	private String webapp;
	
	private static boolean ready = false;
	private static long appTime = System.currentTimeMillis();
	private static long dependencyTime = -1;
	private static DependencyResolver resolver = null;
	private static boolean dependencyDone = true;
	private static SystemConfig confInstance = null;
	
	private static WebApplicationContext webCtx;
	
	public SystemConfig(WebApplicationContext webCtx) {
		SystemConfig.confInstance = this;
		this.webCtx = webCtx;
		/*
		 * Get servlet context from web application context
		 */
		ServletContext ctx = webCtx.getServletContext();
		webapp = ctx.getContextPath();
		if (webapp.equals("/")) webapp = "";
		/*
		 * Transform webapp name as appId
		 */
    	String appId = webapp.length() == 0? "ROOT" : webapp.substring(1);
		/*
		 * Resolve the WEB-INF/conf directory
		 */
		File root = new File(ctx.getRealPath("/"));
		confDir = new File(root, "WEB-INF/config");
		if (!confDir.exists()) {
			log.error("Missing WEB-INF/config folder");
			return;
		}
		/*
		 * Resolve container home
		 */
		String catalinaBase = System.getProperty("catalina.base");
		String catalinaHome = catalinaBase == null ? System.getProperty("catalina.home") : System.getProperty("catalina.base");
		File catalina = catalinaHome == null ? null : new File(catalinaHome);
 		containerHome = catalina == null ? null : (catalina.exists() ? new File(catalina, "conf/ci"): null) ;	
		String ciBase = System.getProperty("ci.base");
		if (ciBase != null) {
			File base = new File(ciBase);
			if (base.exists()) {
				containerHome = base;
			} else {
				log.error("Config home "+ciBase+" not found");
			}
		}
		log.info("Container home located at "+containerHome.getPath());
		if (confDir == null) {
			log.error("Unable to start because config directory is not specified");
			return;
		}
		if (!confDir.exists()) {
			log.error("Unable to start because config "+confDir.getPath()+" not found");
			return;
		}
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			hostname = "localhost";
		}
		intranetName = util.getMyIntranetIpName();
		/*
		 * Configure logging using log4j.xml and logback.xml in WEB-INF/config
		 */
        configureLoggers(); 
        /*
         * Load MimeTypes
         */
        File mimeTypes = selectFile("mime.types");
        if (!mimeTypes.exists()) {
			log.error("Unable to start because config mime.types not found");
			return;
        }
        mimeLoader = new MimeLoader(mimeTypes, util);
        /*
         * Load YAML
         */
        File globalConf = selectFile(appId+"_config_global.yaml");
        if (!globalConf.exists()) {
        	// try again with default name
        	globalConf = selectFile("config_global.yaml");
        	if (!globalConf.exists()) {
    			log.error("Unable to start because "+globalConf.getPath()+" not found");
    			return;
        	}
        }
        File serverConf = selectFile(appId+"_config_server.yaml");
        if (!serverConf.exists()) {
        	// try again with default name
        	serverConf = selectFile("config_server.yaml");
        	if (!serverConf.exists()) {
    			log.error("Unable to start because "+serverConf.getPath()+" not found");
    			return;
        	} 
        }
        ByteArrayInputStream global = new ByteArrayInputStream(util.getUTF(util.file2str(globalConf)));
        ByteArrayInputStream server = new ByteArrayInputStream(util.getUTF(util.file2str(serverConf)));
        
        Yaml yaml = new Yaml();
		boolean invalid = false;
        Object yGlobal = yaml.load(global);
        if (!(yGlobal instanceof Map)) {
			log.error("Invalid YAML format for config_global.yaml");
			invalid = true;
        }
        Object yServer = yaml.load(server);
        if (!(yServer instanceof Map)) {
			log.error("Invalid YAML format for config_server.yaml");
			invalid = true;
        }
        
        if (invalid) return;
        /*
         * Override global values with server specific values
         */
 		Map<String, Object> mapGlobal = (Map<String, Object>) yGlobal;
 		Map<String, Object> mapServer = (Map<String, Object>) yServer;
        
        if (!mapServer.isEmpty()) {
        	Iterator<Entry<String, Object>> outer = mapServer.entrySet().iterator();
        	while (outer.hasNext()) {
                Map.Entry<String, Object> kv = (Map.Entry<String, Object>) outer.next();
                String key = kv.getKey();
                Object value = kv.getValue();
                if (mapGlobal.containsKey(key)) {
                	Object globalValue = mapGlobal.get(key);
                	if (globalValue instanceof Map && value instanceof Map) {
                		Iterator<Entry<String, Object>> inner = ((Map) value).entrySet().iterator();
                		Map<String, Object> innerGlobal = (Map<String, Object>) globalValue;
                		while (inner.hasNext()) {
                			Map.Entry<String, Object> ikv = (Map.Entry<String, Object>) inner.next();
                            String ikey = ikv.getKey();
                            Object ivalue = ikv.getValue();
                    		String compositeKey = key+"."+ikey;
                            if (innerGlobal.containsKey(ikey)) {
                            	Object innerGlobalValue = innerGlobal.get(ikey);
                        		if (innerGlobalValue == null || !innerGlobalValue.equals(ivalue)) {
                            		innerGlobal.put(ikey, ivalue);
                            		log.info("Override with "+compositeKey+"="+(compositeKey.contains("password") ? "******" : ivalue));
                        		}
                            } else {
                        		innerGlobal.put(ikey, ivalue);
                    			log.info("Override with "+compositeKey+"="+(compositeKey.contains("password") ? "******" : ivalue));
                            }
                		}
                		
                	} else {
                		if (globalValue == null || !globalValue.equals(value)) {
                    		mapGlobal.put(key, value);
                			log.info("Override with "+key+"="+(key.contains("password") ? "******" : value));
                		}
                	}
                } else {
            		mapGlobal.put(key, value);
        			log.info("Override with "+key+"="+(key.contains("password") ? "******" : value));
                }
            }
        }
        configMap = mapGlobal;
    	/*
    	 * Create HTTP Client connection pool
    	 */
        int poolCount = getInt("httpClientPool");
        if (poolCount < 30) poolCount = 30;
    	PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
    	cm.setMaxTotal(poolCount);
    	httpclient = HttpClients.custom().setConnectionManager(cm).build();
    	/*
    	 * Initialize async HTTP client if necessary
    	 */
    	if (getBoolean("asyncHttpClient.enabled")) {
    		int maxConnection = getInt("asyncHttpClient.maxConnection");
    		if (maxConnection < 10 || maxConnection > 10000) {
    			log.warn("asyncHttpClient.maxConnection not in range [10, 10000]. Set to default value of 10");
    			maxConnection = 10;
    		}
    		int timeout = getInt("asyncHttpClient.timeout");
    		if (timeout < 10 || timeout > 180) {
    			log.warn("asyncHttpClient.timeout not in range [10, 180]. Set to default value of 10");
    			timeout = 10;
    		}
    		asyncHttpClient = new HttpClient();
    		asyncHttpClient.setMaxConnectionsPerDestination(maxConnection);
    		asyncHttpClient.setConnectTimeout(timeout * 1000);
    		asyncHttpClient.setIdleTimeout(timeout * 1000);
    		threadPoolExecutor = Executors.newCachedThreadPool();
    		asyncHttpClient.setExecutor(threadPoolExecutor);
    		try {
    			asyncHttpClient.start();
			} catch (Exception e) {
				log.error("Unable to create asynchronous HttpClient");
				e.printStackTrace();
			}
    	} 
    	/*
    	 * Save appId
    	 */
    	configMap.put("id", appId);
    	if (locations.size() > 1) Collections.sort(locations);
    	/*
    	 * Is "Consul" local agent running?
    	 */
    	if (util.portReady("127.0.0.1", 8500, 10000)) {
    		log.info("consul is running");
    		
    	}
        /*
         * Start dependency check if needed
         */
        if (configMap.containsKey("dependency") && getUrlList("dependency") != null) setDependencyReady(false);
	}
	/*
	 * Clean up resources
	 */
	public void shutdown() {
		if (httpclient != null)
			try {
				httpclient.close();
				log.info("HttpClient stopped");
			} catch (IOException e) {}
		
		if (threadPoolExecutor != null) {
			threadPoolExecutor.shutdownNow();
			log.info("AsyncHttpClient thread pool stopped");
		}
		if (asyncHttpClient != null)
			try {
				asyncHttpClient.stop();
				log.info("AsyncHttpClient stopped");
			} catch (Exception e) {}
		
	}
	
	public WebApplicationContext getAppCtx() {
		return webCtx;
	}
	
	private File selectFile(String filename) {
		File file = null;
		if (containerHome != null && containerHome.exists()) {
			File overrideFile = new File(containerHome, filename);
			if (overrideFile.exists()) file = overrideFile;
		}
		if (file == null) {
			File developerFile = new File(confDir, System.getProperty("user.name")+"-"+filename);
			file = developerFile.exists() ? developerFile : new File(confDir, filename);
		}		
		if (file != null && file.exists()) {
			String path = file.getPath();
			log.info("Selected config file - "+path);
			if (!locations.contains(path)) locations.add(path);
		} else {
			log.error("Config file not found - "+file.getPath());
		}
		return file;
	}
	
	private void upload2Consul() {
		
	}
	
	@Override
	public boolean exists(String key) {
		Object value = util.getMapElement(key, configMap);
		return value != null;
	}
	
	private boolean isEnvVariable(String value) {
		if (value.startsWith("${") && value.endsWith("}")) {
			String key = value.substring(2, value.length() -1 );
			return key.length() > 0 ? true : false;
		} else {
			return false;
		}
	}
	
	private String getStringFromEnv(String key, String envKey) {
		String value = System.getProperty(envKey.substring(2, envKey.length()-1));
		if (value == null) {
			addIfNotExist(key, envKey+" does not exist");
			return null;
		} else {
			return value;
		}
	}
	
	private Boolean getBooleanFromEnv(String key, String envKey) {
		String value = System.getProperty(envKey.substring(2, envKey.length()-1));
		if (value == null) {
			addIfNotExist(key, envKey+" does not exist");
			return null;
		} else {
			return value.equalsIgnoreCase("true") ? Boolean.TRUE : Boolean.FALSE;
		}
	}
	
	private Long getLongFromEnv(String key, String envKey) {
		String value = System.getProperty(envKey.substring(2, envKey.length()-1));
		if (value == null) {
			addIfNotExist(key, envKey+" does not exist");
			return null;
		} else {
			if (!util.isNumFelix(value)) addIfNotExist(key, envKey+" is not numFelix");
			return util.str2long(value);
		}
	}
	
	private Integer getIntFromEnv(String key, String envKey) {
		String value = System.getProperty(envKey.substring(2, envKey.length()-1));
		if (value == null) {
			addIfNotExist(key, envKey+" does not exist");
			return null;
		} else {
			if (!util.isNumFelix(value)) addIfNotExist(key, envKey+" is not numFelix");
			return util.str2int(value);
		}
	}

	@Override
	public String getString(String key) {
		Object value = util.getMapElement(key, configMap);
		if (value == null) {
			addIfNotExist(key, "does not exist");
			return null;
		}
		if (value instanceof String) {
			if (isEnvVariable((String) value)) value = getStringFromEnv(key, (String) value);
			if (value == null) {
				return null;
			} else {
				setLastAccess(key, "String", value);
				return (String) value;
			}
		} else {
			addIfNotExist(key, "not a String");
			return value.toString();
		}
	}

	@Override
	public List<String> getStringList(String key) {
		Object value = util.getMapElement(key, configMap);
		if (value == null) {
			addIfNotExist(key, "does not exist");
			return null;
		}
		if (value instanceof List) {
			/*
			 * Enforce uniform return value
			 */
			List<String> result = new ArrayList<String>();
		
			boolean incomplete = false;
			for (Object v: (List) value) {
				if (v instanceof String) {
					if (isEnvVariable((String) v)) v = getStringFromEnv(key, (String) value);
					if (v != null) {
						result.add((String) v);
					} else {
						incomplete = true;
					}
				} else {
					incomplete = true;
				}
			}
			if (incomplete) {
				addIfNotExist(key, "is incomplete "+result);
			} else {
				setLastAccess(key, "StringList", result);
			}
			return result;
		} else {
			addIfNotExist(key, "not a string list");
			List<String> converted = new ArrayList<String>();
			if (value != null) converted.add(value.toString());
			if (converted.isEmpty()) {
				addIfNotExist(key, "does not exist");
			} else {
				setLastAccess(key, "StringList", value);
			}
			return converted;
		}
	}


	@Override
	public boolean getBoolean(String key) {
		Object value = util.getMapElement(key, configMap);
		if (value == null) {
			addIfNotExist(key, "does not exist");
			return false;
		}
		if (isEnvVariable(value.toString())) value = getBooleanFromEnv(key, value.toString());
		if (value == null) return false;
		
		if (value instanceof Boolean) {
			setLastAccess(key, "Boolean", value);
			return (boolean) value;
		} else {
			addIfNotExist(key, "not a boolean");
			return value.toString().equalsIgnoreCase("true") ? true : false;
		}
	}

	@Override
	public long getLong(String key) {
		Object value = util.getMapElement(key, configMap);
		if (value == null) {
			addIfNotExist(key, "does not exist");
			return -1;
		}
		if (isEnvVariable(value.toString())) value = getLongFromEnv(key, value.toString());
		if (value == null) return -1;
		
		if (value instanceof Long) {
			setLastAccess(key, "Long", value);
			return ((Long) value).longValue();
		} else if (value instanceof Integer) {
			return ((Integer) value).longValue();	
		} else {
			addIfNotExist(key, "not a number");
			return util.str2long(value.toString());
		}
	}

	@Override
	public int getInt(String key) {
		Object value = util.getMapElement(key, configMap);
		if (value == null) {
			addIfNotExist(key, "does not exist");
			return -1;
		}
		if (isEnvVariable(value.toString())) value = getIntFromEnv(key, value.toString());
		if (value == null) return -1;
		
		if (value instanceof Integer) {
			setLastAccess(key, "Integer", value);
			return ((Integer) value).intValue();	
		} else if (value instanceof Long) {
			addIfNotExist(key, "truncated to Integer");
			return ((Long) value).intValue();
		} else {
			addIfNotExist(key, "not a number");
			return util.str2int(value.toString());
		}
	}

	@Override
	public String getUrl(String key) {
		Object value = util.getMapElement(key, configMap);
		if (value == null) {
			addIfNotExist(key, "does not exist");
			return null;
		}
		if (value instanceof String) {
			if (isEnvVariable((String) value)) value = getStringFromEnv(key, (String) value);
			if (value == null) return null;
			
			try {
				String url = (String) value;
				if (url.endsWith("/")) url = url.substring(0, url.length()-1);
				new URL(url);	// test if it is a proper URL
				setLastAccess(key, "URL", url);
				return url;
			} catch (MalformedURLException e) {
				addIfNotExist(key, "not a URL");
				return null;
			}
		} else {
			addIfNotExist(key, "not a URL");
			return null;
		}

	}
	
	public List<String> getUrlList(String key) {
		Object value = util.getMapElement(key, configMap);
		if (value == null) {
			addIfNotExist(key, "does not exist");
			return null;
		}
		List<String> result = new ArrayList<String>();
		if (value instanceof List) {
			/*
			 * Enforce uniform return value
			 */
			boolean incomplete = false;
			for (Object v: (List) value) {
				if (v instanceof String) {
					if (isEnvVariable((String) v)) v = getStringFromEnv(key, (String) v);
					if (v != null) {
						try {
							String url = v instanceof String? (String) v : v.toString();
							if (url.endsWith("/")) url = url.substring(0, url.length()-1);
							new URL(url); // test if it is a proper URL
							result.add(url);
						} catch (MalformedURLException e) {
							incomplete = true; 
						}	
					} else {
						incomplete = true;
					}
				} else {
					incomplete = true;
				}
			}
			if (incomplete) {
				addIfNotExist(key, "is incomplete "+result);
			} else {
				setLastAccess(key, "URL list", result);
			}
		} else if (value instanceof String) {
			if (isEnvVariable((String) value)) value = getStringFromEnv(key, (String) value);
			if (value != null) {
				try {
					String url = (String) value;
					if (url.endsWith("/")) url = url.substring(0, url.length()-1);
					new URL(url); // test if it is a proper URL
					result.add(url);
				} catch (MalformedURLException e) {}
			}
		} else {
			addIfNotExist(key, "not a URL list");
		}
		return result;
	}

	@Override
	public String getHostPort(String key) {
		Object value = util.getMapElement(key, configMap);
		if (value == null) {
			addIfNotExist(key, "does not exist");
			return null;
		}
		if (value instanceof String) {
			if (isEnvVariable((String) value)) value = getStringFromEnv(key, (String) value);
			if (value == null) return null;
			
			String hostPort = (String) value;
			if (validHostPort(hostPort)) {
				setLastAccess(key, "Host:Port", value);
				return hostPort;
			}
		} 
		
		addIfNotExist(key, "not a host:port string");
		return null;
	}

	@Override
	public List<String> getHostPortList(String key) {
		Object value = util.getMapElement(key, configMap);
		if (value == null) {
			addIfNotExist(key, "does not exist");
			return null;
		}
		List<String> result = new ArrayList<String>();
		if (value instanceof List) {
			List source = (List) value;
			for (Object v: source) {
				if (v instanceof String) {
					if (isEnvVariable((String) v)) v = getStringFromEnv(key, (String) v);
					if (v != null) {
						String hostPort = (String) v;
						if (validHostPort(hostPort)) result.add(hostPort);
					}
				}
			}
			if (result.size() == source.size()) {
				setLastAccess(key, "HostPortList", value);
			} else {
				addIfNotExist(key, "incomplete host:port list");
			}
		} else {
			addIfNotExist(key, "not a host:port list");
		}
		return result;
	}

	@Override
	public Map<String, Object> getMap(String key) {
		Object value = util.getMapElement(key, configMap);
		if (value == null) {
			addIfNotExist(key, "does not exist");
			return null;
		}
		Map<String, Object> map = (value instanceof Map) ? (Map<String, Object>) value : null;
		if (map != null) setLastAccess(key, "Map", value);
		return map;
	}

	@Override
	public List<Map<String, Object>> getMapList(String key) {
		Object value = util.getMapElement(key, configMap);
		if (value == null) {
			addIfNotExist(key, "does not exist");
			return null;
		}
		if (value instanceof List) {
			/*
			 * Enforce uniform return value
			 */
			boolean sameType = true;
			for (Object v: (List) value) {
				if (!(v instanceof Map)) {
					sameType = false;
					break;
				}
			}
			if (sameType) {
				setLastAccess(key, "MapList", value);
				return (List<Map<String, Object>>) value;
			}
		}
		addIfNotExist(key, "not a list of key-value pairs");
		return null;
	}
	
	public int getListSize(String key) {
		Object value = util.getMapElement(key, configMap);
		if (value == null) {
			addIfNotExist(key, "does not exist");
			return -1;
		}
		if (value instanceof List) {
			return ((List) value).size();
		} else {
			return -1;
		}
	}

    private void configureLoggers() {
        /*
         * Setup log4j and logback configurations
         * Update $path parameter and save log4j.xml and logback.xml into working directory.
         */
    	String log4jFile = exists("logging.log4j")? getString("logging.log4j") : "log4j.xml";
    	String logbackFile = exists("logging.logback")? getString("logging.logback") : "logback.xml";
    	
    	if (!logConfigured) {
            File logback = selectFile(logbackFile);
        	if (logback != null && logback.exists()) {
            	try {
                	LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
                	JoranConfigurator jc = new JoranConfigurator();
                	jc.setContext(context);
                	context.reset();
    				jc.doConfigure(logback);
    			} catch (JoranException e1) {
    				log.error("Unable to configure using logback.xml");
    			} 
        	}
            File log4j = selectFile(log4jFile);
        	if (log4j != null && log4j.exists()) {
            	DOMConfigurator.configure(log4j.getPath());       	    		
        	}
    	}
    }
    
    public Utility getUtility() {
    	return util;
    }

	
	private void addIfNotExist(String key, String error) {
		if (!errorMap.containsKey(key)) {
			errorMap.put(key, error);
		}
	}	
	
	private boolean validHostPort(String hostPort) {
		if (hostPort == null) return false;
		if (hostPort.contains(":") && !hostPort.startsWith(":") && !hostPort.endsWith(":")) {
			int start = hostPort.indexOf(':');
			int end = hostPort.lastIndexOf(':');
			if (start == end) return util.isNumFelix(hostPort.substring(end+1));
		}
		return false;
	}
	
	public String getMimeFromExt(String ext) {
		return mimeLoader.getMimeFromExt(ext);
	}
	
	public List<String> getExtFromMime(String mime) {
		return mimeLoader.getExtFromMime(mime);
	}

	@Override
	public String getHostname() {
		return hostname;
	}
	
	@Override
	public String getHostId() {
		return getHostname()+"_"+getString("id")+"_"+getIntranetName();
	}

	@Override
	public String getIntranetName() {
		return intranetName;
	}

	@Override
	public File getConfigDir() {
		return confDir;
	}
	
	@Override
	public String getWebApp() {
		return webapp;
	}


	@Override
	public CloseableHttpClient getHttpClient() {
		return httpclient;
	}
	
	@Override
	public HttpClient getAsyncHttpClient() {
		return asyncHttpClient;
	}

	@Override
	public void setReady(boolean ready) {
		appTime = System.currentTimeMillis();
		this.ready = ready;	
	}

	@Override
	public boolean isReady() {
		return ready && dependencyDone;
	}
	
	@Override
	public long getAppTime() {
		return appTime;
	}
	
	@Override
	public long getDependencyTime() {
		return dependencyTime;
	}
	
	@Override
	public boolean isDependencyResolved() {
		return dependencyDone;
	}
	
	public void setDependencyReady(boolean dependencyDone) {
		if (this.dependencyDone != dependencyDone) {	
			if (!dependencyDone && resolver == null) {
				this.dependencyDone = dependencyDone;
				dependencyTime = System.currentTimeMillis();
				resolver = new DependencyResolver(getUrlList("dependency"));
				resolver.start();
			}
			if (dependencyDone && resolver != null) {
				this.dependencyDone = dependencyDone;
				dependencyTime = System.currentTimeMillis();
				resolver = null;
			}
		}
		

	}
	
	public static SystemConfig getInstance() {
		return confInstance;
	}
	
	public Map<String, Object> getConfigMap() {
		return configMap;
	}
	
	public ConcurrentHashMap<String, String> getErrorMap() {
		return errorMap;
	}
	
	public ConcurrentHashMap<String, String[]> getAccessMap() {
		return accessMap;
	}
	
	public List<String> getConfLocations() {
		return locations;
	}
	
	private void setLastAccess(String key, String type, Object value) {
		String[] result = new String[3];
		long now = System.currentTimeMillis();
		String time = util.long2rfc3339(now, false).replace('T', ' ');
		result[0] = time.substring(0, time.lastIndexOf('-'));
		result[1] = type;
		result[2] = key.contains("password") ? "******" : (value instanceof String? (String) value : value.toString());
		accessMap.put(key, result);
	}
	
	private class DependencyResolver extends Thread {
		private Logger log = LoggerFactory.getLogger(DependencyResolver.class);
		
		private List<String> pendingUrls;
		private Utility util = SystemConfig.getInstance().getUtility();
		private CloseableHttpClient httpclient = SystemConfig.getInstance().getHttpClient();
		
		public DependencyResolver(List<String> urls) {
			this.pendingUrls = urls;
		}
		
		@Override
		public void run() {
			while (pendingUrls.size() > 0) {
				List<String> completedUrls = new ArrayList<String>();
				for (String url: pendingUrls) {
					String result = util.getHttpPage(url, httpclient);
					if ("true".equals(result)) {
						completedUrls.add(url);
						log.info("Dependency resolved - "+url);
					}
				}
				if (completedUrls.size() == pendingUrls.size()) break;
				List<String> remaining = new ArrayList<String>();
				for (String url: pendingUrls) {
					if (!completedUrls.contains(url)) remaining.add(url);
				}
				if (remaining.isEmpty()) break;
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {}
				log.warn("Retry dependency "+remaining);
				pendingUrls = remaining;
			}
			SystemConfig.getInstance().setDependencyReady(true);
		}
	}

}
