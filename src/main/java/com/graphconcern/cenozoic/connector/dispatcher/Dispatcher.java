package com.graphconcern.cenozoic.connector.dispatcher;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.websocket.ClientEndpoint;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;

import org.bouncycastle.asn1.pkcs.RSAPrivateKeyStructure;
import org.bouncycastle.asn1.x509.RSAPublicKeyStructure;
import org.bouncycastle.util.encoders.Base64;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.graphconcern.cenozoic.connector.exception.ConnectorException;
import com.graphconcern.cenozoic.connector.model.Connector;
import com.graphconcern.cenozoic.support.config.SystemConfig;
import com.graphconcern.cenozoic.support.util.Constants;
import com.graphconcern.cenozoic.support.util.CryptoAPI;
import com.graphconcern.cenozoic.support.util.NvPair;
import com.graphconcern.cenozoic.support.util.Utility;

@ClientEndpoint
public class Dispatcher {
	private static Logger log = LoggerFactory.getLogger(Dispatcher.class);

	private static AtomicInteger onceOff = new AtomicInteger();
	private static Utility util = new Utility();
	private static CryptoAPI crypto = new CryptoAPI();
	private static SystemConfig config = SystemConfig.getInstance();
	/*
	 * Session registry
	 */
	private static ConcurrentHashMap<String, Session> registry = new ConcurrentHashMap<String, Session>();
	private static boolean enabled = true;
	private static String protocol;
	private static RSAPrivateKeyStructure connectorPrivateKey;
	private static RSAPublicKeyStructure gatewayPublicKey;
	/*
	 * Storage connector to be wired
	 */
	private Connector connector;
	private String instanceId;	
	private byte[] sessionKey;
	
	private enum Handshake {
		INIT, CHALLENGE, READY, SENDING, RECEIVING
	}
	private Handshake status = Handshake.INIT;
	private long lastActivity = 0;
	/*
	 * Send/receive file parameters
	 */
	private String currentSha256, currentUrl, currentFileType;
	private long currentFileLen = 0;
	private long currentBlock = -1;
	private long currentTotal = 0;
	
	private OutputStream outStream;
	private InputStream inStream;
	
	private JSONParser json = new JSONParser();
	
	public static void init() {
		if (onceOff.getAndIncrement() == 0) {
			initOnce();
		} else {
			onceOff.set(10); // Guarantee this atomic counter never overflows
		}
	}
	
	public static synchronized void initOnce() {
		log.info("Initialize keystore");
		protocol = config.getString("storageConnector.protocol");
		if (protocol == null || protocol.length() == 0) {
			log.error("Unable to start because storageConnector.protocol is undefined");
			enabled = false;
			return;
		}
		String gatewayKey = config.getString("storageConnector.gatewayPublicKey");
		String connectorKey = config.getString("storageConnector.connectorPrivateKey");
		String repo = config.getString("storageConnector.repository");
		
		File gatewayFile = new File(config.getConfigDir(), gatewayKey);
		File connectorFile = new File(config.getConfigDir(), connectorKey);
		if (gatewayFile.exists() && connectorFile.exists()) {
			try {
				connectorPrivateKey = crypto.getPrivateKey(util.file2str(connectorFile));
			} catch (IOException e) {
				log.error("Unable to read from connector private key for protocol "+protocol);
			}
			try {
				gatewayPublicKey = crypto.getPublicKey(util.file2str(gatewayFile));
			} catch (IOException e) {
				log.error("Unable to read from gateway public key for protocol "+protocol);
			}
			if (connectorPrivateKey != null && gatewayPublicKey != null) {
				File repoDir = new File(repo);
				if (!repoDir.exists()) repoDir.mkdirs();
			}
			
		} else {
			log.error("Unable to start because keystore is incomplete");
			enabled = false;
		}
	}
	
	public static void shutdown() {
		enabled = false;
		Enumeration<Session> list = registry.elements();
		while (list.hasMoreElements()) {
			try {
				list.nextElement().close();
			} catch (IOException e) {}
		}
	}
	
	public static boolean isEnabled() {
		return enabled;
	}
	
	@OnOpen
	public void onOpen(Session session) {
		instanceId = session.getId();
		registry.put(instanceId, session);
		log.info("WebSocket #"+instanceId+" opened");
		
		connector = getConnector();
		log.info("WebSocket #"+instanceId+", Connector implementation = "+connector);
		connector.init(config, instanceId);
		
		status = Handshake.INIT;
		lastActivity = System.currentTimeMillis();

		JSONObject request = new JSONObject();
		request.put(Constants.TYPE, "init");
		request.put("protocol", protocol);
		try {
			session.getBasicRemote().sendText(util.json2str(request));
		} catch (IOException e) {
			try {
				session.close();
			} catch (IOException e1) {}
		}
	}
	/*
	 * Synchronize because Spring is not thread safe when requesting a new bean instance
	 */
	private synchronized Connector getConnector() {
		return (Connector) config.getAppCtx().getBean("connector");
	}

	@OnMessage 
	public void textMessage(Session session, String message) {
		
		try {
			JSONObject o = (JSONObject) json.parse(message);
			
			if (o.containsKey(Constants.TYPE)) {
				String type = (String) o.get(Constants.TYPE);
				if (type.equals("error") && o.containsKey("status") && o.containsKey("message")) {
					long statusCode = (long) o.get("status");
					/*
					 * Gateway is temporarily not ready - close session and try again
					 */
					if (statusCode == 503) {
						log.warn("Session " + instanceId+", "+o.get("message"));
						try {
							session.close();
						} catch (IOException e) {}
						return;
					}
				}
				if (type.equals("key-exchange")) {
					if (o.containsKey("challenge")) {
						String encChallenge = (String) o.get("challenge");
						byte[] clearBytes = crypto.rsaDecrypt(Base64.decode(encChallenge), connectorPrivateKey.getModulus(), connectorPrivateKey.getPrivateExponent());
						if (clearBytes == null) {
							log.error("Session " + instanceId+", Unable to run because of failure to decrypt challenge from Storage Gateway");
							Dispatcher.shutdown();
							return;
						}
						byte[] encResponse = crypto.rsaEncrypt(clearBytes, gatewayPublicKey.getModulus());
						/*
						 * Generate encryption key for this session
						 */
						status = Handshake.CHALLENGE;
						sessionKey = crypto.generateSymmetricKey(256);
						log.debug("Session " + instanceId+", Session key generated "+util.bytes2hex(sessionKey));
						byte[] encSessionKey = crypto.rsaEncrypt(sessionKey, gatewayPublicKey.getModulus());
						JSONObject reply = new JSONObject();
						reply.put(Constants.TYPE, "key-exchange");
						reply.put("response", new String(Base64.encode(encResponse)));
						reply.put("session", new String(Base64.encode(encSessionKey)));
						sendTextMessage(session, util.json2str(reply));
						return;
					}
					if (o.containsKey("ready") && o.containsKey("id")) {
						String id = (String) o.get("id");
						String encReady = (String) o.get("ready");
						byte[] clearBytes = crypto.rsaDecrypt(Base64.decode(encReady), connectorPrivateKey.getModulus(), connectorPrivateKey.getPrivateExponent());
						if (clearBytes == null) {
							log.error("Session " + instanceId+", Unable to run because of failure to decrypt session key from Storage Gateway");
							Dispatcher.shutdown();
							return;
						}
						if (Arrays.equals(sessionKey, clearBytes)) {
							status = Handshake.READY;
							registry.remove(session.getId());
							/*
							 * Update registry with virtual storage sessionId to improve log trace-ability
							 */
							instanceId = id;
							registry.put(id, session);
							connector.setInstance(id);						
							/*
							 * Now ready to listen to commands
							 */
							log.info("Session " + instanceId+", Key exchange completed for WebSocket #"+session.getId());
							
						} else {
							log.error("Session " + instanceId+", Unable to run because of key exchange failure");
							Dispatcher.shutdown();
						}
						return;
					}

				}
				if (status == Handshake.READY) {
					
					try {
						if (type.equals("prepare_to_send") && o.containsKey("sha256") && o.containsKey("fileLen") && o.containsKey("fileType")) {
							String fileType = (String) o.get("fileType");
							if (fileType.equals("original") || fileType.equals("preview")) {
								/*
								 * fileType = "original" or "preview"
								 */
								String sha256 = (String) o.get("sha256");
								long fileLen = (long) o.get("fileLen");
								String url = connector.getUrl(sha256, fileLen);
								o.put("exists", connector.exists(url, fileType));
								o.put("url", connector.getUrl(sha256, fileLen));
								sendTextMessage(session, util.json2str(o));
								return;
							}
							

						}
						
						if (type.equals("delete") && o.containsKey("url")) {
							String url = (String) o.get("url");
							o.put("success", connector.delete(url));
							sendTextMessage(session, util.json2str(o));
							return;
						}
						
						if (type.equals("download") && o.containsKey("url") && o.containsKey("fileType")) {
							String url = (String) o.get("url");
							String fileType = (String) o.get("fileType");
							if (fileType.equals("original") || fileType.equals("preview")) {
								Map<String, Object> map = connector.getMetadata(url);
								if (!map.isEmpty() && map.containsKey("sha256") && map.containsKey("fileLen")) {
									currentSha256 = (String) map.get("sha256");
									currentFileLen = (long) map.get("fileLen");
									currentUrl = url;
									o.put("sha256", currentSha256);
									o.put("fileLen", currentFileLen);
									o.put("fileType", fileType);
									o.put("url", url);
									o.put("exists", connector.exists(url, fileType));
									sendTextMessage(session, util.json2str(o));
									
									inStream = connector.getInputStream(url, fileType);
									if (inStream != null) {
										try {
											status = Handshake.SENDING;
											currentFileType = fileType;
											long fileLength = connector.getFileLength(url, fileType);
											currentBlock = 1;
											currentTotal = fileLength / Constants.WS_BLOCKSIZE + (fileLength % Constants.WS_BLOCKSIZE > 0 ? 1 : 0);
											NvPair nv = new NvPair();
											nv.put(Constants.TYPE, "download");
											nv.put("total", currentTotal);
											nv.put("block", currentBlock);
											nv.put("sha256", currentSha256);
											nv.put("fileLen", currentFileLen);
											nv.put("fileType", fileType);
											nv.put("url", currentUrl);
											nv.put("data", readNextBlock(inStream));
											session.getBasicRemote().sendBinary(ByteBuffer.wrap(nv.toByteArray()));
											return;
										} catch (IOException e) {
											try {
												session.close();
											} catch (IOException e1) {}
										}
									}
								}
								return;
							}

						}
					} catch (ConnectorException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}

				if (status == Handshake.SENDING) {
					
					if (status == Handshake.SENDING && type.equals("download") && o.containsKey("ack") && o.containsKey("url") && o.containsKey("sha256") && o.containsKey("fileLen") && o.containsKey("fileType")) {
						long block = (long) o.get("ack");
						String sha256 = (String) o.get("sha256");
						long fileLen = (long) o.get("fileLen");
						String url = (String) o.get("url");
						String fileType = (String) o.get("fileType");
						
						if (currentBlock == block && url.equals(currentUrl) && fileLen == currentFileLen && sha256.equals(currentSha256) && fileType.equals(currentFileType)) {
							if (currentTotal > block) {
								try {
									currentBlock++;
									NvPair nv = new NvPair();
									nv.put(Constants.TYPE, "download");
									nv.put("total", currentTotal);
									nv.put("block", currentBlock);
									nv.put("sha256", currentSha256);
									nv.put("fileLen", currentFileLen);
									nv.put("fileType", currentFileType);
									nv.put("url", currentUrl);
									nv.put("data", readNextBlock(inStream));
									session.getBasicRemote().sendBinary(ByteBuffer.wrap(nv.toByteArray()));
									return;
								} catch (IOException e) {
									try {
										session.close();
									} catch (IOException e1) {}
								}
							} else if (currentTotal == block) {
								try {
									inStream.close();
								} catch (IOException e) {}
								
								inStream = null;
								currentBlock = -1;
								currentTotal = 0;
								status = Handshake.READY;
								log.info("Session " + instanceId+", File delivered "+currentSha256+"-"+currentFileLen+" "+currentFileType);
								return;
							}
						}
					}
					
				}
				
				
			}
		} catch (ParseException e) {
			log.error("Session " + instanceId+", Unable to start because Storage Gateway does not speak JSON");
			Dispatcher.shutdown();
			return;
		}
		/*
		 * reject if protocol is not detected
		 */
		closeInvalidSession(session);
	}

	@OnMessage
	public void binaryMessage(Session session, ByteBuffer buffer, boolean isLast) {
		
		NvPair nv = new NvPair(getBytes(buffer));
		
		if (nv.exists(Constants.TYPE)) {
			String type = nv.getString(Constants.TYPE);
			if (type.equals("sendfile") && nv.exists("total") && nv.exists("block") && nv.exists("sha256") && nv.exists("fileLen") && nv.exists("data") && nv.exists("fileType")) {
				String fileType = nv.getString("fileType");
				long block = nv.getLong("block");
				long total = nv.getLong("total");
				String sha256 = nv.getString("sha256");
				long fileLen = nv.getLong("fileLen");
				byte[] data = nv.get("data");
				boolean accepted = false;
				if (status == Handshake.READY && currentBlock == -1 && block == 1) {
					status = Handshake.RECEIVING;
					try {
						String url = connector.getUrl(sha256, fileLen);
						outStream = connector.getOutputStream(url, fileType);
						outStream.write(data);
						currentBlock = 1;
						currentTotal = total;
						
						currentSha256 = sha256;
						currentFileLen = fileLen;
						currentFileType = fileType;
						accepted = true;

					} catch (IOException e) {
						log.warn("Session " + instanceId+", "+e.getMessage());
						try {
							session.close();
						} catch (IOException e1) {}
						return;
					}
				} else if (status == Handshake.RECEIVING && block == currentBlock+1) {
					try {
						outStream.write(data);
					} catch (IOException e) {
						log.warn("Session " + instanceId+", Error: "+e.getMessage());
						try {
							session.close();
						} catch (IOException e1) {}
						return;
					}
					currentBlock++;
					accepted = true;
				}
				
				if (status == Handshake.RECEIVING && accepted) {
					
					String url = connector.getUrl(sha256, fileLen);
					JSONObject reply = new JSONObject();
					reply.put(Constants.TYPE, "sendfile");
					reply.put("ack", block);
					reply.put("sha256", sha256);
					reply.put("fileLen", fileLen);
					reply.put("fileType", currentFileType);
					reply.put("url", url);
					try {
						session.getBasicRemote().sendText(util.json2str(reply));
					} catch (IOException e) {
						e.printStackTrace();
					}
					/*
					 * All block received? Reset counters.
					 */
					if (currentBlock == currentTotal) {
						try {
							outStream.close();
							log.warn("Session " + instanceId+", Saved "+url+", "+sha256+"-"+fileLen+" "+fileType);
						} catch (IOException e) {
							/*
							 * Checksum error?
							 */
							log.warn("Session " + instanceId+", Error: "+e.getMessage());
							e.printStackTrace();
						}
						outStream = null;
						currentBlock = -1;
						currentTotal = 0;
						status = Handshake.READY;
					}
					return;
				}
			}
		}
		/*
		 * reject if protocol is not detected
		 */
		closeInvalidSession(session);
	}

	@OnClose
	public void onClose(Session session) {
		if (instanceId != null && registry.containsKey(instanceId)) {
			if (connector != null) connector.shutdown();
			registry.remove(instanceId);
			log.info("Session "+instanceId+", ws#"+session.getId()+" closed");
			/*
			 * Restart another session if it is still enabled
			 */
			if (enabled) {
				DispatchLoader dispatcher = new DispatchLoader();
				dispatcher.start();
			}
		}
	}

	@OnError
	public void error(Session session, Throwable e) {
		log.error("Session "+instanceId+", Exception: "+e.getMessage()+", Cause: "+e.getCause());
		if (session.isOpen()) {
			try {
				session.close();
			} catch (IOException e1) {}
		} else {
			if (instanceId != null && registry.containsKey(instanceId)) {
				if (connector != null) connector.shutdown();
				registry.remove(instanceId);
				log.warn("Session "+instanceId+", ws#"+session.getId()+" aborted");
			}
		}
	}
	
	private byte[] getBytes(ByteBuffer message) {
		byte[] b;
		if (message.hasArray()) {
			b = message.array();
		} else {
			b = new byte[message.remaining()];
			message.get(b, 0, b.length);
		}
		return b;
	}
	
	private void closeInvalidSession(Session session) {
		log.warn("Session " + instanceId+", closed due to invalid protocol");
		try {
			session.close();
		} catch (IOException e) {}
	}
	
	private void sendTextMessage(Session session, String message) {
		if (session.isOpen()) {
			try {
				session.getBasicRemote().sendText(message);
			} catch (IOException e) {
				try {
					session.close();
				} catch (IOException e1) {}
			}
		}

	}
	
	private byte[] readNextBlock(InputStream in) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		byte[] buffer = new byte[Constants.WS_BLOCKSIZE];
		int len = in.read(buffer, 0, buffer.length);
		if (len > 0) out.write(buffer, 0, len);
		return out.toByteArray();
	}

}
