/** 
/*Graph Concern Inc Confidential Information.
/* TM and (c) 2013 Graph Concern Inc.  All Rights Reserved.
/* Reproduction in whole or in part without prior written permission of a
/* duly authorized representative is prohibited.
 * 
 * Author: Felix Chiu
 * Date: 3/20/2014
 */
package com.graphconcern.cenozoic.support.util;

public class Constants {
	/*
	 * Web socket transmission buffer size
	 */
	public static final int WS_BLOCKSIZE = 6144;
	/*
	 * Excluded queue and topic prefix
	 * 
	 * Use this to exclude queues and topics for the network MQ.
	 * i.e. they should not routed outside the embedded MQ instance.
	 * 
	 * Default: "private."
	 * +++ DO NOT CHANGE +++
	 */
	public static final String PRIVATE_MQ_PREFIX = "private.";
	public static final String WS_SESSION_PREFIX = "ws.session.";
	public static final String WS_ROOM_PREFIX = "ws.room.";
	/*
	 * Preview image type - This must be "jpg".
	 * +++ DO NOT CHANGE +++
	 */
	public static final String PREVIEW_IMG_TYPE = "jpg";
	/*
	 * Message queues.
	 * Private queues are prefixed with PRIVATE_QUEUE_PREFIX
	 */
	public static final String STORAGE_CONNECTOR = "storage.connector";
	public static final String PREVIEW_ENGINE = "preview.engine";
	public static final String METADATA_SERVICE = "metadata.service";
	public static final String WS_EVENT = "ws.event";
	
	public static final String UPLOAD2STORAGE = PRIVATE_MQ_PREFIX+"upload.2storage";
	public static final String DOWNLOAD_STORAGE = PRIVATE_MQ_PREFIX+"download.storage";
	public static final String PENDING_UPLOAD = PRIVATE_MQ_PREFIX+"pending.upload";
	public static final String SESSION_DB = PRIVATE_MQ_PREFIX+"SESSION.DB";
	public static final String LDAP_REQUEST = PRIVATE_MQ_PREFIX+"LDAP.REQUEST";
	public static final String LDAP_RESPONSE = PRIVATE_MQ_PREFIX+"LDAP.RESPONSE";
	public static final String LDAP_BRIDGE = PRIVATE_MQ_PREFIX+"LDAP.BRIDGE";
	public static final String LOGIN_SERVLET = PRIVATE_MQ_PREFIX+"LOGIN.SERVLET";
	public static final String WS_SESSION = PRIVATE_MQ_PREFIX+"ws.session";
	public static final String WS_ROOM = PRIVATE_MQ_PREFIX+"ws.room";
	/*
	 * Time constants
	 */
	public static final long ONE_MINUTE = 60 * 1000;
	public static final long FIVE_MINUTES = ONE_MINUTE * 5;
	public static final long TEN_MINUTES = ONE_MINUTE * 10;
	public static final long THIRTY_MINUTES = ONE_MINUTE * 30;
	public static final long ONE_HOUR = ONE_MINUTE * 60;
	public static final long THREE_HOUR = ONE_HOUR * 3;	
	public static final long THIRTY_SECONDS = 30 * 1000;
	public static final long THIRY_MINUTES = ONE_MINUTE * 30;
	public static final long ONE_DAY = ONE_MINUTE * 60 * 24;
	public static final long MAX_AGE_STATIC = TEN_MINUTES / 1000;
	public static final long MAX_LONG_POLL = 45 * 1000;
	public static final long INTRANET_TIMEOUT = 15000;
	public static final long BLOB_DOWNLOAD_TIMEOUT = 45000;
	/*
	 * For REST and WEB gatekeepers
	 */
	public static final String INS_HEADER = "ins-headers";
	public static final int DEFAULT_MAX_LEN = 500 * 1024;	// default: 500 KB
	public static final int DEFAULT_HTTP_TIMEOUT = 30;		// default: 30 seconds
	public static final int MAX_HTTP_TIMEOUT = 180;		// maximum: 3 minutes
	/*
	 * Empty string
	 */
	public static final String EMPTY = "";
	public static final byte[] EMPTY_BYTES = new byte[0];
	/*
	 * Temp URL types
	 */
	public static final String ORIGINAL = "original";
	public static final String PREVIEW = "preview";
	public static final String THUMBNAIL = "thumbnail";
	public static final String ALL = "all";
	public static final String FILE = "file";
	/*
	 * Request types
	 */
	public static final String TYPE = "type";
	public static final String CREATE_UPLOAD_URL = "create_upload_url";
	public static final String REGISTER_FILE = "register_file";
	public static final String CLONE_FILE = "clone_file";
	public static final String UPDATE_FILE = "update_file";
	public static final String IMMUTABLE_FILE = "immutable_file";
	public static final String CREATE_DOWNLOAD_URL = "create_download_url";
	public static final String GET_METADATA = "get_metadata";
	public static final String DELETE_FILE = "del_file";
	public static final String CHECK_PREVIEW = "check_preview";
	public static final String DOWNLOAD_TYPE = "download_type";
	public static final String HELLO_WORLD = "hello_world";
	public static final String UPLOAD_IN_PROGRESS = "upcoming_upload";
	/*
	 * web socket
	 */
	public static final String FROM = "from";
	public static final String TO = "to";
	public static final String PRESENCE = "presence";
	public static final String ONLINE = "online";
	public static final String OFFLINE = "offline";
	public static final String INSTANCE = "instance";
	public static final String PING = "ping";
	public static final String PONG = "pong";
	public static final String ROOM = "room";
	public static final String ROOMS = "rooms";
	public static final String MEMBERS = "members";
	public static final String REQUEST = "request";
	public static final String TIMESTAMP = "timestamp";
	/*
	 * Response types
	 */
	public static final String UPLOAD_URL = "upload_url";
	public static final String DOWNLOAD_URL = "download_url";
	public static final String UUID = "uuid";
	public static final String MESSAGE = "message";
	public static final String STATUS = "status";
	public static final String METADATA = "metadata";
	/*
	 * MQ and other attributes
	 */
	public static final String MSG_COMMAND = "cmd";
	public static final String MSG_OBJECT = "obj";
	public static final String DOC_VERSION_UUID = "docVerUuid";
	public static final String NEW_DOC_VERSION = "newDocVer";
	public static final String OLD_DOC_VERSION = "oldDocVer";
	public static final String FILE_UUID = "fileUuid";
	public static final String EVENT_PATH = "eventPath";
	public static final String EXCEPTION_PATH = "errorPath";
	public static final String SECURE = "secure";
	public static final String FILENAME = "filename";
	/*
	 * Servlet 3.0 async attributes
	 */
	public static final String REQUEST_ID = "requestId"; 
	public static final String X_CORRELATION_ID = "X-Correlation-ID";
	/*
	 * Realms
	 */
	public static final String CI_REALM = "CI";
	/*
	 * Mapping to correctly persist Shiro classes to the session store
	 */
	public static final String AUTHENTICATED_SESSION_KEY = "org.apache.shiro.subject.support.DefaultSubjectContext_AUTHENTICATED_SESSION_KEY";
	public static final String PRINCIPALS_SESSION_KEY = "org.apache.shiro.subject.support.DefaultSubjectContext_PRINCIPALS_SESSION_KEY";
	public static final String AUTHENTICATED = "_authenticated";	// map to Boolean
	public static final String PRINCIPAL = "_principal";			// map to SimplePrincipalCollection
	public static final String[][] SHIRO_MAP = { { AUTHENTICATED_SESSION_KEY, AUTHENTICATED }, 
												 { PRINCIPALS_SESSION_KEY, PRINCIPAL },
											   };
	/*
	 * Custom expiry attribute
	 */
	public static final String CUSTOM_EXPIRY = "expires";
	/*
	 * Security service tags
	 */
	public static final String RETURN_QUEUE = "return.queue";
	public static final String X_SESSION = "X-Session";
	public static final String X_USER = "X-User";	
	public static final String X_ACCOUNT = "X-Account";	
	public static final String X_ROLES = "X-Roles";
	public static final String SESSION_ID = "session.id";
	public static final String UID = "uid";
	public static final String USER_ID = "userId";
	public static final String PASSWORD = "password";
	public static final String NICKNAME = "nickname";
	public static final String LOCK = "lock";
	public static final String ERROR_MSG = "error";
	public static final String SUCCESS = "success";
	public static final String FAIL = "fail";
	public static final String CREATE = "create";
	public static final String SENDER = "sender";
	public static final String OAUTH_TIMESTAMP = "_ts";
	public static final String ACCESS_TIME = "_access";
	/*
	 * Request attributes for session and subject
	 */
	public static final String CURRENT_SESSION = "_session";
	public static final String CURRENT_SUBJECT = "_subject";
	/*
	 * Redirection for login servlet
	 */
	public static final String CONTINUE_URL = "continue";
	public static final String RETURN_PATH = "home";
	public static final String RETURN_2_LEVEL = "home_2";
	/*
	 * Session type
	 */
	public static final String SESSION_TYPE = "_type";
	public static final String ACCESS_TOKEN = "token";
	public static final String USER_SESSION = "user";
	public static final String AUTHORIZATION_CODE = "code";
	public static final String AJAX = "ajax";
	public static final String NONCE = "nonce";
	public static final String OPENID_NONCE = "openid.nonce";
	public static final String OPENID_OP_ASSOCIATION = "openid.op";
	public static final String OPENID_RP_ASSOCIATION = "openid.rp";
	public static final String OPENID_LATEST_ASSOCIATION = "openid.rp.current";
	
	public static final String HOUSEKEEPER = "housekeeper";
	/*
	 * Authentication method tag
	 */
	public static final String AUTH_METHOD = "_auth";
	public static final String AUTH_ONEID = "oneid";
	public static final String AUTH_BASIC = "basic";
	public static final String AUTH_LDAP = "ldap";
	public static final String AUTH_OPENID = "openid";
	/*
	 * Pending flag for device authentication tag
	 */
	public static final String DEVICE_AUTH_EMAIL = "_device_oneid";
	/*
	 * OneID specific tags
	 */
	public static final String ONE_ID = "1_uid";
	public static final String TWO_FA = "1_2fa";
	/*
	 * OneID Script
	 */
	public static final String ONEID_SCRIPT = "<script src=\"https://api%.oneid.com/js/oneid.js\" type=\"text/javascript\"></script>";
	/*
	 * Constraints
	 */
	public static final int MAX_REST_THREAD = 20;	// Maximum number of concurrent REST calls per client instance
	public static final int MAX_APP_KEY = 20;		// Maximum number of app keys per developer
	/*
	 * CI Session Cookie
	 */
	public static final String CI_COOKIE = "cis";
	public static final int COOKIE_DAY = 60 * 60 * 24;
	/*
	 * Maximum HTML content-length for REST call
	 * Set to 2 MB
	 */
	public static final int MAX_CONTENT_LENGTH = 1024 * 1024 * 2;
	
	/*
	 * HTTP request headers to be filtered out because they are unsafe to pass thru API gateway
	 */
	public static String[] FILTER_HTTP_HEADERS = {"connection", "upgrade"};

}
