package com.graphconcern.cenozoic.search.servlets;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import javax.servlet.AsyncContext;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.graphconcern.cenozoic.search.init.Startup;
import com.graphconcern.cenozoic.search.util.CollectionHelper;
import com.graphconcern.cenozoic.search.util.UtilConstants;
import com.graphconcern.cenozoic.support.config.SystemConfig;
import com.graphconcern.cenozoic.support.util.MqUtil;
import com.graphconcern.cenozoic.support.util.Utility;

/**
 * Servlet implementation class SearchService
 */
@WebServlet(value = {"/search/*"}, asyncSupported = true)
public class SearchService extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static MqUtil mqUtil;

	private static SystemConfig config;

	private static Utility util = new Utility();

	private static SearchServiceListener listener;

	private static Destination myTempQueue;

	public static ConcurrentHashMap<String, AsyncContext> asyncCtx = new ConcurrentHashMap<String, AsyncContext>();

	private static final Logger log = LoggerFactory.getLogger(SearchService.class);

	@Override
	public void init(ServletConfig servletConfig) throws ServletException {
		super.init(servletConfig);
		config = SystemConfig.getInstance();
		if (listener == null) {
			mqUtil = Startup.getMq().getMqUtil();
			
			try {
				myTempQueue = mqUtil.createTemporaryQueue();
				listener = new SearchServiceListener();
				mqUtil.createTempQueueMessageConsumer(myTempQueue, listener);
				log.info("Starting search worker listener at " + myTempQueue);
			} catch (JMSException e) {
				log.error("\tError with create temp queue message consumer: " + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected void processRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		if (!config.isReady()) {
			util.sendJsonError(response, 503, "System temporarily unavailable - Please try again.");
			return;
		}	

		String space = CollectionHelper.getSpaceArea(request, response);
		String service = UtilConstants.SERVICE_KEY_SEARCH;

		// Trigger the searching function
		try {
			// Step 1: Generate an UUID for searching
			String identity = util.generateUUID();

			// Step 2: Putting the UUID into the request attribute for asynchronize retrieval
			request.setAttribute(UtilConstants.TYPE_KEY_ID, identity);

			// Step 3: Putting the identity value into the content for asynchronize retrieval
			//         and prepare callback listener for picking up the response. 
			final AsyncContext ctx = request.startAsync();
			ctx.setTimeout(UtilConstants.INTRANET_TIMEOUT); 
			ctx.addListener(new SearchAsyncListener()); //create another one for search action return
			asyncCtx.put(identity, ctx);

			// Step 4: Prepare the JSON object for submission
			JSONObject content = null;
			try {
				content = CollectionHelper.getJSONObject(request);
				JSONObject json = new JSONObject(); 
				json.put(UtilConstants.PARAM_KEY_REQUEST_SPACE, space);
				json.put(UtilConstants.TYPE_KEY_ID, identity);
				json.put(UtilConstants.PARAM_KEY_REQUEST_SERVICE, service);
				json.put(UtilConstants.PARAM_KEY_REQUEST_CONTENT, content.toJSONString());

				// Step 5: Sending into temporary internal MQ
				log.debug("\tSubmit to MQ: " + content.toJSONString());
				mqUtil.sendRequest(UtilConstants.QUEUE_SEARCH_WORKER, util.json2str(json), myTempQueue);
			} catch (ParseException e) {
				// crash and burn
				throw new IOException("Error parsing JSON request string");
			}
		} catch (JMSException e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}	

	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		processRequest(request, response);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		processRequest(request, response);
	}



	private class SearchServiceListener implements MessageListener {

		private JSONParser parser = new JSONParser();

		@Override
		public void onMessage(Message message) {
			if (message instanceof TextMessage) {
				TextMessage text = (TextMessage) message;
				try {
					JSONObject json = (JSONObject) parser.parse(text.getText());
					if (json.containsKey(UtilConstants.STATUS_KEY) && json.containsKey(UtilConstants.TYPE_KEY_ID)) {
						String identity = (String) json.get(UtilConstants.TYPE_KEY_ID);
						String status = (String) json.get(UtilConstants.STATUS_KEY);

						AsyncContext ctx = asyncCtx.get(identity);
						if (ctx != null) {
							if (status!=null && !status.equals(UtilConstants.STATUS_ERROR)) {
								HttpServletResponse response = (HttpServletResponse) ctx.getResponse();
								response.setCharacterEncoding("UTF-8");
								response.setContentType("application/json");
								response.getOutputStream().write(util.json2bytes(json));
								ctx.complete();
							}
						}
					}
				} catch (ParseException | JMSException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		}

	}

}
