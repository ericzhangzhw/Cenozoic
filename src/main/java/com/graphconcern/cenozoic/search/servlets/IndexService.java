package com.graphconcern.cenozoic.search.servlets;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.graphconcern.cenozoic.search.index.IndexController;
import com.graphconcern.cenozoic.search.init.Startup;
import com.graphconcern.cenozoic.search.util.UtilConstants;
import com.graphconcern.cenozoic.search.util.CollectionHelper;
import com.graphconcern.cenozoic.support.config.SystemConfig;
import com.graphconcern.cenozoic.support.util.Utility;

@WebServlet(value={"/index/*"})
public class IndexService extends HttpServlet {

	private static final long serialVersionUID = -6047745176328511811L;

	private static SystemConfig config;	
	private IndexController indexController;
	private static Utility util = new Utility();
	private static final Logger log = LoggerFactory.getLogger(IndexService.class);

	@Override
	public void init(ServletConfig servletConfig) throws ServletException {
		super.init(servletConfig);
		config = SystemConfig.getInstance();
		indexController = Startup.getIndexController();

	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		processRequest(request, response);
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		processRequest(request, response);
	}

	@SuppressWarnings("unchecked")
	protected void processRequest(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		if (!config.isReady()) {
			util.sendJsonError(response, 503, "System temporarily unavailable - Please try again.");
			return;
		}

		String space = CollectionHelper.getSpaceArea(request, response);
		String service = UtilConstants.SERVICE_KEY_INDEX;

		try {
			JSONObject document = CollectionHelper.getJSONObject(request);

			indexController.createDocument(space, document);

			/*
			 * Prepare the response JSON file for indexing response. 
			 */
			JSONObject rep = new JSONObject();
			rep.put(UtilConstants.PARAM_KEY_REQUEST_SERVICE, service);
			rep.put(UtilConstants.PARAM_KEY_REQUEST_SPACE, space);
			rep.put(UtilConstants.PARAM_KEY_REQUEST_CONTENT, document.toJSONString());
			rep.put(UtilConstants.REPLY_HTTP_STATUS_KEY, UtilConstants.REPLY_HTTP_STATUS_OK);
			rep.put(UtilConstants.REPLY_MESSAGE_KEY, "Index Successfully");
			
			/*
			 * Prepare the response object with defining the Content Type and Encoding. 
			 */
			response.setCharacterEncoding("UTF-8");
			response.setContentType("application/json");
			
			/*
			 * Prepare the output stream and write out the prepared JSON file
			 */
			OutputStream out = response.getOutputStream();
			out.write(util.json2bytes(rep));
			out.close();

		} catch (ParseException pe) {
			log.error("Invalid data format or empty search information.");
			pe.printStackTrace();
		}


	}





}
