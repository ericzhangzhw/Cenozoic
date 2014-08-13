package com.graphconcern.support.config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONObject;
import com.graphconcern.support.util.Utility;

@WebServlet({"/_config/*"})
public class SystemStatus extends HttpServlet {
	private static Utility util = new Utility();
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		String ip = request.getRemoteAddr();
		if (!util.isIntranet(ip)) {
			response.sendError(404);
			return;
		}
		SystemConfig config = SystemConfig.getInstance();
		if (config == null) {
			response.sendError(503);
			return;
		}
		String path = request.getPathInfo();
		if (path == null) {
			util.sendJsonError(response, 401, "URL should be /status or /ready");
			return;
		}			
		List<String> elements = util.getUriPathElement(path);
		if (elements.size() != 1) {
			util.sendJsonError(response, 401, "URL should be /status or /ready");
			return;
		}
		boolean ready = config.isReady();
		if (elements.get(0).equalsIgnoreCase("ready")) {
			response.setContentType("text/plain");
			response.setHeader("Cache-Control", "no-cache");
			response.setDateHeader("Expires", 0);
			response.getWriter().print(ready? "true" : "false");
			return;
		}
		if (!elements.get(0).equalsIgnoreCase("status")) {
			util.sendJsonError(response, 401, "URL should be /status or /ready");
			return;
		}
		ConcurrentHashMap<String, String[]> accessMap = config.getAccessMap();
		ConcurrentHashMap<String, String> errorMap = config.getErrorMap();
		List<String> locations = config.getConfLocations();
		
		long now = System.currentTimeMillis();
		long appTime = config.getAppTime();
		long dependencyTime = config.getDependencyTime();
		String ntime = util.long2rfc3339(now, false).replace('T', ' ');
		String stime = util.long2rfc3339(appTime, false).replace('T', ' ');
		String dtime = util.long2rfc3339(dependencyTime, false).replace('T', ' ');
		/*
		 * Output in JSON?
		 */
		String format = request.getParameter("format");
		if ("json".equalsIgnoreCase(format)) {
			response.setCharacterEncoding("UTF-8");
			response.setContentType("application/json");
			response.setHeader("Cache-Control", "no-cache");
			response.setDateHeader("Expires", 0);
			
			JSONObject result = new JSONObject();
			result.put("hostname", config.getHostname());
			result.put("intranet", util.getMyIntranetIP());
			JSONObject readyView = new JSONObject();
			readyView.put("ready", ready);
			readyView.put("time", util.long2rfc3339(appTime, true));
			result.put("status", readyView);
			if (dependencyTime > 0) {
				JSONObject dependView = new JSONObject();
				dependView.put("ready", config.isDependencyResolved());
				dependView.put("time", util.long2rfc3339(dependencyTime, true));
				result.put("dependency", dependView);
			}
			if (accessMap.isEmpty()) {
				result.put("access", false);
			} else {
				result.put("access", true);
				JSONObject accessView = new JSONObject();
				Iterator<Entry<String, String[]>> access = accessMap.entrySet().iterator();
				List<String> accessList = new ArrayList<String>();
				while (access.hasNext()) {
					Entry<String, String[]> entry = access.next();
					accessList.add(entry.getKey());
				}
				Collections.sort(accessList);	
				for (String key: accessList) {
					String[] value = accessMap.get(key);
					List<String> valueList = new ArrayList<String>();
					for (String v: value) valueList.add(v);
					accessView.put(key, valueList);
				}
				result.put("accessLog", accessView);
			}
			if (errorMap.isEmpty()) {
				result.put("errors", false);
			} else {
				result.put("errors", true);
				JSONObject errorView = new JSONObject();
				Iterator<Entry<String, String>> errors = errorMap.entrySet().iterator();
				List<String> errorList = new ArrayList<String>();
				while (errors.hasNext()) {
					Entry<String, String> entry = errors.next();
					errorList.add(entry.getKey());
				}
				Collections.sort(errorList);	
				for (String key: errorList) {
					errorView.put(key, errorMap.get(key));
				}
				result.put("errorLog", errorView);
			}
			if (!locations.isEmpty()) {
				result.put("source", locations);
			}
			response.getOutputStream().write(util.json2bytes(result));
			return;
		}
		/*
		 * Output in HTML
		 */
		StringBuffer sb = new StringBuffer();
		sb.append("<html><head><title>Config Status</title></head><body>");
		sb.append("<style>tr:nth-child(even) { background-color: #E0F8F7; } span { margin: 5px }</style>");
		sb.append("<div style='margin: 15px 5px 15px 5px'>Server: <span style='color: #996633'>"+config.getHostname()+" "+util.getMyIntranetIP()+", "+ntime+"</span></div>");
		
		if (ready) {
			sb.append("<div style='margin: 15px 5px 15px 5px'>Status: <span style='color: #0066FF'>Application ready since "+stime+"</span></div>");
		} else {
			sb.append("<div style='margin: 15px 5px 15px 5px'>Status: <span style='color: #CC3300'>Application not ready since "+stime+"</span></div>");
		}
		if (dependencyTime > 0) {
			if (config.isDependencyResolved()) {
				sb.append("<div style='margin: 15px 5px 15px 5px'>Dependency: <span style='color: #0066FF'>Resolved since "+dtime+"</span></div>");
			} else {
				sb.append("<div style='margin: 15px 5px 15px 5px'>Dependency: <span style='color: #CC3300'>Pending since "+dtime+"</span></div>");
			}	
		}
		if (accessMap.isEmpty()) {
			sb.append("<div style='margin: 15px 5px 15px 5px; color: #CC3300'>The application has not read any configuration parameter.</div>");
		} else {
			sb.append("<div style='margin: 15px 5px 15px 5px; color: #0066FF'>The application has read the following configuration parameters:</div>");
			sb.append("<div style='margin: 5px'><table>");
			sb.append("<td><span><u>Parameter</u></span></td>");
			sb.append("<td><span><u>Time</u></span></td>");
			sb.append("<td><span><u>Type</u></span></td>");
			sb.append("<td><span><u>Value</u></span></td></tr>");
			Iterator<Entry<String, String[]>> access = accessMap.entrySet().iterator();
			List<String> list = new ArrayList<String>();
			while (access.hasNext()) {
				Entry<String, String[]> entry = access.next();
				list.add(entry.getKey());
			}
			Collections.sort(list);	
			for (String key: list) {
				String normalizedKey = key.length() > 100 ? key.substring(0, 100) + "..." : key;
				sb.append("<tr><td><span>");
				sb.append(util.escapeHTML(normalizedKey));
				String[] value = accessMap.get(key);
				sb.append("</span></td><td><span style='color: #808080'>");
				sb.append(value[0]); // make sure time-stamp is displayed as one piece
				sb.append("</span></td><td><span>");
				sb.append(value[1]);
				sb.append("</span></td><td><span>");
				sb.append(util.escapeHTML(value[2]));
				sb.append("</span></td></tr>");
			}
			sb.append("</table></div>");
		}

		if (errorMap.isEmpty()) {
			sb.append("<div style='margin: 15px 5px 15px 5px; color: #0066FF'>There are no errors detected when loading parameters.</div>");
		} else {
			sb.append("<div style='margin: 15px 5px 15px 5px; color: #CC3300'>There are some configuration errors:</div>");
			sb.append("<div style='margin: 5px'><table>");
			sb.append("<tr><td><span><u>Parameter</u></span></td><td><span><u>Error</u></span></td></tr>");
			Iterator<Entry<String, String>> errors = errorMap.entrySet().iterator();
			List<String> list = new ArrayList<String>();
			while (errors.hasNext()) {
				Entry<String, String> entry = errors.next();
				list.add(entry.getKey());
			}
			Collections.sort(list);	
			for (String key: list) {
				String normalizedKey = key.length() > 100 ? key.substring(0, 100) + "..." : key;
				sb.append("<tr><td><span>");
				sb.append(util.escapeHTML(normalizedKey));
				sb.append("</span></td><td><span style='color: #808080'>");
				sb.append(util.escapeHTML(errorMap.get(key)));
				sb.append("</span></td></tr>");
			}
			sb.append("</table></div>");
		}
		if (!locations.isEmpty()) {
			sb.append("<div style='margin: 15px 5px 15px 5px; color: #0066FF'>Configuration files:</div>");
			for (String s: locations) sb.append("<div style='margin: 5px;'>"+util.escapeHTML(s)+"</div>");
		}
		sb.append("</body></html>");
		
		response.setCharacterEncoding("UTF-8");
		response.setContentType("text/html");
		response.setHeader("Cache-Control", "no-cache");
		response.setDateHeader("Expires", 0);
		response.getOutputStream().write(util.getUTF(sb.toString()));
		
	}

}
