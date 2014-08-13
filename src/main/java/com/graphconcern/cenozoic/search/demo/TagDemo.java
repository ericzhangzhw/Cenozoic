package com.graphconcern.cenozoic.search.demo;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.yaml.snakeyaml.Yaml;

import com.graphconcern.cenozoic.support.config.SystemConfig;
import com.graphconcern.cenozoic.support.util.Utility;

@SuppressWarnings("serial")
@WebServlet(value={"/tagdemo"})
public class TagDemo extends HttpServlet {
	
	private static Utility util = new Utility();
	
	private static CloseableHttpClient httpclient;
	private static String template;
	
	public void init(ServletConfig servletConfig) throws ServletException {
		if (httpclient == null) {
			SystemConfig config = SystemConfig.getInstance();
			httpclient = config.getHttpClient();
			File confDir = config.getConfigDir();
			File templateRoot = new File(confDir.getParentFile(), "templates");
			File f = new File(templateRoot, "demo.html");
			if (f.exists()) {
				template = util.file2str(f);
			}
		}
	}
	
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		
		String path = request.getHeader("X-User");
		if (path == null) path = "demo" ;
		
		String result = template.replace("$index", "").replace("$search", "").replace("$docid", "test").replace("$tag1", "hello world").replace("$tag2", "world");
		response.getOutputStream().write(util.getUTF(result));
		
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		
		request.setCharacterEncoding("UTF-8");
		
		String path = request.getHeader("X-User");
		if (path == null) path = "demo" ;
		
		String command = request.getParameter("c");
		
		if (command.equals("index")) {
			String docId = request.getParameter("doc_id");
			String tags = request.getParameter("doc_tags");
			if (docId.length() == 0) docId = "unknown";
			if (tags.length() == 0) tags = "empty";
			
			JSONObject header = new JSONObject();
			header.put("uuid", "id");
			header.put("tags", "index_store");
			JSONObject body = new JSONObject();
			body.put("uuid", docId);
			body.put("tags", tags);
			
			JSONObject content = new JSONObject();
			content.put("header", header);
			content.put("body", body);

			HttpPost httpPost = new HttpPost("http://127.0.0.1:8080/clucene/index/" + path);
			StringEntity entity = new StringEntity(util.json2str(content), ContentType.create("application/json", "UTF-8"));
			httpPost.setEntity(entity);
			
			try {
				CloseableHttpResponse res = httpclient.execute(httpPost);
				StatusLine status = res.getStatusLine();
				if (status.getStatusCode() == 200) {
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					res.getEntity().writeTo(baos);
					res.close();
					
					String result = template.replace("$index", util.getUTF(baos.toByteArray()))
							.replace("$search", "").replace("$docid", "test").replace("$tag1", tags).replace("$tag2", "world");
					
					response.getOutputStream().write(util.getUTF(result));
					
				} else {
					response.getWriter().write("Sorry the system cannot finish your request: HTTP-"+status);
				}

				
			} catch (IOException ex) {
				ex.printStackTrace();
			}
			
		}
		if (command.equals("search")) {
			String tags = request.getParameter("doc_tags");
			if (tags.length() == 0) tags = "empty";
			
			JSONObject body = new JSONObject();
			body.put("tags", tags);
			
			JSONObject content = new JSONObject();
			content.put("query_string", body);
			
			HttpPost httpPost = new HttpPost("http://127.0.0.1:8080/clucene/search/" + path);
			StringEntity entity = new StringEntity(util.json2str(content), ContentType.create("application/json", "UTF-8"));
			httpPost.setEntity(entity);
			
			try {
				CloseableHttpResponse res = httpclient.execute(httpPost);
				StatusLine status = res.getStatusLine();
				if (status.getStatusCode() == 200) {
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					res.getEntity().writeTo(baos);
					res.close();
					
					String s = util.getUTF(baos.toByteArray());
					JSONParser json = new JSONParser();
					try {
						HashMap o = (HashMap) json.parse(s);
						Yaml yaml = new Yaml();
						String yamlString = yaml.dump(o).replace("\n", "<br>");
						
						String result = template.replace("$index", "").replace("$search", yamlString).replace("$docid", "test").replace("$tag1", "hello world").replace("$tag2", tags);
						
						response.getOutputStream().write(util.getUTF(result));
						
					} catch (ParseException e) {
						response.getWriter().write("Sorry the system cannot finish your request due to some unexpected errors");
					}
				} else {
					response.getWriter().write("Sorry the system cannot finish your request: HTTP-"+status);
				}

				
			} catch (IOException ex) {
				ex.printStackTrace();
			}
			
		}
		
		
	}
	

}
