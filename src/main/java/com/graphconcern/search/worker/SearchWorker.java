package com.graphconcern.search.worker;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.graphconcern.search.engine.CqlDirectory;
import com.graphconcern.search.init.Startup;
import com.graphconcern.search.model.SearchableDoc;
import com.graphconcern.search.servlets.SearchService;
import com.graphconcern.search.util.CollectionHelper;
import com.graphconcern.search.util.UtilConstants;
import com.graphconcern.support.util.MqUtil;
import com.graphconcern.support.util.Utility;
import com.datastax.driver.core.Session;

public class SearchWorker implements MessageListener {

	private Session session;

	private String accessCode;

	private MqUtil mqUtil;

	private JSONParser parser = new JSONParser();

	private Utility util = new Utility();

	private static final Logger log = LoggerFactory.getLogger(SearchWorker.class);

	public SearchWorker() {
		this.session = Startup.getCassandraSession();
		this.accessCode = util.generateUUID();
		mqUtil = Startup.getMq().getMqUtil();
		log.info("Search worker created with access code: " + accessCode);

	}

	@SuppressWarnings("unchecked")
	@Override
	public void onMessage(Message message) {
		if (message instanceof TextMessage) {
			TextMessage text = (TextMessage) message;
			try {
				JSONObject json = (JSONObject) this.parser.parse(text.getText());
				String identity = (String) json.get(UtilConstants.TYPE_KEY_ID);
				String space = (String) json.get(UtilConstants.PARAM_KEY_REQUEST_SPACE);

				/*
				 * Checking if the space has been locked for optimization, and wait until the optimization has been done. 
				 */
				while(!OptimizeWorker.isAvailable(space, accessCode) && SearchService.asyncCtx.get(identity) != null) {
					Thread.sleep(UtilConstants.OPTIMIZATION_WAIT);
				}
				if (SearchService.asyncCtx.get(identity) != null ) {
					
					SearchableDoc searchDoc = new SearchableDoc(
							CollectionHelper.getHashtable((JSONObject) 
									parser.parse((String) 
											json.get(UtilConstants.PARAM_KEY_REQUEST_CONTENT)), 
											UtilConstants.CONTENT_KEY_QUERY_STRING));

					if (searchDoc.size() > 0) {

						Destination dest = message.getJMSReplyTo();

						IndexSearcher searcher = null;
						TopDocs result = null;

						Directory dir = new CqlDirectory(mqUtil, session, space, this.accessCode);
						try {
							String responseCode = UtilConstants.STATUS_FOLLOWUP;

							if (DirectoryReader.indexExists(dir) ) {
								IndexReader reader = DirectoryReader.open(dir);
								SearcherFactory factory = new SearcherFactory();
								searcher = factory.newSearcher(reader);

								BooleanQuery booleanQuery = new BooleanQuery();
								for(String key : searchDoc.keySet()) {

									Query query = new WildcardQuery(new Term(key, searchDoc.get(key).toLowerCase()));
									booleanQuery.add(query, BooleanClause.Occur.MUST);

								}
								log.info("Actual query string : " + booleanQuery.toString());
								result = searcher.search(booleanQuery, Integer.MAX_VALUE - 1);

								responseCode = UtilConstants.STATUS_OK;

							} else {
								responseCode = UtilConstants.STATUS_FOLLOWUP;
								log.warn("WARNING: No index for this space");
							}

							JSONObject resultResponse = generateResponse(searcher, result);
							resultResponse.put(UtilConstants.STATUS_KEY, responseCode);
							resultResponse.put(UtilConstants.TYPE_KEY_ID, identity);
							resultResponse.put(UtilConstants.PARAM_KEY_REQUEST_SPACE, space);
							log.debug("Preparing response message: " + resultResponse.toJSONString());
							mqUtil.sendResponse(dest, util.json2str(resultResponse));

						} catch (IOException ioe) {
							log.error("ERROR:\n\tIOException found. " + ioe.getMessage());
							ioe.printStackTrace();
						}
					}
				} else {
					log.warn("It has already been time out.");
				}
			} catch (ParseException | JMSException | InterruptedException e) {
				e.printStackTrace();
			}

		}
	}

	@SuppressWarnings("unchecked")
	private JSONObject generateResponse(IndexSearcher searcher, TopDocs result) {
		if (searcher != null && result != null) {

			ScoreDoc[] hits = result.scoreDocs;
			int numTotalHits = result.totalHits;

			log.info("Total number of hits: " + numTotalHits);

			JSONObject resultSet = new JSONObject();
			LinkedHashMap<Integer, LinkedHashMap<String, String>> body = new LinkedHashMap<Integer, LinkedHashMap<String, String>>();
			resultSet.put(UtilConstants.REPLY_KEY_TOTAL_HITS, Integer.toString(numTotalHits));

			int i = 1;
			for(ScoreDoc sDoc : hits) {
				try {
					Document document = searcher.doc(sDoc.doc);
					List<IndexableField> list= document.getFields();
					LinkedHashMap<String, String> item = new LinkedHashMap<String, String>();
					for (IndexableField field : list) {
						item.put(field.name(), document.get(field.name()));
					}
					body.put(new Integer(i), item);
					i++;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			resultSet.put(UtilConstants.REPLY_KEY_RESULT_SET, body);
			log.info("Search Result: " + resultSet.toJSONString());
			return resultSet;
		}
		return new JSONObject();
	}

}