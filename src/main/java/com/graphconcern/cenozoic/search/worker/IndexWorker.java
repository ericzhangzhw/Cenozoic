package com.graphconcern.cenozoic.search.worker;

import java.io.File;
import java.io.IOException;
import java.util.Hashtable;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.graphconcern.cenozoic.search.index.IndexController;
import com.graphconcern.cenozoic.search.init.Startup;
import com.graphconcern.cenozoic.search.util.CollectionHelper;
import com.graphconcern.cenozoic.search.util.UtilConstants;
import com.graphconcern.cenozoic.search.analysis.SpecificKeywordAnalyzer;
import com.graphconcern.cenozoic.search.engine.CqlDirectory;
import com.graphconcern.cenozoic.search.engine.CqlLock;
import com.graphconcern.cenozoic.search.engine.CqlLockFactory;
import com.graphconcern.cenozoic.support.util.MqUtil;
import com.graphconcern.cenozoic.support.util.Utility;
import com.datastax.driver.core.Session;


public class IndexWorker implements MessageListener {

	private static final Logger log = LoggerFactory.getLogger(IndexWorker.class);

	private static Utility util = new Utility();

	private IndexController indexController;

	private String accessCode;
	private MqUtil mqUtil;

	public IndexWorker() {
		this.indexController = Startup.getIndexController();
		this.session = Startup.getCassandraSession();
		this.accessCode = util.generateUUID();
		this.mqUtil = Startup.getMq().getMqUtil();
		log.info("Index worker created with access code: " + accessCode);
	}

	public String getAccessCode() {
		return accessCode;
	}
	private Session session;


	@Override
	public void onMessage(Message message) {
		log.info("Message Received @ " + this.getClass().getName() + " with access code " + accessCode);
		if (message instanceof TextMessage) {
			TextMessage text = (TextMessage) message;
			if (text!=null) {
				try {
					log.info("Message: space = " + text.getText());
					String space = text.getText();
					build(space);
					indexController.getIndexManager().resume(space);
				} catch (IOException | JMSException e) {
					e.printStackTrace();
				} 
			}
		} 
	}

	private void build(String space) throws IOException {
		String batchNumber = indexController.getIndexManager().getBatchNumber(space);
		log.debug("Space: " + space + " Bath Number: " + batchNumber);

		if (space!=null && batchNumber!=null) {
			Directory directory = new CqlDirectory(mqUtil, session, space, accessCode);

			CqlLockFactory lockFactory = (CqlLockFactory) directory.getLockFactory();
			CqlLock lock = (CqlLock) lockFactory.makeLock(IndexWriter.WRITE_LOCK_NAME);

			if (!lock.isLocked()) {

				File batchFolder = new File(new File(indexController.getBatchFolder(), space), batchNumber);
				File[] filelist = batchFolder.listFiles();
				
				if (filelist!=null && filelist.length > 0) {
					Analyzer analyzer = new StandardAnalyzer(UtilConstants.LUCENE_VERSION);
					IndexWriterConfig iwc = new IndexWriterConfig(UtilConstants.LUCENE_VERSION, analyzer);
					iwc.setOpenMode((directory.listAll().length <= 0) ? OpenMode.CREATE : OpenMode.APPEND);

					try {
						IndexWriter writer = new IndexWriter(directory, iwc);
						for (File file : filelist) {
							try {
								JSONObject document = this.indexController.getPhysicalData(space, file.getName());
								if (document != null) {
									this.indexingDocument(writer, document);
								} else {
									log.warn("Message found in Space ["+space+"], but physical document ["+file.getName()+"] is disappeared.");
								}
							} catch (ParseException e) {
								e.printStackTrace();
							}
						}
						writer.commit();
						
						/*
						 * Prepare for index optimization 
						 */
						try {
							mqUtil.postMessageToTopic(UtilConstants.QUEUE_OPTIMIZE_WORKER, OptimizeWorker.getRequestMessage(accessCode, space));
							writer.forceMerge(UtilConstants.OPTIMIZATION_MAX_NUM_OF_SEGMENTS);
							mqUtil.postMessageToTopic(UtilConstants.QUEUE_OPTIMIZE_WORKER, OptimizeWorker.getReleaseMessage(accessCode, space));
						} catch (JMSException e1) {
							e1.printStackTrace();
						}
						
						writer.close();
						indexController.removeDocument(space, batchNumber);
						
					} catch (LockObtainFailedException e) {
						log.info("Unable to open IndexWriter: "+e.getMessage());
					}
					
				}
			}
			directory.close();
			
		}
	}
	
	private void indexingDocument(IndexWriter writer, JSONObject document) throws IOException {

		if (document != null) {

			Document luceneDocument = new Document();

			Hashtable<String, String> schema = CollectionHelper.getHashtable(document, UtilConstants.CONTENT_KEY_HEADER);
			Hashtable<String, String> body = CollectionHelper.getHashtable(document, UtilConstants.CONTENT_KEY_BODY);

			String fld = null;
			String text = null;
			for (String key : schema.keySet()) {
				String storeType = Store.NO.toString();

				String type = schema.get(key);
				String value = body.get(key);

				if (type != null && value != null) {
					if (type.equals(UtilConstants.TYPE_KEY_STORE) || type.equals(UtilConstants.TYPE_KEY_STORE_UNTOKENIZED)) {
						storeType = Store.YES.toString();
					}
					else if (type.equals(UtilConstants.TYPE_KEY_ID)) {
						storeType = Store.YES.toString();
						fld = key;
						text = body.get(key);

						// Index and Store the key of the key and the key as a value. 
						luceneDocument.add(new TextField(UtilConstants.TYPE_KEY_ID, key, Field.Store.YES));
						luceneDocument.add(new TextField(UtilConstants.TYPE_KEY_ID, value, Field.Store.YES));
					}
					else if (type.equals(UtilConstants.TYPE_KEY_INDEX_STORE) || type.equals(UtilConstants.TYPE_KEY_INDEX_STORE_UNTOKENIZED)) {
						storeType = Store.YES.toString();
					}
					else if (type.equals(UtilConstants.TYPE_KEY_INDEX) || type.equals(UtilConstants.TYPE_KEY_INDEX_UNTOKENIZED)) {
						storeType = Store.NO.toString();
					}
					TextField field = new TextField(key, value, Field.Store.valueOf(storeType));

					if (type.contains(UtilConstants.TYPE_KEY_UNTOKENIZED)) {
						field.setTokenStream(field.tokenStream(new SpecificKeywordAnalyzer(UtilConstants.LUCENE_VERSION)));
					}


					luceneDocument.add(field);
				} else {
					log.error("ERROR:\n\tFound type is ["+type+"] and value is ["+value+"].");
				}
			}
			if (fld!=null && text!=null) {
				if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
					writer.addDocument(luceneDocument);
				} else {
					writer.updateDocument(new Term(fld, text), luceneDocument);
				}
			}
		}
	}


}
