package com.graphconcern.cenozoic.datastore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;

/**
 * 
 *
 */
public class CassandraClient {

	private static final Logger log = LoggerFactory.getLogger(CassandraClient.class);
	private Session session_ = null;
	private String keySpace_ = null;
	private int port;
	private List<String> nodes;
	private Map<String, BoundStatement> boundCache = new ConcurrentHashMap<String, BoundStatement>();

	public CassandraClient(String serverNodeAddr, int port, String keySpace,
			int replicationFactor) {
		if (null == keySpace_) {
			keySpace_ = keySpace;
			// createKeyspace(keySpace_, replicationFactor);
		}
		String[] node_str = serverNodeAddr.split(",");
		nodes = new ArrayList<String>();
		for (String node : node_str){
			nodes.add(node);
		}
		this.port = port;

	}

	public CassandraClient(List<String> nodes, int port, String keySpace) {
		log.info("Starting Cassandra client....");
		this.port = port;
		Cluster.Builder builder = Cluster.builder().withPort(port);
		for (String node : nodes) {
			builder.addContactPoint(node);
		}
		Cluster cluster = builder.build();
		Metadata metadata = cluster.getMetadata();
		log.info("Connected to Cassandra cluster: " + metadata.getClusterName());

		for (Host host : metadata.getAllHosts()) {
			log.info("Datatacenter: " + host.getDatacenter() + ", Host: "
					+ host.getAddress() + ", Rack: " + host.getRack());
		}
		keySpace_ = keySpace;
		session_ = cluster.connect(keySpace);
		log.info("Cassandra client started.");
	}

	public void init() {
		log.info("Starting Cassandra client....");
		Cluster.Builder builder = Cluster.builder().withPort(port);
		for (String node : nodes) {
			builder.addContactPoint(node);
		}
		Cluster cluster = builder.build();
		session_ = cluster.connect(keySpace_);
		log.info("Cassandra client started.");
	}

	public void shutdown() {
		log.info("Stopping Cassandra client....");
        session_.getCluster().shutdown();
		log.info("Cassandra client stopped.");
	}

	public Session getSession() {
		if (null != keySpace_ && null != session_)
        {
            return session_;
        }
		throw new RuntimeException("Cassandra session not initialized");
	}

	public BoundStatement getBoundStatement(String operation) {
		if (boundCache.containsKey(operation)) {
			return boundCache.get(operation);
		}
		BoundStatement bndStmt = new BoundStatement(getSession().prepare(
				operation));
		boundCache.put(operation, bndStmt);
		return bndStmt;

	}

	public void createKeyspace(String keySpace, int replicationFactor) {
		try {
			getSession().execute("CREATE KEYSPACE "
									+ keySpace
									+ " WITH replication "
									+ "= {'class':'SimpleStrategy', 'replication_factor':"
									+ replicationFactor + "};");
		} catch (AlreadyExistsException e) {
			log.info("In CassandraClient.createKeyspace(): KeySpace '"
					+ keySpace + "' already exists.");
		}
	}

	public void execute(String operation) {
		try {
			log.info("Executing: {}", operation);
			getSession().execute(operation);
		} catch (Exception e) {
			log.error("Error in CassandraClient.execute(): "
					+ e.getLocalizedMessage());
		}
	}

	public ResultSet executeQuery(String query) {
		try {
			System.out.println(query);
			return getSession().execute(query);
		} catch (Exception e) {
			log.error("Error in CassandraClient.execute(): "
				+ e.getLocalizedMessage());
		}
		return null;
	}

	public ResultSetFuture executePreparedOpAsync(String operation,
			Object... bindVals) {
		try {
			return getSession().executeAsync(
				getBoundStatement(operation).bind(bindVals));
		} catch (Exception e) {
			log.error("Error in CassandraClient.executePreparedOpAsync(): "
				+ e.getLocalizedMessage());
		}
		return null;
	}

	public ResultSet executePreparedOp(String operation, Object... bindVals) {
		try {
			return getSession().execute(
				getBoundStatement(operation).bind(bindVals));
		} catch (Exception e) {
			log.error("Error in CassandraClient.executePreparedOp(): {}", e.getLocalizedMessage());
			throw new CassandraException(e);
		}
	}

	public ResultSet executedPreparedOpBatch(Map<String, Object[]> batchData) {
		try {
			BatchStatement batch = new BatchStatement();
			for (String operationName : batchData.keySet()) {
				batch.add(getBoundStatement(operationName).bind(
					batchData.get(operationName)));
			}
			return getSession().execute(batch);
		} catch (Exception e) {
			log.error("Error in CassandraClient.executePreparedOpBatch(): {}",
				e.getLocalizedMessage());
			throw new CassandraException(e);
		}
	}

	public class BatchOpTuple {
		private String Operation;
		private Object[] bindVals;

		public String getOperation() {
			return Operation;
		}

		public void setOperation(String operation) {
			Operation = operation;
		}

		public Object[] getBindVals() {
			return bindVals;
		}

		public void setBindVals(Object[] bindVals) {
			this.bindVals = bindVals;
		}
	}
	
	public void executedPreparedOps(List<BatchOpTuple> batchData) {
		log.info("In CassandraClient.executePreparedOps(): batchData.size={}", batchData.size());
		try {
			for (BatchOpTuple tuple : batchData) {
				executePreparedOp(tuple.getOperation(), tuple.getBindVals());
			}
		} catch (Exception e) {
			log.error("Error in CassandraClient.executePreparedOps(): {}",
				e.getLocalizedMessage());
			throw new CassandraException(e);
		}
	}

	public ResultSet executedPreparedOpBatch(List<BatchOpTuple> batchData) {
		log.info("In CassandraClient.executePreparedOpBatch(): batchData.size={}", batchData.size());
		try {
			BatchStatement batch = new BatchStatement();
			for (BatchOpTuple tuple : batchData) {
				batch.add(getBoundStatement(tuple.getOperation()).bind(
					tuple.getBindVals()));
			}
			return getSession().execute(batch);
		} catch (Exception e) {
			log.error("Error in CassandraClient.executePreparedOpBatch(): {}",
				e.getLocalizedMessage());
			throw new CassandraException(e);
		}
	}

	public void dropKeyspace() {
		try {
			getSession().execute("DROP KEYSPACE " + keySpace_);
			keySpace_ = null;
			session_ = null;
		} catch (Exception e) {
			log.error("Error in CassandraClient.dropKeyspace(): "
				+ e.getLocalizedMessage());
		}
	}

	public void executeBatch(List<String> statements) {
		StringBuffer buff = new StringBuffer();
		buff.append("Begin Batch\n");
		for (String stmt : statements) {
			buff.append(stmt).append("\n");
		}
		buff.append("Apply Batch;\n");
		String query = buff.toString();
		System.out.println(query);
		getSession().execute(query);
	}

	public String appendToSet(String columnFamily, String fieldName,
			Set<String> toAdd, Map<String, String> constraint) {
		return updateSet(columnFamily, fieldName, '+', toAdd, constraint);
	}

	public String removeFromSet(String columnFamily, String fieldName,
			Set<String> toRemove, Map<String, String> constraint) {
		return updateSet(columnFamily, fieldName, '-', toRemove, constraint);
	}

	public String updateSet(String columnFamily, String fieldName,
			char operation, Set<String> values, Map<String, String> constraint) {
		StringBuffer buff = new StringBuffer();
		buff.append("update ").append(columnFamily).append(" set ")
				.append(fieldName).append(" = ").append(fieldName).append(" ")
				.append(operation).append(" {");
		for (String val : values) {
			buff.append("'").append(val).append("',");
		}
		buff.deleteCharAt(buff.length() - 1);
		buff.append("} where ");
		for (String key : constraint.keySet()) {
			buff.append(key).append(" = '").append(constraint.get(key))
					.append("'").append(" and ");
		}
		buff.delete(buff.length() - " and ".length(), buff.length() - 1);
		buff.append(";");
		String query = buff.toString();
		System.out.println(query);
		return query;
	}
}
