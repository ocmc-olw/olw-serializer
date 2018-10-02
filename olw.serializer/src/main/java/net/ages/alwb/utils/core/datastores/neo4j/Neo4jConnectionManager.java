package net.ages.alwb.utils.core.datastores.neo4j;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.summary.Notification;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.SummaryCounters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.ocmc.ioc.liturgical.schemas.constants.HTTP_RESPONSE_CODES;
import org.ocmc.ioc.liturgical.schemas.constants.RELATIONSHIP_TYPES;
import org.ocmc.ioc.liturgical.schemas.constants.SCHEMA_CLASSES;
import org.ocmc.ioc.liturgical.schemas.constants.TOPICS;
import org.ocmc.ioc.liturgical.schemas.models.ModelHelpers;
import org.ocmc.ioc.liturgical.schemas.models.db.internal.LTKVJsonObject;
import org.ocmc.ioc.liturgical.schemas.models.db.stats.QueryStatistics;
import org.ocmc.ioc.liturgical.schemas.models.supers.LTKDb;
import org.ocmc.ioc.liturgical.schemas.models.ws.response.RequestStatus;
import org.ocmc.ioc.liturgical.schemas.models.ws.response.ResultJsonObjectArray;

import org.ocmc.ioc.liturgical.utils.ErrorUtils;

/**
 * Methods for interacting with a Neo4j database through the bolt interface
 * @author mac002
 *
 */
public class Neo4jConnectionManager implements LowLevelDataStoreInterface {
	private static final Logger logger = LoggerFactory
			.getLogger(Neo4jConnectionManager.class);

	 private static String macAddress = "unknown";
	 private static String hostName = "unknown";
	 private Gson gson = new GsonBuilder().disableHtmlEscaping().create();
	  private JsonParser parser = new JsonParser();
	  private String boltUrl = "bolt://127.0.0.1";
	  private String username = "";
	  private String password = "";
	  private Driver dbDriver = null;
	  private boolean connectionOK = false;
	  private boolean recordQueries = false;
	  private boolean readOnly = false;
	  private static String doc = "doc.";
	  private static String id = "id";
	  
	  public Neo4jConnectionManager(
			  String username
			  ,String password
			  , boolean readOnly
			  ) {
		  this.username = username;
		  this.password = password;
		  this.readOnly  = readOnly;
		  initializeDriver();
		  getMacAddress();
		  }

	  public Neo4jConnectionManager(
			  String boltUrl
			  , String username
			  , String password
			  , boolean readOnly
			  ) {
		  this.boltUrl = "bolt://"+boltUrl+":7687";
		  this.username = username;
		  this.password = password;
		  this.readOnly  = readOnly;
		  initializeDriver();
		  getMacAddress();
	  }
	  
	  public Neo4jConnectionManager(
			  String boltUrl
			  , String boltPort
			  , String username
			  , String password
			  , boolean readOnly
			  ) {
		  this.boltUrl = "bolt://"+boltUrl+":" + boltPort;
		  this.username = username;
		  this.password = password;
		  this.readOnly  = readOnly;
		  initializeDriver();
		  getMacAddress();
	  }
	  public void initializeDriver() {
		  try {
			  logger.info("Using " + boltUrl + " for external database...");
			  dbDriver = GraphDatabase.driver(boltUrl, AuthTokens.basic(username, password));
			  testConnection();
			  if (this.connectionOK) {
				  this.createIndex("Root", "library");
				  this.createIndex("Root", "topic");
				  this.createIndex("Root", "key");
				  this.createIndex("Root", "visibility");
				  this.createIndex("Root", "status");
			  }
		  } catch (Exception e) {
			  logger.error(e.getMessage());
			  e.printStackTrace();
		  }
	  }
	  
	  public ResultJsonObjectArray testConnection() {
		  ResultJsonObjectArray result = getResultObjectForQuery("match (n) return count(n) limit 10");
		  if (result.getValueCount() > 0) {
			  logger.info("Connection to Neo4j database is OK.");
			  this.connectionOK = true;
		  } else {
			  this.connectionOK = false;
			  logger.error("Can't connect to the Neo4j database.");
			  logger.error(result.getStatus().getUserMessage());
		  }
		  return result;
	  }

	  
	public ResultJsonObjectArray getForQuery(String query) {
			return getResultObjectForQuery(query);
	}

	/**
	 * If the result of the query contains records with an id and value,
	 * they will be converted to either LTKVString or LTKVJsonObject
	 * depending on whether the value is simple or complex.
	 * 
	 * Otherwise, it will return a JsonObject based on the record map.
	 * 
	 * @param query the query
	 * @return the result array
	 */
	  public ResultJsonObjectArray getResultObjectForQuery(
			  String query
			  ) {
			ResultJsonObjectArray result = new ResultJsonObjectArray(true);
			try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
				StatementResult neoResult = session.run(query);
				
				while (neoResult.hasNext()) {
					org.neo4j.driver.v1.Record record = neoResult.next();
						JsonObject o = parser.parse(gson.toJson(record.asMap())).getAsJsonObject();
						if (o.has("properties(link)")) {
							o = parser.parse(gson.toJson(record.get("properties(link)").asMap())).getAsJsonObject();
						} else if (o.has("properties(doc)")) {
							o = parser.parse(gson.toJson(record.get("properties(doc)").asMap())).getAsJsonObject();
						}
						result.addValue(o);
				}
				RequestStatus resultStatus = result.getStatus();
				result.setStatus(resultStatus);
			} catch (Exception e) {
				result.setStatusCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
				result.setStatusMessage(e.getMessage());
			}
			return result;
	}


	  protected void finalize() throws Throwable {
		  super.finalize();
		  dbDriver.close();
		}
	  
	public boolean isConnectionOK() {
		return connectionOK;
	}

	public void setConnectionOK(boolean connectionOK) {
		this.connectionOK = connectionOK;
	}
	
	/**
	 * The purpose of this method is to ensure that any node with 
	 * an id property has a unique constraint so that duplicate IDs 
	 * are not allowed.
	 * @param label
	 * @return
	 */
	private StatementResult setIdConstraint(String label) {
		StatementResult neoResult = null;
		String query = "create constraint on (p:" + label + ") assert p.id is unique"; 
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			neoResult = session.run(query);
		} catch (Exception e) {
			this.initializeDriver();
			ErrorUtils.report(logger, e);
		}
		return neoResult;
	}
	
	private StatementResult createIndex(String label, String prop) {
		StatementResult neoResult = null;
		StringBuffer sb = new StringBuffer();
		sb.append("create index on :");
		sb.append(label);
		sb.append("(");
		sb.append(prop);
		sb.append(")");
		String query = sb.toString(); 
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			neoResult = session.run(query);
		} catch (Exception e) {
			ErrorUtils.report(logger, e);
		}
		return neoResult;
	}

	private void setIdConstraints(List<String> labels) {
		try {
			for (String label : labels) {
				setIdConstraint(label); 
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public StatementResult setPropertyConstraint(String label, String property) {
		StatementResult neoResult = null;
		String query = "create constraint on (p:" + label + ") assert p." + property + " is unique"; 
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			neoResult = session.run(query);
		} catch (Exception e) {
			ErrorUtils.report(logger, e);
		}
		return neoResult;
	}

	@Override
	public RequestStatus insert(LTKVJsonObject doc) throws DbException {
		RequestStatus result = new RequestStatus();
		int count = 0;
		setIdConstraint(doc.getSchemaAsLabel());
		String query = "create (n:" + doc.getDelimitedLabels(":") + ") set n = {props} return n";
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			Map<String,Object> props = ModelHelpers.getAsPropertiesMap(doc);
			StatementResult neoResult = session.run(query, props);
			count = neoResult.consume().counters().nodesCreated();
			if (count > 0) {
		    	result.setCode(HTTP_RESPONSE_CODES.CREATED.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.CREATED.message + ": created " + doc.get_id());
			} else {
		    	result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message + "  " + doc.get_id());
			}
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		return result;
	}

	public RequestStatus insert(LTKDb doc) throws DbException {
		RequestStatus result = new RequestStatus();
		int count = 0;
		String query = "create (n:" + doc.fetchOntologyLabels() + ") set n = {props} return n";
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			Map<String,Object> props = ModelHelpers.getAsPropertiesMap(doc);
			StatementResult neoResult = session.run(query, props);
			count = neoResult.consume().counters().nodesCreated();
			if (count > 0) {
		    	result.setCode(HTTP_RESPONSE_CODES.CREATED.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.CREATED.message + ": created " + doc.getId());
				setIdConstraints(doc.fetchOntologyLabelsList());
			} else {
		    	result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message + "  " + doc.getId());
			}
		} catch (Exception e){
			if (e.getMessage().contains("already exists")) {
				result.setCode(HTTP_RESPONSE_CODES.CONFLICT.code);
				result.setDeveloperMessage(HTTP_RESPONSE_CODES.CONFLICT.message);
			} else {
				result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
				result.setDeveloperMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			}
			result.setUserMessage(e.getMessage());
		}
		return result;
	}

	private static void getMacAddress() {
		StringBuilder sb = new StringBuilder();
		try {
			InetAddress ip = InetAddress.getLocalHost();
			NetworkInterface network = NetworkInterface.getByInetAddress(ip);
			byte[] mac = network.getHardwareAddress();
			if (mac != null) {
				for (int i = 0; i < mac.length; i++) {
					sb.append(String.format("%02X%s", mac[i], (i < mac.length - 1) ? "-" : ""));
				}
			} else {
				sb.append("unknown mac address");
			}
			macAddress = sb.toString();
			hostName = ip.getCanonicalHostName();
		} catch (Exception e) {
			ErrorUtils.report(logger, e);
		}
	}
	

	@Override
	public void insert(List<JsonObject> docs) throws DbException {
		// TODO Need to implement
		for (JsonObject doc : docs) {
			insert(doc);
		}
	}

	/**
	 * Creates a relationship between two nodes, and adds the LTKDb
	 * properties to the relationship.
	 * 
	 * TODO: when creating relationships, the performance is 
	 * orders of magnitude better if you give use both the label and
	 * id of the nodes being connected.  This version only uses the IDs.
	 * In the future, need to add the labels.  
	 * 
	 
	 * @param fromId the from id
	 * @param doc the doc
	 * @param toId the to id
	 * @param type the type
	 * @return the request status
	 * @throws DbException about error in database
	 */
	public RequestStatus createRelationship(
			String fromId
			, LTKDb doc
			, String toId
			, RELATIONSHIP_TYPES type
			) throws DbException {
		RequestStatus result = new RequestStatus();
		setIdConstraint(doc.toSchemaAsLabel());
		String matchFrom = "MATCH (f:" + TOPICS.TEXT.label + ") where f.id = \'" 
				+ fromId 
				+ "\' match (t:" + TOPICS.ONTOLOGY_ROOT.label + ") where t.id = \'" 
				+ toId 
				+ "\'"
				;
        String queryCreate = " CREATE (f)-[r:" 
        		+ type.typename 
        		+ "]->(t) set r = {props} return r";
		String query = matchFrom + queryCreate;
		int count = 0;
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			Map<String,Object> props = ModelHelpers.getAsPropertiesMap(doc);
			StatementResult neoResult = session.run(query, props);
			count = neoResult.consume().counters().relationshipsCreated();
			if (count == 0) {
		    	result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message + "   " + doc.getId());
			} else {
		    	result.setCode(HTTP_RESPONSE_CODES.CREATED.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.CREATED.message + ": created " + doc.getId());
			}
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		return result;
	}

	/**
	 * Creates relationships of type 'type' between IDs specified in the pairs.
	 * The relationships do not have any properties.
	 * @param pairs the pairs
	 * @param type the type
	 * @return the request status
	 */
	public RequestStatus createRelationships(NodePairParameters pairs, String type) {
		RequestStatus result = new RequestStatus();
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			StringBuffer query = new StringBuffer();
			query.append("UNWIND {pairs} as pair ");
			query.append(" MATCH (a), (b)");
			query.append(" WHERE a.id = `pair.start` AND b.id = `pair.end`");
			query.append(" MERGE (a)-[:`");			
			query.append(type);			
			query.append("`]->(b)");			
			Map<String,Object> parms = new TreeMap<String,Object>();
			parms.put("pairs", pairs.toJsonObject().get("parameters").getAsJsonObject().get("pairs").getAsJsonArray().toString().toString());
			StatementResult neoResult = session.run(query.toString(), parms);
//			StatementResult neoResult = session.run(query.toString(), Values.parameters(pairs.toJsonString()));
			result.code = HTTP_RESPONSE_CODES.CREATED.code;
			result.userMessage = " " + neoResult.consume().counters().relationshipsCreated() + " relationships created";
		} catch (Exception e){
			result.code = HTTP_RESPONSE_CODES.BAD_REQUEST.code;
			result.userMessage = e.getMessage();
			ErrorUtils.report(logger, e);
		}
		return result;
	}

	public RequestStatus createRelationship(
			String fromId
			, String toId
			, String type
			) throws DbException {
		return runRelationshipQuery(
				this.relationshipQueryBuilder(
						fromId
						, ""
						, toId
						, ""
						, type
						, "CREATE"
						)
				);
	}

	public RequestStatus createRelationship(
			String fromId
			, String fromLabel
			, String toId
			, String toLabel
			, String type
			) throws DbException {
		return runRelationshipQuery(
				this.relationshipQueryBuilder(
						fromId
						, fromLabel
						, toId
						, toLabel
						, type
						, "CREATE"
						)
				);
	}

	public RequestStatus mergeRelationship(
			String fromId
			, String toId
			, String type
			) throws DbException {
		return runRelationshipQuery(
				this.relationshipQueryBuilder(
						fromId
						, ""
						, toId
						, ""
						, type
						, "MERGE"
						)
				);
	}

	public RequestStatus mergeWhereEqual(LTKDb doc) throws DbException {
		RequestStatus result = new RequestStatus();
		int count = 0;
		setIdConstraint(doc.toSchemaAsLabel());
		String query = 
				"match (n:" + TOPICS.ROOT.label + ") where n.id = \"" 
				+ doc.getId() 
		        + "\" on create set n = {props} "
		        + "\" on merge set n = {props} "
		        + "return count(n)";
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			Map<String,Object> props = ModelHelpers.getAsPropertiesMap(doc);
			StatementResult neoResult = session.run(query, props);
			count = neoResult.consume().counters().propertiesSet();
			if (count > 0) {
		    	result.setCode(HTTP_RESPONSE_CODES.OK.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.OK.message + ": updated " + doc.getId());
			} else {
		    	result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message + " " + doc.getId());
			}
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		return result;
	}

	public String relationshipQueryBuilder(
			String fromId
			, String fromLabel
			, String toId
			, String toLabel
			, String type
			, String verb
			) {
		StringBuffer sb = new StringBuffer();
		sb.append("MATCH (f:");
		sb.append(fromLabel);
		sb.append(") where f.id = \'"); 
		sb.append(fromId); 
		sb.append("\' match (t:");
		sb.append(toLabel);
		sb.append(") where t.id = \'" );
		sb.append(toId); 
		sb.append("\'");
		sb.append(" ");
		sb.append(verb);
		sb.append(" (f)-[r:"); 
        sb.append("`");
        sb.append(type); 
        sb.append("`");
        sb.append("]->(t) return r");
		return sb.toString();
	}
	public RequestStatus runRelationshipQuery(
			String query
			) throws DbException {
		RequestStatus result = new RequestStatus();
		result.setDeveloperMessage(query);
		int count = 0;
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			StatementResult neoResult = session.run(query);
			count = neoResult.consume().counters().relationshipsCreated();
			if (count == 0) {
		    	result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
		    	result.setUserMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			} else {
		    	result.setCode(HTTP_RESPONSE_CODES.CREATED.code);
		    	result.setUserMessage(HTTP_RESPONSE_CODES.CREATED.message);
			}
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setUserMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(query + " " + e.getMessage());
		}
		return result;
	}

	@Override
	public RequestStatus updateWhereEqual(LTKVJsonObject doc) throws DbException {
		RequestStatus result = new RequestStatus();
		int count = 0;
		setIdConstraint(doc.getSchemaAsLabel());
		String query = 
				"match (n) where n.id = \"" 
				+ doc.get_id() 
		        + "\" set n = {props} return count(n)";
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			Map<String,Object> props = ModelHelpers.getAsPropertiesMap(doc);
			StatementResult neoResult = session.run(query, props);
			count = neoResult.consume().counters().propertiesSet();
			if (count > 0) {
		    	result.setCode(HTTP_RESPONSE_CODES.OK.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.OK.message + ": updated " + doc.get_id());
			} else {
		    	result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message + " " + doc.get_id());
			}
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		return result;
	}


	public RequestStatus updateWhereEqual(LTKDb doc) throws DbException {
		RequestStatus result = new RequestStatus();
		int count = 0;
		setIdConstraint(doc.toSchemaAsLabel());
		String query = 
				"match (n:" + TOPICS.ROOT.label + ") where n.id = \"" 
				+ doc.getId() 
		        + "\" set n = {props} return count(n)";
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			Map<String,Object> props = ModelHelpers.getAsPropertiesMap(doc);
			StatementResult neoResult = session.run(query, props);
			count = neoResult.consume().counters().propertiesSet();
			if (count > 0) {
		    	result.setCode(HTTP_RESPONSE_CODES.OK.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.OK.message + ": updated " + doc.getId());
			} else {
		    	result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message + " " + doc.getId());
			}
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		return result;
	}

	public RequestStatus updateWhereEqual(String currentId, LTKDb doc) throws DbException {
		RequestStatus result = new RequestStatus();
		int count = 0;
		setIdConstraint(doc.toSchemaAsLabel());
		String query = 
				"match (n:" + TOPICS.ROOT.label + ") where n.id = \"" 
				+ currentId
		        + "\" set n = {props} return count(n)";
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			Map<String,Object> props = ModelHelpers.getAsPropertiesMap(doc);
			StatementResult neoResult = session.run(query, props);
			count = neoResult.consume().counters().propertiesSet();
			if (count > 0) {
		    	result.setCode(HTTP_RESPONSE_CODES.OK.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.OK.message + ": updated " + doc.getId());
			} else {
		    	result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message + " " + doc.getId());
			}
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		return result;
	}
	public RequestStatus updateWhereRelationshipEqual(LTKDb doc) throws DbException {
		RequestStatus result = new RequestStatus();
		int count = 0;
		setIdConstraint(doc.toSchemaAsLabel());
		String query = 
				"match ()-[r]->() where r.id = \"" 
				+ doc.getId() 
		        + "\" set r = {props} return count(r)";
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			Map<String,Object> props = ModelHelpers.getAsPropertiesMap(doc);
			StatementResult neoResult = session.run(query, props);
			count = neoResult.consume().counters().propertiesSet();
			if (count > 0) {
		    	result.setCode(HTTP_RESPONSE_CODES.OK.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.OK.message + ": updated " + doc.getId());
			} else {
		    	result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message + " " + doc.getId());
			}
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		return result;
	}

	@Override
	public void insert(JsonObject doc) throws DbException {
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			JsonObject theProps = doc.get("doc").getAsJsonObject().get("properties").getAsJsonObject();
			JsonArray theLabels = doc.get("doc").getAsJsonObject().get("labels").getAsJsonArray();
			Map<String,Object> propMap = ModelHelpers.toHashMap(theProps);
			Map<String,Object> props = new TreeMap<String,Object>();
			props.put("props", propMap);
			String query = "create (n" + this.createLabels(theLabels) + ") set n = {props} return n";
			StatementResult neoResult = session.run(query, props);
		} catch (Exception e){
			throw e;
		}
	}

	@Override
	public RequestStatus deleteNodeWhereEqual(String id) throws DbException {
		RequestStatus result = new RequestStatus();
		int count = 0;
		String query = 
				"match (n) where n.id = \"" 
				+ id 
		        + "\" delete n";
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			StatementResult neoResult = session.run(query);
			count = neoResult.consume().counters().nodesDeleted();
			if (count > 0) {
		    	result.setCode(HTTP_RESPONSE_CODES.OK.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.OK.message + ": deleted " + id);
			} else {
		    	result.setCode(HTTP_RESPONSE_CODES.NOT_FOUND.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.NOT_FOUND.message + " " + id);
			}
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		return result;
	}

	
	
	public RequestStatus recordSummary(ResultSummary summary, RequestStatus status) {
		try {
			SummaryCounters countersSummary = summary.counters();
			status.constraintsAdded = countersSummary.constraintsAdded();
			status.counterTotal = status.counterTotal + status.constraintsAdded;
			status.constraintsRemoved = countersSummary.constraintsRemoved();
			status.counterTotal = status.counterTotal + status.constraintsRemoved;
			status.indexesAdded = countersSummary.indexesAdded();
			status.counterTotal = status.counterTotal + status.indexesAdded;
			status.indexesRemoved = countersSummary.indexesRemoved();
			status.counterTotal = status.counterTotal + status.indexesRemoved;
			status.labelsAdded = countersSummary.labelsAdded();
			status.counterTotal = status.counterTotal + status.labelsAdded;
			status.labelsRemoved = countersSummary.labelsRemoved();
			status.counterTotal = status.counterTotal + status.labelsRemoved;
			status.nodesCreated = countersSummary.nodesCreated();
			status.counterTotal = status.counterTotal + status.nodesCreated;
			status.nodesDeleted = countersSummary.nodesDeleted();
			status.counterTotal = status.counterTotal + status.nodesDeleted;
			status.propertiesSet = countersSummary.propertiesSet();
			status.counterTotal = status.counterTotal + status.propertiesSet;
			status.relationshipsCreated = countersSummary.relationshipsCreated();
			status.counterTotal = status.counterTotal + status.relationshipsCreated;
			status.relationshipsDeleted = countersSummary.relationshipsDeleted();
			status.counterTotal = status.counterTotal + status.relationshipsDeleted;
			status.containsUpdates = countersSummary.containsUpdates();
			List<Notification> notices = summary.notifications();
			for (Notification n : notices) {
				status.notifications.add(n.code() + ": " + n.title() + " - " + n.description() + " | ");
			}
		} catch (Exception e) {
			ErrorUtils.report(logger, e);
		}
		return status;
	}


	public RequestStatus deleteNodeWhereEqual(String id, String label) throws DbException {
		RequestStatus result = new RequestStatus();
		int count = 0;
		String query = 
				"match (n:" + label + ") where n.id = \"" 
				+ id 
		        + "\" delete n return count(n)";
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			StatementResult neoResult = session.run(query);
			count = neoResult.consume().counters().nodesDeleted();
			if (count > 0) {
		    	result.setCode(HTTP_RESPONSE_CODES.OK.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.OK.message + ": deleted " + id);
			} else {
		    	result.setCode(HTTP_RESPONSE_CODES.NOT_FOUND.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.NOT_FOUND.message + " " + id);
			}
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		return result;
	}


	public String createLabels(JsonArray labels) {
		StringBuffer sb = new StringBuffer();
		for (JsonElement label : labels) {
			String strLabel = label.getAsString();
			sb.append(":");
			sb.append("`");
			sb.append(strLabel);
			sb.append("`");
		}
		return sb.toString();
	}

	/**
	 * Delete a relationship where the relationship itself has an ID
	 * @param id of relationship
	 * @return the request status
	 * @throws DbException about error in the database
	 */
	public RequestStatus deleteRelationshipWhereEqual(String id) throws DbException {
		RequestStatus result = new RequestStatus();
		int count = 0;
		String query = 
				"match ()-[r]->() where r.id = \"" 
				+ id 
		        + "\" delete r";
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			StatementResult neoResult = session.run(query);
			count = neoResult.consume().counters().relationshipsDeleted();
			if (count > 0) {
		    	result.setCode(HTTP_RESPONSE_CODES.OK.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.OK.message + ": deleted " + id);
			} else {
		    	result.setCode(HTTP_RESPONSE_CODES.NOT_FOUND.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.NOT_FOUND.message + " " + id);
			}
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		return result;
	}
	
	/**
	 * Deletes the node matching the specified ID and all its relationships.
	 * @param id the id
	 * @return the request status
	 * @throws DbException about error in the database
	 */
	public RequestStatus deleteNodeAndRelationshipsForId(
			String id
			) throws DbException {
		RequestStatus result = new RequestStatus();
		int count = 0;
		String query = 
				"match ()-[r]->(t) where t.id = \"" 
				+ id 
		        + "\" delete t, r";
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			StatementResult neoResult = session.run(query);
			count = neoResult.consume().counters().relationshipsDeleted();
			if (count > 0) {
		    	result.setCode(HTTP_RESPONSE_CODES.OK.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.OK.message + ": deleted " + id);
			} else {
		    	result.setCode(HTTP_RESPONSE_CODES.NOT_FOUND.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.NOT_FOUND.message + " " + id);
			}
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		return result;
	}

	public RequestStatus processConstraintQuery(String query) {
		RequestStatus result = new RequestStatus();
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			StatementResult neoResult = session.run(query);
			int count = 0;
			if (query.toLowerCase().contains("create constraint")) {
				count = neoResult.consume().counters().constraintsAdded();
			} else {
				count = neoResult.consume().counters().constraintsRemoved();
			}
			if (count < 1) {
		    	result.setCode(HTTP_RESPONSE_CODES.CONFLICT.code);
		    	result.setMessage(HTTP_RESPONSE_CODES.CONFLICT.message);
			}
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		logger.info(query + ": " + result.getCode());
		return result;
	}

	public boolean isRecordQueries() {
		return recordQueries;
	}

	public void setRecordQueries(boolean recordQueries) {
		this.recordQueries = recordQueries;
	}
	
	public RequestStatus dropAllConstraints() {
		RequestStatus result = new RequestStatus();
		String query = "call db.constraints()";
		int count = 0;
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			ResultJsonObjectArray s = getForQuery(query);
			for (JsonObject o : s.getValues()) {
				String c = o.get("description").getAsString();
				ResultJsonObjectArray drop = getForQuery("drop " + c);
				logger.info(drop.getStatus().code + " for drop " + c);
				StatementResult neoResult = session.run(query);
				count = count + neoResult.consume().counters().constraintsRemoved();
			}
			result.setMessage(count + " constraints dropped");
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		return result;
	}

	public RequestStatus addRootLabel() {
		RequestStatus result = new RequestStatus();
		this.dropConstraintOnId(TOPICS.ROOT.label);
		String query = "match (n) where 'id' in keys(n) set n:" + TOPICS.ROOT.label + " return count(n)";
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			ResultJsonObjectArray s = getForQuery(query);
			StatementResult sResult = this.setIdConstraint(TOPICS.ROOT.label);
			result.setMessage(TOPICS.ROOT.label + " added to " + s.getValueCount() + " nodes");
			query = "match (n:TablesRoot)  set n:" + TOPICS.ROOT.label + " return count(n)";
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		return result;
	}

	public RequestStatus renameLabel(String from, String to) {
		RequestStatus result = new RequestStatus();
		String query = "match (n:" + from + ") remove n:" + from + " set n:" + to + " return count(n)";
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			ResultJsonObjectArray s = getForQuery(query);
			this.dropConstraintOnId(from);
			this.setIdConstraint(to);
			String message = "Changed label " 
					+ from 
					+ " to " 
					+ to 
					+ " for " 
					+ s.getValueCount() 
					+ " nodes. And, dropped constraint for old label and added one for the new label."
					;
			s.status.setMessage(message);
			logger.info(message);
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		return result;
	}

	/**
	 * Creates a constraint on the ID property of all topics in TOPICS enum
	 * @return the request status
	 */
	public RequestStatus addConstraintsForAllTopics() {
		RequestStatus result = new RequestStatus();
		try  {
			for (TOPICS t : TOPICS.values()) {
				this.setIdConstraint(t.label);
			}
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		return result;
	}

	/**
	 * Removes the specified label from nodes in the database
	 * and removes the unique constraint for its ID property (if exists)
	 * @param label the label
	 * @return the request status
	 */
	public RequestStatus removeLabel(String label) {
		RequestStatus result = new RequestStatus();
		String query = "match (n:" + label + ") remove n:" + label + " return count(n)";
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			ResultJsonObjectArray s = getForQuery(query);
			this.dropConstraintOnId(label);
			result.setMessage("Removed label " + label + " and constraint on id if existed");
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		return result;
	}

	public RequestStatus dropConstraintOnId(String label) {
		return dropConstraint(label,"id");
	}
	
	public RequestStatus dropConstraint(String label, String property) {
		RequestStatus result = new RequestStatus();
		String query = "DROP CONSTRAINT ON ( n:" + label + " ) ASSERT n." + property + " IS UNIQUE";
		int count = 0;
		try (org.neo4j.driver.v1.Session session = dbDriver.session()) {
			ResultJsonObjectArray s = getForQuery(query);
			result.setMessage("Dropped constraint on " + label + " for property " + property);
		} catch (Exception e){
			result.setCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
			result.setMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message);
			result.setDeveloperMessage(e.getMessage());
		}
		logger.info(result.getCode() + " " + result.getDeveloperMessage());
		return result;
	}


}
