package org.ocmc.olw.serializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.ocmc.ioc.liturgical.schemas.id.managers.IdManager;
import org.ocmc.ioc.liturgical.schemas.models.ws.response.ResultJsonObjectArray;
import org.ocmc.ioc.liturgical.utils.ErrorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

import net.ages.alwb.utils.core.datastores.neo4j.Neo4jConnectionManager;

/**
 * Serializes Neo4j nodes and relationships to Json files
 * and commits them to gitlab.liml.org
 * 
 * @author mac002
 *
 */
public class Serializer implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(Serializer.class);
	String user = null;
	String pwd = null;
	String url = null;
	String whereClause = "";
	String whereLibraries = "";
	boolean debugEnabled = false;
	File gitFolder = null;
	String gitPath = "";
	String repoToken = "";
	String repoUser = "";
	String repoDomain = "";
	private Neo4jConnectionManager dbms = null;
	GitlabUtils gitUtils = null;

	public Serializer(
			String user
			, String pwd
			, String url
			, String whereClause
			, String whereLibraries
			, String repoDomain
			, String repoUser
			, String repoToken
			, boolean debugEnabled
			) {
		this.user = user;
		this.pwd = pwd;
		this.url = url;
		this.gitPath = Constants.GIT_FOLDER;
		this.gitUtils = new GitlabUtils(repoDomain,repoToken);
		this.whereClause = whereClause;
		this.whereLibraries = whereLibraries;
		this.repoDomain = repoDomain;
		this.repoUser = repoUser;
		this.repoToken = repoToken;
		this.debugEnabled = debugEnabled;
	}
	
	@Override
	public void run() {
		Instant start = Instant.now();
		String startTime = Instant.now().toString();
		this.sendMessage("Serializing Neo4j to Json");
		 dbms = new Neo4jConnectionManager(
				  url
				  , user
				  , pwd
				  , false
				  );
		List<String> projects = new ArrayList<String>();
		projects.add("db2json");
		projects.add("db2ares");
		this.gitUtils.pullAllProjects(this.gitPath, projects);
		this.writeNodes2Json();
		this.writeLinks2Json();
		this.writeLinkProps2Json();
		this.writeAges();
		Instant finish = Instant.now();
		long timeElapsed = Duration.between(start, finish).toHours();
		String elapsedMsg = "";
		if (timeElapsed < 1) {
			timeElapsed = Duration.between(start, finish).toMinutes();
			elapsedMsg = ".Elapsed.minutes=" + timeElapsed;
		} else {
			elapsedMsg = ".Elapsed.hours=" + timeElapsed;
		}
		String pushResult = this.gitUtils.addCommitPushAllProjects(this.gitPath, "Serializer:" + startTime + "." + elapsedMsg);
		if (this.debugEnabled) {
			System.out.println(pushResult);
		}
		this.sendMessage(startTime + " started  serializing Neo4j.");
		this.sendMessage(Instant.now().toString() + " finished serializing Neo4j." + elapsedMsg);
	}
	
	private void sendMessage(String m) {
		String msg = Instant.now().toString() + " " + m;
		logger.info(msg);
		SerializerApp.sendMessage(m);
	}

	/**
	 * Writes nodes for each library~topic to a single json file
	 * for that topic
	 */
	private synchronized void writeNodes2Json() {
		 	logger.info(Instant.now().toString() + " Writing Nodes to Json");
		    StringBuffer sb = new StringBuffer();
			sb.append("match (n:Root)");
			if (this.whereClause.length() > 0) {
				if (this.whereLibraries.length() > 0) {
					if (this.whereClause.equals("WHERE")) {
						sb.append(" where n.library in [");
					} else if (this.whereClause.equals("WHERE_NOT")) {
						sb.append(" where not n.library in [");
					}
			    	String [] parts = this.whereLibraries.split(",");
			    	StringBuffer sbLibs = new StringBuffer();
			    	for (String p : parts) {
				        if (sbLibs.length() > 0) {
				        	sbLibs.append(", ");
				        }
				        sbLibs.append("'");
				        sbLibs.append(p.trim());
				        sbLibs.append("'");
			    	}
			    	sb.append(sbLibs.toString());
			    	sb.append("]");
				}
			sb.append(" return distinct n.library + '|' + n.topic as libTopic order by libTopic;");
			ResultJsonObjectArray result = dbms.getForQuery(sb.toString());
			if (this.debugEnabled) {
				logger.info(Integer.toString(result.status.code));
				logger.info(result.status.developerMessage);
				logger.info(Long.toString(result.valueCount));
			}
			long processed = 0;
			long total = result.valueCount;
			
			for (JsonObject o : result.getValues()) {
				try {
					if (o != null && o.has("libTopic"))  {
						String libTopic = o.get("libTopic").getAsString();
						String [] parts = libTopic.split("\\|");
						String library = "";
						String topic = "";
						if (parts.length == 2) {
							library = parts[0];
							topic = parts[1];
						}
						String topicQuery = "match (n:Root) where n.id starts with '" + library + "~" + topic + "~" + "' return properties(n) as props;";
						ResultJsonObjectArray topics = dbms.getForQuery(topicQuery);
						if (topics.valueCount > 0) {
							String path = "";
							if (library.equals("en_sys_linguistics")) {
								if (topic.toLowerCase().contains("wordinflected")) {
									path = "WordInflected/" + topic + ".json";
								} else if (topic.toLowerCase().contains("perseus") || topic.startsWith("urn")) {
									topic = topic.toLowerCase();
									topic = topic.replaceAll(":", "/");
									topic = topic.replaceAll("\\.", "/");
									topic = topic.replaceAll("~", "/");
									if (topic.startsWith("tlg")) {
										topic = "perseus/" + topic;
									}
									path = topic + ".json";
								} else if (topic.contains("~")) {
									topic = topic.replaceAll("~", "/");
									path = topic  + ".json";
								} else {
									if (topic.length() > 0) {
										if (topic.length() > 3) {
											path = "tokens/"  + topic.substring(0, 1) + "/" + topic.substring(0, 2) + "/"  + topic.substring(0, 3) + "/" + topic + ".json";
										} else if (topic.length() > 2) {
											path = "tokens/"  + topic.substring(0, 1) + "/" + topic.substring(0, 2) + "/" + topic + ".json";
										} else {
											path = "tokens/" + topic.substring(0, 1) + "/" + topic + ".json";
										}
									} else {
										path = "tokens" + "/" + topic + ".json";
									}
								}
							} else {
								topic = topic.toLowerCase();
								topic = topic.replaceAll(":", "/");
								topic = topic.replaceAll("\\.", "/");
								topic = topic.replaceAll("~", "/");
								path =  topic + ".json";
							}
							processed++;
							String out = this.gitPath + Constants.PROJECT_DB2JSON_NODES + "/" + library + "/" + path;
							FileUtils.write(new File(out),topics.getValuesAsJsonArray().toString());
							if (this.debugEnabled) {
								logger.info(Instant.now().toString() + " " + processed + " of " + total + " - " + out);
							}
						}
						String pushResult = this.gitUtils.addCommitPushAllProjects(this.gitPath, "Serializer:db2json:nodes:" + library + "." + Instant.now().toString());
					}
				} catch (Exception e) {
					System.out.println(o.toString());
					e.printStackTrace();
				}
			}
		 	logger.info(Instant.now().toString() + " finished writing Json");
		}

	}
	
	
	/**
	 * Writes links to a single json file
	 * for that topic
	 */
	private synchronized void writeLinks2Json() {
		 	logger.info(Instant.now().toString() + " Writing Links to Json");
		    StringBuffer sb = new StringBuffer();
			sb.append("match (f:Root)-[r]->(t:Root)");
			if (this.whereClause.length() > 0) {
				if (this.whereLibraries.length() > 0) {
					if (this.whereClause.equals("WHERE")) {
						sb.append(" where f.library in [");
					} else if (this.whereClause.equals("WHERE_NOT")) {
						sb.append(" where not f.library in [");
					}
			    	String [] parts = this.whereLibraries.split(",");
			    	StringBuffer sbLibs = new StringBuffer();
			    	for (String p : parts) {
				        if (sbLibs.length() > 0) {
				        	sbLibs.append(", ");
				        }
				        sbLibs.append("'");
				        sbLibs.append(p.trim());
				        sbLibs.append("'");
			    	}
			    	sb.append(sbLibs.toString());
			    	sb.append("]");
				}
			}
			sb.append(" return distinct f.library + '|' + type(r) as libType order by libType;");
			ResultJsonObjectArray libTypes = dbms.getForQuery(sb.toString());
			for (JsonObject libType : libTypes.getValues()) {
				String [] parts = libType.get("libType").getAsString().split("\\|");
				if (parts.length == 2) {
					StringBuffer ltSb = new StringBuffer();
					ltSb.append("match (f:Root)-[r:");
					ltSb.append(parts[1]);
					ltSb.append("]->(t:Root) where f.id starts with '");
					ltSb.append(parts[0]);
					ltSb.append("' return f.id as from, type(r) as type, t.id as to order by f.id;");
					ResultJsonObjectArray result = dbms.getForQuery(ltSb.toString());
					String out = this.gitPath + Constants.PROJECT_DB2JSON_LINKS + "/" + parts[0] + "/" + parts[1] + ".json";
					try {
						FileUtils.write(new File(out),result.getValuesAsJsonArray().toString());
					} catch (Exception e) {
						ErrorUtils.report(logger, e);
					}
				}
				String pushResult = this.gitUtils.addCommitPushAllProjects(this.gitPath, "Serializer:db2json:links:" + libType + "." + Instant.now().toString());
			}
			
	}
	
	/**
	 * Writes properties of links to a single json file
	 * for that topic
	 */
	private synchronized void writeLinkProps2Json() {
		 	logger.info(Instant.now().toString() + " Writing Link Properties to Json");
			String typesQuery = "match (f:Root)-[r]->(t:Root) where type(r) starts with 'REFERS_TO' return distinct type(r) order by type(r)";
			ResultJsonObjectArray tqResult = dbms.getForQuery(typesQuery);
			for (JsonObject o : tqResult.getValues()) {
				try {
					if (o != null && o.has("type(r)"))  {
						String type = o.get("type(r)").getAsString();
						StringBuffer sb = new StringBuffer();
						sb.append("match (f:Root)-[r:");
						sb.append(type);
						sb.append("]->(t:Root) return properties(r) as props order by r.id;");
						ResultJsonObjectArray result = dbms.getForQuery(sb.toString());
						for (JsonObject p : result.getValues()) {
							if (p != null && p.has("props"))  {
								JsonObject props = p.get("props").getAsJsonObject();
								String out = this.gitPath + Constants.PROJECT_DB2JSON_LINK_NODES + "/" + type + ".json";
								FileUtils.write(new File(out),props.toString());
							}
						}
						String pushResult = this.gitUtils.addCommitPushAllProjects(this.gitPath, "Serializer:db2json:linkprops:" + type + "." + Instant.now().toString());
					}
				} catch (Exception e) {
					ErrorUtils.report(logger, e);
				}
			}
	}

	/**
	 * Reads the serailized Json files and converts
	 * AGES libraries to ares format and writes
	 * the ares files
	 */
	private void writeAges() {
	 	logger.info(Instant.now().toString() + " Writing AGES ares files");
		try {
			String lQ = "match (n:Liturgical) return distinct n.library as library order by n.library;";
			ResultJsonObjectArray lQr = dbms.getForQuery(lQ);
			if (this.debugEnabled) {
				logger.info(Integer.toString(lQr.status.code));
				logger.info(lQr.status.developerMessage);
				logger.info(Long.toString(lQr.valueCount));
			}
			long processed = 0;
			long total = lQr.valueCount;
			
			for (JsonObject libO : lQr.getValues()) {
				StringBuffer tQsb = new StringBuffer();
				if (libO != null && libO.has("library")) {
					String library = libO.get("library").getAsString();
					String [] libParts = library.split("_");
					String  agesDomain = "_" + libParts[0] + "_" + libParts[1].toUpperCase() + "_" + libParts[2]; 
					logger.info(Instant.now().toString() + " processing library " + library);
					tQsb.append("match (n:Liturgical) where n.id starts with '");
					tQsb.append(library);
					tQsb.append("' return distinct n.topic as topic order by n.topic;");
					ResultJsonObjectArray tQr = dbms.getForQuery(tQsb.toString());

					for (JsonObject topicO : tQr.getValues()) {
						
						String topic = topicO.get("topic").getAsString();
						String agesResource = topic + agesDomain;
						logger.info(Instant.now().toString() + " processing library/topic " + library + "~" + topic);
						StringBuffer vQsb = new StringBuffer();
						vQsb.append("match (n:Liturgical) where n.id starts with '");
						vQsb.append(library);
						vQsb.append("~");
						vQsb.append(topic);
						vQsb.append("' return n.key as key, n.value as value, n.redirectId as redirectId order by n.id;");

						StringBuffer sb = new StringBuffer();
						sb.append("A_Resource_Whose_Name = ");
						sb.append(agesResource);
						sb.append("\n\n");
						ResultJsonObjectArray vQr = dbms.getForQuery(vQsb.toString());
						for (JsonObject vO : vQr.getValues()) {
							sb.append(vO.get("key").getAsString());
							sb.append(" = ");
							String value = "";
							String redirect = "";
							if (vO.has("value")) {
								value = vO.get("value").getAsString();
							} else if (vO.has("redirectId")) {
								redirect = vO.get("redirect").getAsString();
							}
							if (value.length() > 0) {
								sb.append(LibraryUtils.wrapQuotes(value));
							} else if (redirect.length() > 0) {
								sb.append(redirect);
							}
							sb.append("\n");
							String topicPath = topic.replaceAll("_", "/");
							topicPath = topicPath.replaceAll("\\.", "/");
							String out = this.gitPath + Constants.PROJECT_DB2ARES + "/" + library + "/" + topicPath + "/" + agesResource + ".ares";
							FileUtils.write(new File(out), sb.toString());
							if (this.debugEnabled) {
								logger.info(Instant.now().toString() + " wrote " + out);
							}
						}
					}
					String pushResult = this.gitUtils.addCommitPushAllProjects(this.gitPath, "Serializer:db2ares:" + library + "." + Instant.now().toString());
					processed++;
					logger.info(Instant.now().toString() + " processed " + library + " " + processed + " of " + total + " libraries.");
				}
			}
				
		} catch (Exception e) {
			ErrorUtils.report(logger, e);
		}
	}
	
}
