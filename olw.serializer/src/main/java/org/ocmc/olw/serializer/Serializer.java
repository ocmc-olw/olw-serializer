package org.ocmc.olw.serializer;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
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
	boolean reinit = false;
	File gitFolder = null;
	String gitPath = "";
	String repoToken = "";
	String repoUser = "";
	String repoDomain = "";
	private Neo4jConnectionManager dbms = null;
	private GitlabUtils gitUtils = null;
	private List<String> librariesList = new ArrayList<String>();

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
			, boolean reinit
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
		this.reinit = reinit;
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
		 this.createLibrariesList();

		 int total = this.librariesList.size();
		 int processed = 0;
		 
		 for (String library : this.librariesList) {
			Instant libStart = Instant.now();
			processed++;

			// the work occurs in this procedure
			this.processLibrary(library, processed, total);

			this.sendMessage(processed + " of " + total + " Neo4j to " + library + "." + this.getElapsedMessage(libStart));
		 }
		this.sendMessage(startTime + " started  serializing Neo4j.");
		this.sendMessage(Instant.now().toString() + " finished serializing Neo4j." + this.getElapsedMessage(start));
	}
	
	private String getElapsedMessage(Instant start) {
		Instant finish = Instant.now();
		long timeElapsed = Duration.between(start, finish).toHours();
		String elapsedMsg = "";
		if (timeElapsed < 1) {
			timeElapsed = Duration.between(start, finish).toMinutes();
			if (timeElapsed < 1) {
				timeElapsed = Duration.between(start, finish).getSeconds();
				elapsedMsg = ".Elapsed.seconds=" + timeElapsed;
			} else {
				elapsedMsg = ".Elapsed.minutes=" + timeElapsed;
			}
		} else {
			elapsedMsg = ".Elapsed.hours=" + timeElapsed;
		}
		return elapsedMsg;
	}
	private void sendMessage(String m) {
		String msg = Instant.now().toString() + " " + m;
		logger.info(msg);
		SerializerApp.sendMessage(m);
	}

	private void processLibrary(String library, int libNbr, int totalLibs) {
//		this.writeNodes2Json();
		this.writeLinks2Json(Constants.PROJECT_DB2JSON_LINKS, library, libNbr, totalLibs);
//		this.writeLinkProps2Json();
		this.writeAges(Constants.PROJECT_DB2ARES , library, libNbr, totalLibs);
	}
	/**
	 * If libraries were specified for either inclusion or exclusion
	 * in the environmental properties, we will use them.
	 * 
	 * Otherwise we will create the list by querying the database.
	 * 
	 * This list will exclude en_sys_linguistics.  It will be handled separately
	 * as a special case.
	 */
	private void createLibrariesList() {
	    StringBuffer sb = new StringBuffer();
		sb.append("match (n:Root)");
		if (this.whereClause.length() > 0 && this.whereLibraries.length() > 0) {
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
		    sb.append(" and not n.library = 'en_sys_linguistics' return distinct n.library as lib order by lib;");
		} else {
			sb.append("match (n:Root) where not n.library = 'en_sys_linguistics' return distinct n.library as lib");
		}
		
		ResultJsonObjectArray result = dbms.getForQuery(sb.toString());
		for (JsonObject o : result.getValues()) {
			try {
				if (o != null && o.has("lib"))  {
					this.librariesList.add(o.get("lib").getAsString());
				}
			} catch (Exception e) {
				ErrorUtils.report(logger, e);
			}
		}
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
							GitlabUtils.gitAdd(this.gitPath, Constants.PROJECT_DB2JSON_NODES, out);
							if (this.debugEnabled) {
								logger.info(Instant.now().toString() + " " + processed + " of " + total + " - " + out);
							}
						}
						String pushResult = this.gitUtils.commitPushAllProjects(this.gitPath, "Serializer:db2json:nodes:" + library + "." + Instant.now().toString());
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
	private synchronized void writeLinks2Json(String groupPath, String library, int libNbr, int totalLibs) {
		Instant start = Instant.now();
		if (this.debugEnabled) {
			logger.info(Instant.now().toString() + " db2json links procssing " + library);
		}
		this.preProcessLibrary(groupPath, library);
		StringBuffer sb = new StringBuffer();
		sb.append("match (f:Root)-[r]->(:Root) where f.library = '");
		sb.append(library);
		sb.append("' return distinct f.topic + '|' + type(r) as libType order by libType;");
		ResultJsonObjectArray libTypes = dbms.getForQuery(sb.toString());
		for (JsonObject libType : libTypes.getValues()) {
			String [] parts = libType.get("libType").getAsString().split("\\|");
			if (parts.length == 2) {
				String topic = parts[0];
				String type = parts[1];
				StringBuffer ltSb = new StringBuffer();
				ltSb.append("match (f:Root)-[r:");
				ltSb.append(type);
				ltSb.append("]->(t:Root) where f.id starts with  '");
				ltSb.append(library);
				ltSb.append("~");
				ltSb.append(topic);
				ltSb.append("' return f.id as from, type(r) as type, t.id as to order by f.id;");
				String topicPath = topic.replaceAll("\\.", "/");
				topicPath = topicPath.replaceAll("-", "/");
				topicPath = topicPath.replaceAll("~", "/");
				ResultJsonObjectArray result = dbms.getForQuery(ltSb.toString());
				String out = this.gitPath 
						+ groupPath 
						+ "/" + library 
						+ "/" + topicPath 
						+ "/" + topic + ".json";
				try {
					FileUtils.write(new File(out),result.getValuesAsJsonArray().toString());
				} catch (Exception e) {
					ErrorUtils.report(logger, e);
				}
			} else {
				logger.error("Unexpected libType parts length for " + libType);
			}
		}
		GitlabUtils.gitAddCommitPush(this.gitPath + groupPath, "olwsys", library, ".", Instant.now().toString() + ".Serialized." + this.getElapsedMessage(start));
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
								String out = this.gitPath + Constants.PROJECT_DB2JSON_LINK_PROPS + "/" + type + ".json";
								FileUtils.write(new File(out),props.toString());
								GitlabUtils.gitAdd(this.gitPath, Constants.PROJECT_DB2JSON_LINK_PROPS, out);
							}
						}
						String pushResult = this.gitUtils.commitPushAllProjects(this.gitPath, "Serializer:db2json:linkprops:" + type + "." + Instant.now().toString());
					}
				} catch (Exception e) {
					ErrorUtils.report(logger, e);
				}
			}
	}

	private void preProcessLibrary(String group, String library) {
		String fullPath = group + "/" + library;
		File f = new File(this.gitPath + fullPath);
		if (this.reinit) {
			// delete local repo
			if (f.exists()) {
				try {
					FileUtils.deleteDirectory(f);
				} catch (IOException e) {
					ErrorUtils.report(logger, e);
				}
			}
			// delete cloud repo
			if (this.gitUtils.existsProjectOnServer(fullPath)) {
				this.gitUtils.deleteProject(fullPath);
				try {
					Thread.sleep(3000); // give the server time to process the delete before we attempt to recreate the project
				} catch (InterruptedException e) {
				}
			}
		}
		if (f.exists()) {
			GitlabUtils.pullGitlabProject(this.gitPath + group, this.repoDomain, group, library, this.repoToken);
		} else {
			try {
				FileUtils.forceMkdir(f.getParentFile());
				if (! this.gitUtils.existsProjectOnServer(group + "/" + library)) {
					ResultJsonObjectArray createResult = this.gitUtils.createProjectInGroup(group, library);
					if (createResult.getStatus().code != 200) {
						try {
							Thread.sleep(5000);
							this.gitUtils.createProjectInGroup(group, library);
						} catch (InterruptedException e) {
						}
						
					}
				}
				GitlabUtils.cloneGitlabProject(this.gitPath + group, this.repoDomain, group, library, this.repoToken);
			} catch (IOException e) {
				ErrorUtils.report(logger, e);
			}
		}
	}
	/**
	 * Reads the serailized Json files and converts
	 * AGES libraries to ares format and writes
	 * the ares files
	 */
	private void writeAges(String groupPath, String library, int libNbr, int totalLibs) {
		try {
			Instant start = Instant.now();
			String fullPath = groupPath + "/" + library;
			this.preProcessLibrary(groupPath, library);
			String [] libParts = library.split("_");
			String  agesDomain = "_" + libParts[0] + "_" + libParts[1].toUpperCase() + "_" + libParts[2]; 
			StringBuffer tQsb = new StringBuffer();
			tQsb.append("match (n:Liturgical) where n.id starts with '");
			tQsb.append(library);
			tQsb.append("' return distinct n.topic as topic order by n.topic;");
			ResultJsonObjectArray tQr = dbms.getForQuery(tQsb.toString());

			long processed = 0;
			long total = tQr.valueCount;

			for (JsonObject topicO : tQr.getValues()) {
				String topic = topicO.get("topic").getAsString();
				String agesResource = topic + agesDomain;
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
				}
				String topicPath = topic.replaceAll("_", "/");
				topicPath = topicPath.replaceAll("\\.", "/");
				String out = this.gitPath + fullPath + "/" + topicPath + "/" + agesResource + ".ares";
				FileUtils.write(new File(out), sb.toString());
				processed++;
				if (this.debugEnabled) {
					logger.info(libNbr + " of " + totalLibs + " " + Instant.now().toString() + " processed library/topic " + processed + " of " + total + " " + library + "~" + topic);
				}
			}
			GitlabUtils.gitAddCommitPush(this.gitPath + groupPath, "olwsys", library, ".", Instant.now().toString() + ".Serialized." + this.getElapsedMessage(start));
		} catch (Exception e) {
			ErrorUtils.report(logger, e);
		}
	}
	
}
