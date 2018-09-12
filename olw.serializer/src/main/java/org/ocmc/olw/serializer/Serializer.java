package org.ocmc.olw.serializer;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.ocmc.ioc.liturgical.schemas.id.managers.IdManager;
import org.ocmc.ioc.liturgical.schemas.models.ws.response.ResultJsonObjectArray;
import org.ocmc.ioc.liturgical.utils.ErrorUtils;
import org.ocmc.rest.client.GitlabRestClient;
import org.ocmc.rest.client.RestInitializationException;
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
	private GitlabRestClient gitUtils = null;
	private List<String> librariesList = new ArrayList<String>();
	int pushDelay = 30000;  // 60000 = 1 minute
	PathMap pathMap = new PathMap();

	public Serializer(
			String user
			, String pwd
			, String url
			, String whereClause
			, String whereLibraries
			, String repoDomain
			, String repoUser
			, String repoToken
			, int pushDelay
			, boolean debugEnabled
			, boolean reinit
			) {
		this.user = user;
		this.pwd = pwd;
		this.url = url;
		this.gitPath = Constants.GIT_FOLDER;
		this.pushDelay = pushDelay;
		try {
			this.gitUtils = new GitlabRestClient(repoDomain,repoToken);
		} catch (RestInitializationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		 
		 if (this.reinit) {
			 this.gitUtils.deleteAllProjectsInGroup("serialized/db2ares");
			 this.gitUtils.deleteAllProjectsInGroup("serialized/db2json/gr_gr_cog");
			 this.gitUtils.deleteAllProjectsInGroup("serialized/db2json/links");
			 this.gitUtils.deleteAllProjectsInGroup("serialized/db2json/linkprops");
			 this.gitUtils.deleteAllProjectsInGroup("serialized/db2json/nodes");
		 }
		 
		 for (String library : this.librariesList) {
			Instant libStart = Instant.now();
			processed++;

			// the work occurs in this procedure
			this.processLibrary(library, processed, total);

			this.sendMessage(processed + " of " + total + " Neo4j to " + library + "." + this.getElapsedMessage(libStart));
		 }
		 String startMsg = startTime + " started  serializing Neo4j.";
		 String finishMsg = Instant.now().toString() + " finished serializing Neo4j." + this.getElapsedMessage(start);
		this.sendMessage(startMsg);
		this.sendMessage(finishMsg);
		String out = this.gitPath 
				+ "serialized/serializer.log"; 
		try {
			FileUtils.write(new File(out), startMsg + "\n" + finishMsg);
		} catch (IOException e) {
		}

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
		try {
			Thread.sleep(this.pushDelay);
			this.writeAges(Constants.PROJECT_DB2ARES , library, libNbr, totalLibs);
			Thread.sleep(this.pushDelay);
			this.writeNodes2Json(Constants.PROJECT_DB2JSON_NODES, library, libNbr, totalLibs);
			Thread.sleep(this.pushDelay);
			this.writeLinks2Json(Constants.PROJECT_DB2JSON_LINKS, library, libNbr, totalLibs);
			Thread.sleep(this.pushDelay);
			if (library.equals("gr_gr_cog")) {
				this.writeLinkProps2Json(Constants.PROJECT_DB2JSON_LINK_PROPS, library, libNbr, totalLibs);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
	private synchronized void writeNodes2Json(
			String groupPath
			, String library
			, int libNbr
			, int totalLibs
			) {
		Instant start = Instant.now();
		if (this.debugEnabled) {
			logger.info(Instant.now().toString() + " db2json nodes processing " + library);
		}
		this.preProcessLibrary(groupPath, library);
		StringBuffer sb = new StringBuffer();
			sb.append("match (n:Root) where n.library = '");
			sb.append(library);
			sb.append("' return distinct n.topic as topic order by n.topic;");
			ResultJsonObjectArray result = dbms.getForQuery(sb.toString());
			long processed = 0;
			long total = result.valueCount;
			
			for (JsonObject o : result.getValues()) {
				try {
					if (o != null && o.has("topic"))  {
						String topic = o.get("topic").getAsString();
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
							String out = this.gitPath 
									+ groupPath 
									+ "/" + library 
									+ "/" + topic + ".json";
							FileUtils.write(new File(out),topics.getValuesAsJsonArray().toString());
							if (this.debugEnabled) {
								logger.info(Instant.now().toString() + " " + processed + " of " + total + " - " + out);
							}
						}
					}
				} catch (Exception e) {
					System.out.println(o.toString());
					e.printStackTrace();
				}
			}
			this.gitUtils.gitAddCommitPush(
					this.gitPath + groupPath
					, "olwsys"
					, library
					, "."
					, Instant.now().toString() + ".Serialized nodes to json." + this.getElapsedMessage(start)
					);
		 	logger.info(Instant.now().toString() + " db2json nodes finished processing " + library);
		}
	
	
	/**
	 * Writes links to a single json file
	 * for that topic
	 */
	private synchronized void writeLinks2Json(
			String groupPath
			, String library
			, int libNbr
			, int totalLibs
			) {
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
		gitUtils.gitAddCommitPush(this.gitPath + groupPath, "olwsys", library, ".", Instant.now().toString() + ".Serialized." + this.getElapsedMessage(start));
	}
	
	/**
	 * Writes properties of links to a single json file
	 * for that topic
	 */
	private synchronized void writeLinkProps2Json(String groupPath, String library, int libNbr, int totalLibs) {
		    Instant start = Instant.now();
		 	logger.info(Instant.now().toString() + " Writing Link Properties to Json");
			String typesQuery = "match (f:Root)-[r]->(t:Root) where f.library = '" + library + "' and type(r) starts with 'REFERS_TO' return distinct type(r) order by type(r)";
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
								String out = this.gitPath 
										+ groupPath 
										+ "/" + library 
										+ "/" + type + ".json";
								FileUtils.write(new File(out),props.toString());
							}
						}
						this.gitUtils.gitAddCommitPush(
								this.gitPath 
								+ groupPath
								, "olwsys"
								, library
								, "."
								, Instant.now().toString() + ".Serializer." + type + "."  + this.getElapsedMessage(start)
								);
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
			if (this.gitUtils.existsProject(fullPath)) {
				this.gitUtils.deleteProject(fullPath);
				try {
					Thread.sleep(3000); // give the server time to process the delete before we attempt to recreate the project
				} catch (InterruptedException e) {
				}
			}
		}
		if (f.exists()) {
			gitUtils.pullGitlabProject(this.gitPath + group, this.repoDomain, group, library);
		} else {
			try {
				FileUtils.forceMkdir(f.getParentFile());
				if (! this.gitUtils.existsProject(group + "/" + library)) {
					ResultJsonObjectArray createResult = this.gitUtils.postProject(group, library);
				}
				gitUtils.cloneGitlabProject(this.gitPath + group, this.repoDomain, group, library);
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
				// he.h.m1~TonProfitinIonan.ode = misc_en_US_repass.Ode6
				// has a redirect but doesn't show up in generated ares
				// he.h.m1~TonProfitinIonan.name = ""
				// also does not show up
				vQsb.append("match (n:Liturgical) where n.id starts with '");
				vQsb.append(library);
				vQsb.append("~");
				vQsb.append(topic);
				vQsb.append("' return n.key as key, n.value as value, n.redirectId as redirectId order by n.id;");

				if (topic.equals("dismissals")) {
					System.out.print("");
				}
				StringBuffer sb = new StringBuffer();
				sb.append("A_Resource_Whose_Name = ");
				sb.append(agesResource);
				sb.append("\n\n");
				ResultJsonObjectArray vQr = dbms.getForQuery(vQsb.toString());
				for (JsonObject vO : vQr.getValues()) {
					String key = vO.get("key").getAsString();
					if (key.equals("dis00.andascension")) {
						System.out.print("");
					}
					if (StringUtils.isNumeric(key)) {
						continue; // there is some issue in the database where there are several hundred keys that are a number.  This should not be.
					}
					sb.append(key);
					sb.append(" = ");
					String value = "";
					String redirect = "";
					if (vO.has("value")) {
						value = vO.get("value").getAsString();
					}
					 if (vO.has("redirectId")) {
						redirect = vO.get("redirectId").getAsString();
					}
					 if (redirect.length() > 0) {
							// misc_en_US_repass.Mode1   <= AGES redirect format
							// en_us_repass~misc~Mode1 <= OLW redirect format
						 try {
								IdManager idManager = new IdManager(redirect);
								String agesLanguage  = idManager.getLibraryLanguage();
								String agesCountry = idManager.getLibraryCountry().toUpperCase();
								String agesRealm = idManager.getLibraryRealm();
								String agesTopic = idManager.getTopic();
								String agesKey = idManager.getKey();
								redirect = agesTopic + "_" + agesLanguage + "_" + agesCountry + "_" + agesRealm + "." + agesKey;
						 } catch (Exception e) {
							 ErrorUtils.report(logger, e);
						 }
						sb.append(redirect);
					 } else {
							if (value.length() > 0) {
								String quoted = LibraryUtils.wrapQuotes(value);
								quoted = quoted.replaceAll("\\\\", "\\");
								sb.append(quoted);
							} else  {
								sb.append("\"\"");
							}
					 }
					sb.append("\n");
				}
				String topicPath = topic.replaceAll("_", "/");
				topicPath = topicPath.replaceAll("\\.", "/");
				StringBuffer out = new StringBuffer();
				out.append(this.gitPath + fullPath + "/");
				if (this.pathMap.containsKey(topic)) {
					out.append(this.pathMap.getPath(topic) + "/" + agesResource + ".ares");
				} else {
					out.append(topicPath + "/" + agesResource + ".ares");
				}
				FileUtils.write(new File(out.toString()), sb.toString());
				processed++;
				if (this.debugEnabled) {
					logger.info(
							libNbr 
							+ " of " + totalLibs 
							+ " " + Instant.now().toString() 
							+ " processed library/topic " 
							+ processed 
							+ " of " + total 
							+ " " + library + "~" + topic
							);
				}
			}
			gitUtils.gitAddCommitPush(
					this.gitPath + groupPath
					, "olwsys"
					, library, "."
					, Instant.now().toString() + ".Serialized." + this.getElapsedMessage(start)
					);
		} catch (Exception e) {
			ErrorUtils.report(logger, e);
		}
	}
	
}
