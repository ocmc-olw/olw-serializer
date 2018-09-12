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
 * Converts db2json to csv that can be
 * used to load Neo4j or a sql db
 * and commits them to gitlab.liml.org
 * 
 * @author mac002
 *
 */
public class Json2Csv implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(Json2Csv.class);
	boolean debugEnabled = false;
	boolean reinit = false;
	File gitFolder = null;
	String gitPath = "";
	String repoToken = "";
	String repoUser = "";
	String repoDomain = "";
	private GitlabRestClient gitUtils = null;
	private List<String> librariesList = new ArrayList<String>();
	int pushDelay = 30000;  // 60000 = 1 minute
	PathMap pathMap = new PathMap();

	public Json2Csv(
			String repoDomain
			, String repoUser
			, String repoToken
			, int pushDelay
			, boolean debugEnabled
			, boolean reinit
			) {
		this.gitPath = Constants.GIT_FOLDER;
		this.pushDelay = pushDelay;
		try {
			this.gitUtils = new GitlabRestClient(repoDomain,repoToken);
		} catch (RestInitializationException e) {
			e.printStackTrace();
		}
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
		this.sendMessage("Converting json to csv");
		 this.createLibrariesList();

		 int total = this.librariesList.size();
		 int processed = 0;
		 
		 if (this.reinit) {
			 this.gitUtils.deleteAllProjectsInGroup("serialized/db2csv");
		 }
		 
		 for (String library : this.librariesList) {
			Instant libStart = Instant.now();
			processed++;

			// the work occurs in this procedure
			this.processLibrary(library, processed, total);

			this.sendMessage(processed + " of " + total + " json2csv for " + library + "." + this.getElapsedMessage(libStart));
		 }
		 String startMsg = startTime + " started  json2csv";
		 String finishMsg = Instant.now().toString() + " finished json2csv." + this.getElapsedMessage(start);
		this.sendMessage(startMsg);
		this.sendMessage(finishMsg);
		String out = this.gitPath 
				+ "serialized/json2csv.log"; 
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
			this.writeNodeJson2Csv(Constants.PROJECT_DB2JSON_NODES, library, libNbr, totalLibs);
			Thread.sleep(this.pushDelay);
			this.writeLinksJson2Csv(Constants.PROJECT_DB2JSON_LINKS, library, libNbr, totalLibs);
			Thread.sleep(this.pushDelay);
			if (library.equals("gr_gr_cog")) {
				this.writeLinkPropsJson2Csv(Constants.PROJECT_DB2JSON_LINK_PROPS, library, libNbr, totalLibs);
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
	}
	
	/**
	 * Writes nodes for each library~topic to a single json file
	 * for that topic
	 */
	private synchronized void writeNodeJson2Csv(
			String groupPath
			, String library
			, int libNbr
			, int totalLibs
			) {
		if (this.debugEnabled) {
			logger.info(Instant.now().toString() + " json2csv nodes processing " + library);
		}
		this.preProcessLibrary(groupPath, library);
		StringBuffer sb = new StringBuffer();
			long processed = 0;
			long total = 0;
		 	logger.info(Instant.now().toString() + " json2csv nodes finished processing " + library);
		}
	
	
	/**
	 * Writes links to a single json file
	 * for that topic
	 */
	private synchronized void writeLinksJson2Csv(
			String groupPath
			, String library
			, int libNbr
			, int totalLibs
			) {
		if (this.debugEnabled) {
			logger.info(Instant.now().toString() + " json2csv links processing " + library);
		}
		this.preProcessLibrary(groupPath, library);
		StringBuffer sb = new StringBuffer();
			long processed = 0;
			long total = 0;
		 	logger.info(Instant.now().toString() + " json2csv links finished processing " + library);
	}
	
	/**
	 * Writes properties of links to a single json file
	 * for that topic
	 */
	private synchronized void writeLinkPropsJson2Csv(String groupPath, String library, int libNbr, int totalLibs) {
		if (this.debugEnabled) {
			logger.info(Instant.now().toString() + " json2csv link props processing " + library);
		}
		this.preProcessLibrary(groupPath, library);
		StringBuffer sb = new StringBuffer();
			long processed = 0;
			long total = 0;
		 	logger.info(Instant.now().toString() + " json2csv link props finished processing " + library);
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
	
}
