package org.ocmc.olw.serializer.utils;

import java.time.Instant;

import org.ocmc.ioc.liturgical.schemas.models.ws.response.ResultJsonObjectArray;

import com.google.gson.JsonObject;

import net.ages.alwb.utils.core.datastores.neo4j.Neo4jConnectionManager;

/**
 * This is an unfinished class.
 * For the specified library, reads all TextLiturgical occurrences and generates
 * an HTML file containing the library's version and Dedes English.
 *  
 * @author mac002
 *
 */
public class LibraryToHtml {
	private String uid = "";
	private String pwd = "";
	private String url = "";
	private String leftLibrary = "";
	private String rightLibrary = "";
	
	private Neo4jConnectionManager db = null;

	public LibraryToHtml(
			String uid
			, String pwd
			, String url
			, String leftLibrary
			, String rightLibrary
			) {
		this.uid = uid;
		this.pwd = pwd;
		this.url = url;
		this.leftLibrary = leftLibrary;
		this.rightLibrary = rightLibrary;
	}
	

	private String getRow(
			String topic
			, String key
			, String rightValue
			) {
		StringBuffer row = new StringBuffer();
		String leftId = this.leftLibrary + "~" + topic + "~" + key;
		String [] parts = this.leftLibrary.split("_");
		String agesLeftLib = parts[0] + "_" + parts[1].toUpperCase() + "_" + parts[2];
		parts = this.rightLibrary.split("_");
		String agesRightLib = parts[0] + "_" + parts[1].toUpperCase() + "_" + parts[2];
		String query = "match (n:Liturgical) where n.id = '" + leftId + "';"; 
		ResultJsonObjectArray qResult = db.getForQuery(query);
		String leftValue = "";
		if (qResult.valueCount == 1) {
			leftValue = qResult.getFirstObjectValueAsObject().get("value").getAsString();
		}
		row.append("<tr><td class='leftCell'><p class='hymn'><span class='kvp' data-key='");
		row.append(topic);
		row.append("_");
		row.append(agesLeftLib);
		row.append("|");
		row.append(key);
		row.append("'>");
		row.append(leftValue);
		row.append("</span></p></td><td class='rightCell'><p class='hymn'><span class='kvp' data-key='");
		row.append(topic);
		row.append("_");
		row.append(agesRightLib);
		row.append("|");
		row.append(key);
		row.append("'>");
		row.append(rightValue);
		row.append("</span></p></td></tr>");
		return row.toString();
	}
	private synchronized void process() {
		db = new Neo4jConnectionManager(
				  url
				  , this.uid
				  , this.pwd
				  , false
				  );

		String queryLibs = "match (n:Liturgical) where n.library = '" + this.rightLibrary + "' return n.id as id,  n.topic as topic, n.key as key, n.value as value order by n.id;";
		ResultJsonObjectArray libsResult = db.getForQuery(queryLibs);
		for (JsonObject iobj : libsResult.getValues()) {
			try {
				String rightId = iobj.get("id").getAsString();
				String rightTopic = iobj.get("topic").getAsString();
				String rightKey = iobj.get("key").getAsString();
				String rightValue = iobj.get("value").getAsString();
				String row = this.getRow(rightTopic, rightKey, rightValue);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println(url + " finished creating list " + Instant.now().toString());
	}


	public static void main(String[] args) {
		String uid = System.getenv("UID");
		String pwd = System.getenv("PWD");
		String url = System.getenv("URL");
		LibraryToHtml it = new LibraryToHtml(uid, pwd,url, "gr_gr_cog","en_us_repass");
		it.process();
	}

}
