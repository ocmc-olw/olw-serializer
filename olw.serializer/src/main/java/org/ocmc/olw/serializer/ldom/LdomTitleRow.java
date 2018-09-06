package org.ocmc.olw.serializer.ldom;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.ocmc.ioc.liturgical.schemas.constants.MODES_TO_NEO4J;
import org.ocmc.ioc.liturgical.schemas.models.ws.response.ResultJsonObjectArray;

import net.ages.alwb.utils.core.datastores.neo4j.Neo4jConnectionManager;

/**
 * Inserts an row at the top of the table that gives title information.
 * The purpose of this is two-fold:
 * 1. When displayed in the editor, so the user can enter a language specific translation.
 * 2. When generating PDF, it will have the title.
 * 
 * If the HTML already has the title information, an additional title row will not be inserted.
 * @author mac002
 *
 */
public class LdomTitleRow {
	private Neo4jConnectionManager dbManager = null;
	private Document doc = null;
	private boolean needsTitleRow = false;
	private String timestamp = "";
	private String commemoration = "";
	private String title = "";
	private String service = "";
	private String month = "";
	private String day = "";
	private String mode = "";
	private TitleParts titleParts = null;

	public LdomTitleRow(
			Neo4jConnectionManager dbManager
			, Document doc
			) {
		this.dbManager = dbManager;
		this.doc = doc;
		this.parseTitle();
	}
	
	private void parseTitle() {
		Elements titleElements = this.doc.select("title");
		this.timestamp = titleElements.attr("data-timestamp");
		this.commemoration = titleElements.attr("data-commemoration");
		this.title = this.doc.select("title").text();
		if (
				this.title.toLowerCase().equals("octoechos")
				|| this.title.toLowerCase().equals("triodion")
				) {
			this.needsTitleRow = true;
		}
		titleParts = new TitleParts(this.title, doc.location());
	}
	
	private ResultJsonObjectArray getForIdStartsWith(String id) {
		String query = "match (n:Root) where n.id = '" + id + "'";
		ResultJsonObjectArray result = this.dbManager.getResultObjectForQuery(query);
		return result;
	}
	public String getTitleRowHtml() {
		StringBuffer sb = new StringBuffer();
		String agesIdGrk = titleParts.getTemplateTitlesId("gr_GR_cog", "", true);
		String agesIdEn = titleParts.getTemplateTitlesId("en_US_dedes", "", true);
		String dbIdGrk = titleParts.getTemplateTitlesId("gr_gr_cog", "", false);
		String dbIdEn = titleParts.getTemplateTitlesId("en_us_dedes", "", false);
		ResultJsonObjectArray queryResult = this.getForIdStartsWith(dbIdGrk);
		String grValue = queryResult.getFirstObject().get("value").getAsString();
		queryResult = this.getForIdStartsWith(dbIdEn);
		String enValue = queryResult.getFirstObject().get("value").getAsString();
		sb.append(this.getRowHtml(agesIdGrk, grValue, agesIdEn, enValue));
		if (this.titleParts.getMode().length() > 0) {
			sb.append(this.getModeRowHtml());
		}
		return sb.toString();
	}
	
	private String getModeRowHtml() {
		StringBuffer sb = new StringBuffer();
		String agesIdGrk = MODES_TO_NEO4J.getAgesId(titleParts.getMode(), "gr_GR_cog");
		String agesIdEn = MODES_TO_NEO4J.getAgesId(titleParts.getMode(), "en_US_dedes");
		String dbIdGrk = MODES_TO_NEO4J.getNeo4jId(titleParts.getMode(), "gr_gr_cog");
		String dbIdEn = MODES_TO_NEO4J.getNeo4jId(titleParts.getMode(), "en_us_dedes");
		ResultJsonObjectArray queryResult = this.getForIdStartsWith(dbIdGrk);
		String grValue = queryResult.getFirstObject().get("value").getAsString();
		queryResult = this.getForIdStartsWith(dbIdEn);
		String enValue = queryResult.getFirstObject().get("value").getAsString();
		sb.append(this.getRowHtml(agesIdGrk, grValue, agesIdEn, enValue));
		return sb.toString();
	}

	
	public String getRowHtml(
			String agesIdGrk
			, String grkValue
			, String agesIdEn
			, String enValue
			) {
		StringBuffer sb = new StringBuffer();
		sb.append("<tr>");
		sb.append("<td class='leftCell'>");
		sb.append("<p class='mixed'>");
		sb.append("<span class='boldred'>");
		sb.append("<span class='kvp' data-key='" + agesIdGrk + "'>" + grkValue + "</span>");
		sb.append("</span> ");
		sb.append("</p>");
		sb.append("</td>");
		sb.append("<td class='rightCell'>");
		sb.append("<p class='mixed'>");
		sb.append("<span class='boldred'>");
		sb.append("<span class='kvp' data-key='" + agesIdEn + "'>" + enValue + "</span>");
		sb.append("</span> ");
		sb.append("</p>");
		sb.append("</tr>");
		return sb.toString();
	}

	public boolean needsTitleRow() {
		return this.needsTitleRow;
	}

	public Element getTitleRow() {
		return null;
	}
	
	public Document getDoc() {
		return doc;
	}

	public void setDoc(Document doc) {
		this.doc = doc;
	}

	public boolean isNeedsTitleRow() {
		return needsTitleRow;
	}

	public void setNeedsTitleRow(boolean needsTitleRow) {
		this.needsTitleRow = needsTitleRow;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getCommemoration() {
		return commemoration;
	}

	public void setCommemoration(String commemoration) {
		this.commemoration = commemoration;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getService() {
		return service;
	}

	public void setService(String service) {
		this.service = service;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public String getMode() {
		return mode;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}
}

