package org.ocmc.olw.serializer.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.ocmc.ioc.liturgical.schemas.constants.SCHEMA_CLASSES;
import org.ocmc.ioc.liturgical.schemas.models.ModelHelpers;
import org.ocmc.ioc.liturgical.schemas.models.supers.LTKDb;
import org.ocmc.ioc.liturgical.schemas.models.synch.GithubRepo;
import org.ocmc.ioc.liturgical.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

/**
 * Converts db2json json files into json2cvs cvs files.
 * There is one file for each schema type.
 * 
 * @author mac002
 *
 */
public class Json2Csv {
	private static final Logger logger = LoggerFactory.getLogger(Json2Csv.class);
	private String createForm = "CreateForm:1.1";
	private int createFormLength = createForm.length();
	private String pathIn = "";
	private String pathOut = "";
	private StreamWriterPool nodeWriterPool = null;
	private StreamWriterPool linkWriterPool = null; // i.e. relationships.
	private String delimiter = ","; // separator;
	private Map<String,Integer> csvHeaderCountMap = new TreeMap<String,Integer>();
	private Map<String,String> csvHeadersMap = new TreeMap<String,String>(); // the header string to be written out, retrieved by schema
	private Map<String,Map<String,String>> csvHeaderStringMap = new TreeMap<String,Map<String,String>>(); // for each header the key and key  data type
	private Map<String,Map<String,Object>> csvHeaderObjectMap = new TreeMap<String,Map<String,Object>>();
	private List<String> linkTypesSeen  = new ArrayList<String>();
	private Map<String,String> idMap = new TreeMap<String,String>(); // the header string to be written out, retrieved by schema
	private boolean debug = false;
	private String genericLinkHeader = ":START_ID,:END_ID,:TYPE\n";
	
	public Json2Csv(String pathIn, String pathOut) {
		this.pathIn = pathIn;
		this.pathOut = pathOut;
		if (! pathOut.endsWith("/")) {
			this.pathOut = this.pathOut + "/";
		}
		this.nodeWriterPool = new StreamWriterPool(this.pathOut + "nodes/");
		this.linkWriterPool = new StreamWriterPool(this.pathOut + "links/");
	}
	
	public void process() {
		Gson gson = new Gson();
		JsonParser parser = new JsonParser();
		List<File> files = FileUtils.getFilesFromSubdirectories(this.pathIn, "json");
		int filesProcessed = 0;
		int totalFiles  = files.size();
		int nodeCount = 0;
		int relationshipCount = 0;
		int otherCount = 0;
		for (File f : files) {
			filesProcessed++;
			List<String> lines = FileUtils.linesFromFile(f);
			int linesProcessed = 0;
			int totalLines = lines.size();
			for (String line : lines) {
				linesProcessed++;
				JsonElement lineElement = parser.parse(line);
				JsonArray array = new JsonArray();
				if (lineElement.isJsonArray()) {
					array = lineElement.getAsJsonArray();
				} else {
					array.add(lineElement.getAsJsonObject());
				}
				for (JsonElement ele : array) {
					JsonObject o = ele.getAsJsonObject();
					if (o.has("props")) { // this is a node
						JsonObject p = o.get("props").getAsJsonObject();
						nodeCount++;
						LTKDb ltkDbClass = null;
						String schema = p.get("_valueSchemaId").getAsString();
						ltkDbClass = SCHEMA_CLASSES.ltkDbForSchemaName(schema);
						LTKDb record = null;
						if (ltkDbClass == null) {
							if (schema.startsWith("GithubRepo")) {
								// ignore
							} else {
								logger.info("Can't find LTKDB for " + schema);
							}
						} else {
//							// Problem: the Create class names return the db version.  What happens if convert to db version?
//							if (! ltkDbClass.getClass().getSimpleName().equals(schema)) {
//								schema = ltkDbClass.getClass().getSimpleName();
//							}
							record = 
									 gson.fromJson(
											p.toString()
											, ltkDbClass.getClass()
								);
							String id = record.getId();
							if (this.idMap.containsKey(id)) {
								logger.error("duplicate id " + id + " found in " + f.getAbsolutePath());
								logger.error("Also occurs in " + this.idMap.get(id));
							} else {
								this.idMap.put(id,f.getAbsolutePath());
								String header = "";
								if (schema.endsWith(this.createForm)) {
									schema = schema.substring(0, schema.length() - this.createFormLength);
								} else {
									schema = schema.substring(0, schema.length()-4);
								}
								if (! this.csvHeaderStringMap.containsKey(schema)) {
									this.csvHeaderCountMap.put(schema, 1);
									header = this.toCsvHeader(ltkDbClass);
									this.nodeWriterPool.write(schema, header);
								} else {
									Integer i = this.csvHeaderCountMap.get(schema);
									i++;
									this.csvHeaderCountMap.put(schema, i);
								}
								this.nodeWriterPool.write(schema, this.toCsvContent(schema, record.fetchOntologyLabels(), record.toJsonObject()));
							}
						}
					} else if (o.has("from")){ // this is a relationship
						relationshipCount++;
						String type = o.get("type").getAsString();
						if (! linkTypesSeen.contains(type)) {
							linkTypesSeen.add(type);
							this.linkWriterPool.write(type, this.genericLinkHeader);
						}
						StringBuffer sbLink = new StringBuffer();
						sbLink.append(o.get("from").getAsString());
						sbLink.append(this.delimiter);
						sbLink.append(o.get("to").getAsString());
						sbLink.append(this.delimiter);
						sbLink.append(type);
						this.linkWriterPool.write(type, sbLink.toString());
					} else {
						otherCount++;
					}
				}
				logger.info(filesProcessed + ":" + totalFiles + ":" + linesProcessed + ":" + totalLines);
			}
		}
		StringBuffer config = new StringBuffer();
		config.append("--mode csv ");
		for (String path : this.nodeWriterPool.getPaths()) {
			config.append("--nodes ");
			config.append(path);
			config.append(" ");
		}
		for (String path : this.linkWriterPool.getPaths()) {
			config.append("--relationships ");
			config.append(path);
			config.append(" ");
		}
		config.append(" --database graph.db");
		
		this.nodeWriterPool.closeWriters();
		this.linkWriterPool.closeWriters();

		FileUtils.writeFile(this.pathOut + "import.config", config.toString());
		for (Entry<String,Integer> entry : this.csvHeaderCountMap.entrySet()) {
			logger.info(entry.getKey() + ": "+ entry.getValue());
		}
		logger.info("Total schemas: " + this.csvHeaderCountMap.size());
		logger.info("Nodes: " + nodeCount);
		logger.info("Relationships: " + relationshipCount);
		logger.info("Other (this is bad!): " + otherCount);
	}
	
	private String toCsvHeader(LTKDb model) {
		String schema = model.getClass().getSimpleName();
		StringBuffer sbHeader = new StringBuffer();
		Map<String,String> map = new TreeMap<String,String>();
		String k = "id";
		JsonObject o = model.toJsonObject();

		o.remove(k);
		sbHeader.append("id:ID");
		sbHeader.append(delimiter);
		map.put(k, "String");
	
		k = "library";
		o.remove(k);
		sbHeader.append(k);
		sbHeader.append(":string");
		sbHeader.append(delimiter);
		map.put(k, "String");
		
		k = "topic";
		o.remove(k);
		sbHeader.append(k);
		sbHeader.append(":string");
		sbHeader.append(delimiter);
		map.put(k, "String");

		k = "key";
		o.remove(k);
		sbHeader.append(k);
		sbHeader.append(":string");
		sbHeader.append(delimiter);
		map.put(k, "String");

		k = "_valueSchemaId";
		o.remove(k);
		sbHeader.append(k);
		sbHeader.append(":string");
		map.put(k, "String");

		for (Entry<String,JsonElement> entry : o.entrySet()) {
			String key = entry.getKey();
			String type = "";
			JsonElement e = entry.getValue();
			if (e.isJsonArray()) {
				type = "string[]";
			} else if (e.isJsonNull()) {
				logger.info("trouble");
			} else if (e.isJsonObject()) {
				logger.info("trouble");
			} else if (e.isJsonPrimitive()) {
				JsonPrimitive jp = e.getAsJsonPrimitive();
				if (jp.isString()) {
					type = "string";
				} else if (jp.isBoolean()){
					type = "boolean";
				} else if (jp.isNumber()){
					type = "double";
				}  else {
					logger.info("trouble");
				}
			} else {
				logger.info("trouble");
			}
			map.put(key, type);
		}
		for (Entry<String,String> entry : map.entrySet()) {
			String key = entry.getKey();
			if (key.equals("id") || key.equals("library") || key.equals("topic")  || key.equals("key")  || key.equals("_valueSchemaId") ) {
				continue; 
			}
			if (sbHeader.length() > 0) {
				sbHeader.append(this.delimiter);
			}
			sbHeader.append(key);
			sbHeader.append(":");
			sbHeader.append(entry.getValue());
		}
		sbHeader.append(delimiter);
		sbHeader.append(":LABEL");
		this.csvHeaderStringMap.put(schema, map);
		Map<String,Object> hashMap = ModelHelpers.toHashMap(model);
		this.csvHeaderObjectMap.put(schema, hashMap);
		this.csvHeadersMap.put(schema, sbHeader.toString());
		return sbHeader.toString();
	}

	private String toCsvContent(String schema, String labels, JsonObject o) {
		StringBuffer sbDebug = new StringBuffer();
		int debugPropCount = 0;
		List<String> debugHeaderList = new ArrayList<String>();
		String separator = "\n\t";
		
		StringBuffer sbContent = new StringBuffer();
		Map<String,String> keyMap = this.csvHeaderStringMap.get(schema);
		Map<String,Object> hashMap = this.csvHeaderObjectMap.get(schema);
		if (keyMap == null) {
			System.out.println("stop");
		}
		if (keyMap.size() != o.entrySet().size()) {
			List<String> objectKeys = new ArrayList<String>();
			for (Entry<String,JsonElement> entry : o.entrySet()) {
				objectKeys.add(entry.getKey());
			}
			for (String key : keyMap.keySet()) {
				if (!objectKeys.contains(key)) {
					o.addProperty(key, hashMap.get(key).toString());
				}
			}
			for (String key : objectKeys) {
				if (! keyMap.containsKey(key)) {
					logger.info("Header key map does not have " + key);
				}
			}
		}
		
		String k = "id";
		String p = o.get(k).getAsString();
		sbContent.append(this.q(p));
		sbContent.append(delimiter);
		o.remove(k);
		if (this.debug) {
			for (String hKey : this.csvHeadersMap.get(schema).split(",")) {
				debugHeaderList.add(hKey);
			}
			sbDebug.append(debugHeaderList.get(debugPropCount) + separator + k + " = " + p + "\n");
			debugPropCount++;
		}
		
		k = "library";
		p = o.get(k).getAsString();
		sbContent.append(this.q(p));
		sbContent.append(delimiter);
		o.remove(k);
		if (this.debug) {
			sbDebug.append(debugHeaderList.get(debugPropCount) + separator + k + " = " + p + "\n");
			debugPropCount++;
		}
		
		k = "topic";
		p = o.get(k).getAsString();
		sbContent.append(this.q(p));
		sbContent.append(delimiter);
		o.remove(k);
		if (this.debug) {
			sbDebug.append(debugHeaderList.get(debugPropCount) + separator + k + " = " + p + "\n");
			debugPropCount++;
		}

		k = "key";
		p = o.get(k).getAsString();
		sbContent.append(this.q(p));
		sbContent.append(delimiter);
		o.remove(k);
		if (this.debug) {
			sbDebug.append(debugHeaderList.get(debugPropCount) + separator + k + " = " + p + "\n");
			debugPropCount++;
		}

		k = "_valueSchemaId";
		p = o.get(k).getAsString();
		p = p.replace("CreateForm", "");
		sbContent.append(this.q(p));
		o.remove(k);
		if (this.debug) {
			sbDebug.append(debugHeaderList.get(debugPropCount) + separator + k + " = " + p + "\n");
			debugPropCount++;
		}

		for (String key : keyMap.keySet()) {
			if (key.equals("id") || key.equals("library") || key.equals("topic")  || key.equals("key")  || key.equals("_valueSchemaId") ) {
				continue; 
			}
			if (this.debug) {
				sbDebug.append(debugHeaderList.get(debugPropCount) + separator + key + " = ");
				debugPropCount++;
			}
			JsonElement e = o.get(key);
			sbContent.append(this.delimiter);
			if (e == null) {
				sbContent.append(this.q(hashMap.get(key).toString()));
				if (this.debug) {
					sbDebug.append(this.q(hashMap.get(key).toString()) + "\n");
				}
			} else {
				if (e.isJsonArray()) {
					StringBuffer array = new StringBuffer();
					for (JsonElement ae : e.getAsJsonArray()) {
						if (array.length() > 1) {
							array.append(";");
						}
						array.append(ae.toString());
					}
					sbContent.append(this.q(array.toString()));
					if (this.debug) {
						sbDebug.append(array.toString() + "\n");
					}
				} else if (e.isJsonNull()) {
					logger.info("trouble");
				} else if (e.isJsonObject()) {
					logger.info("trouble");
				} else if (e.isJsonPrimitive()) {
					JsonPrimitive jp = e.getAsJsonPrimitive();
					if (jp.isString()) {
						String value = jp.getAsString();
						if (value.length() == 0) {
							value = hashMap.get(key).toString();
						}
						sbContent.append(this.q(value));
						if (this.debug) {
							sbDebug.append(this.q(value) + "\n");
						}
					} else if (jp.isBoolean()){
						sbContent.append(jp.toString());
						if (this.debug) {
							sbDebug.append(jp.toString() + "\n");
						}
					} else if (jp.isNumber()){
						sbContent.append(jp.toString());
						sbContent.append(jp.toString());
						if (this.debug) {
							sbDebug.append(jp.toString() + "\n");
						}
					}  else {
						logger.info("trouble");
					}
				} else {
					logger.info("trouble");
				}
			}
		}
		sbContent.append(delimiter);
		sbContent.append(labels.replace("CreateForm", "").replaceAll(":", ";"));
		sbContent.append("\n");
		if (debug) {
			logger.info(schema);
			logger.info(this.csvHeadersMap.get(schema));
			logger.info(sbDebug.toString());
		}
		return sbContent.toString();
	}

	private String q(String s) {
		//When using LOAD CSV to read a file which includes data with double quote characters (“), 
		// the quotes need to be escaped as 2 double quote characters
		s = s.replaceAll("\n", "");
		s = s.replaceAll("\r", "");
		s = s.replaceAll("\"", "\"\"");
//		return ModelHelpers.wrapQuotes(s);
		return "\"" + s + "\"";
	}
	/**
	 * Converts db2json json files into db2cvs files.
	 * There is one file for each schema type.
	 * @param args not used
	 */
	public static void main(String[] args) {
		String dirIn = System.getenv("DIR_IN");
		String dirOut = System.getenv("DIR_OUT");
		Json2Csv json2Csv = new Json2Csv(dirIn, dirOut);
		json2Csv.process();
	}

}
