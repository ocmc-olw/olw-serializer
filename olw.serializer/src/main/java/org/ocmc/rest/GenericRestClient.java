package org.ocmc.rest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import org.ocmc.ioc.liturgical.schemas.constants.HTTP_RESPONSE_CODES;
import org.ocmc.ioc.liturgical.schemas.models.ws.response.ResultJsonObjectArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class GenericRestClient {
	// TODO rework this to provide a builder with dot add ons
	// TODO implement PUT
	private static final Logger logger = LoggerFactory.getLogger(GenericRestClient.class);
	private static Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
	private static JsonParser parser = new JsonParser();
	public static enum METHODS  {GET, POST, PUT, DELETE};
	private String token = "";
    private String apiUrl = "";

    public GenericRestClient(String url, String token) {
    	this.token = token;
    	try {
        	this.apiUrl = url;
    	} catch (Exception e) {
    		
    	}
    }
    
    public ResultJsonObjectArray get(String topic) {
    	return this.request(METHODS.GET, topic, "", "");
    }

    public ResultJsonObjectArray get(String topic, String path) {
    	return this.request(METHODS.GET, topic, path, "");
    }

    public ResultJsonObjectArray get(String topic, String path, String query) {
    	return this.request(METHODS.GET, topic, path, query);
    }

    public ResultJsonObjectArray post(String topic, String path, String query) {
    	return this.request(METHODS.POST, topic, path, query);
    }

    public ResultJsonObjectArray delete(String topic, String path) {
    	return this.request(METHODS.DELETE, topic, path, "");
    }


    /**
     * Generic HTTP request to a rest api
     * @param method i.e. RESTCLIENT.METHODS.GET, POST, PUT, DELETE
     * @param topic i.e. RESTCLIENT.TOPICS.groups or projects
     * @param path is optional and is the group and entity, e.g. serialized/db2json/nodes.  This will URLEncoder.encode applied to it.
     * @param query is optional and is e.g. ?name=somename&namespace_id=somenamespace
     * @return the result object array.  Each occurrence returned will be a JsonObject
     */
    public synchronized ResultJsonObjectArray request(
    		METHODS method
    		, String topic
    		, String path
    		, String query
    		) {
    	ResultJsonObjectArray result = new ResultJsonObjectArray(false);
    	try {
   		String encodedPath = "";
    		URL url = null;
    		StringBuilder sb = new StringBuilder();
    		sb.append(this.apiUrl);
    		sb.append(topic.trim());
    		if (path.length() > 0) {
    			encodedPath = URLEncoder.encode(path.trim(), "UTF-8");
    			if (query.length() > 0) {
    				sb.append("/");
    				sb.append(URLEncoder.encode(path.trim(), "UTF-8"));
    				sb.append(query.trim());
    			} else {
    				sb.append("/");
    				sb.append(URLEncoder.encode(path.trim(), "UTF-8"));
    			}
    		} else {
    			if (query.length() > 0) {
    				sb.append(query.trim());
    			}
    		}
    		url = new URL(sb.toString());
    		result.setQuery(url.toString());
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            conn.setRequestMethod(method.name());
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Private-Token", this.token);
            conn.setRequestProperty("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.155 Safari/537.36");
            conn.setConnectTimeout(0);
            
            int responseCode = conn.getResponseCode();
            result.setStatusCode(responseCode);
            result.setStatusMessage(conn.getResponseMessage());
            boolean allOk = true;
            switch (method) {
			case DELETE:
	            if (conn.getResponseCode() != 202) {
	            	allOk = false;
	            }
				break;
			case POST:
	            if (conn.getResponseCode() != HTTP_RESPONSE_CODES.CREATED.code) {
	            	allOk = false;
	            }
				break;
			case PUT:
				break;
			default:
	            if (conn.getResponseCode() != 200) {
	            	allOk = false;
	            }
				break;
            }
            if (allOk) {
                BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
                String apiOutput = br.readLine();
                if (apiOutput.startsWith("[")) {
                	JsonArray array = parser.parse(apiOutput).getAsJsonArray();
                	for (JsonElement e : array) {
                		result.addValue(e.getAsJsonObject());
                	}
                } else {
                	result.addValue(parser.parse(apiOutput).getAsJsonObject());
                }
            }
            conn.disconnect();
 
    	} catch (Exception e) {
    		result.setStatusCode(HTTP_RESPONSE_CODES.BAD_REQUEST.code);
    		result.setStatusDeveloperMessage(HTTP_RESPONSE_CODES.BAD_REQUEST.message + " - " + path + e.getMessage()) ;
    	}
    	return result;
    }
	public static Gson getGson() {
		return gson;
	}

	public static void setGson(Gson gson) {
		GenericRestClient.gson = gson;
	}

	public static JsonParser getParser() {
		return parser;
	}

	public static void setParser(JsonParser parser) {
		GenericRestClient.parser = parser;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public static Logger getLogger() {
		return logger;
	}

	public String getApiUrl() {
		return apiUrl;
	}

	public void setApiUrl(String apiUrl) {
		this.apiUrl = apiUrl;
	}
}
