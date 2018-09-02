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

public class RestClient {
	private static final Logger logger = LoggerFactory.getLogger(RestClient.class);
	private static Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
	private static JsonParser parser = new JsonParser();
	public static enum METHODS  {GET, POST, PUT, DELETE};
	public static enum TOPICS  {groups, projects};
	private String token = "";
    private String apiUrl = "";

    public RestClient(String url, String token) throws TokenException {
    	this.token = token;
    	if (token == null) {
    		throw new TokenException("Token cannot be null");
    	}
    	try {
        	this.apiUrl = url;
    	} catch (Exception e) {
    		
    	}
    }
    
    public ResultJsonObjectArray get(TOPICS topic, String path, String query) {
    	return this.request(METHODS.GET, topic.name(), path, query);
    }

    public ResultJsonObjectArray post(TOPICS topic, String path, String query) {
    	return this.request(METHODS.POST, topic.name(), path, query);
    }

    public ResultJsonObjectArray delete(TOPICS topic, String path) {
    	return this.request(METHODS.DELETE, topic.name(), path, "");
    }

    public ResultJsonObjectArray deleteProject(String name) {
    	return this.request(METHODS.DELETE, TOPICS.projects.name(), name, "");
    }

    public ResultJsonObjectArray postProject(String group, String project) {
    	ResultJsonObjectArray theGroup = this.get(TOPICS.groups, group,"");
    	int id = theGroup.getFirstObject().get("id").getAsInt();
    	return this.request(
    			METHODS.POST
    			, TOPICS.projects.name()
    			, ""
    			, "?name=" + project + "&namespace_id=" + id
    			);
    }

    public ResultJsonObjectArray request(
    		METHODS method
    		, String topic
    		, String path
    		, String query
    		) {
    	ResultJsonObjectArray result = new ResultJsonObjectArray(false);
    	try {
    		String encodedPath = "";
    		URL url = null;
    		if (path.length() > 0) {
    			encodedPath = URLEncoder.encode(path, "UTF-8");
    			if (query.length() > 0) {
                    url = new URL(this.apiUrl + topic + "/" + encodedPath + query);
    			} else {
                    url = new URL(this.apiUrl + topic + "/" + encodedPath);
    			}
    		} else {
    			if (query.length() > 0) {
                    url = new URL(this.apiUrl + topic + query);
    			} else {
                    url = new URL(this.apiUrl + topic);
    			}
    		}
    		result.setQuery(url.toString());
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            conn.setRequestMethod(method.name());
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Private-Token", this.token);
            conn.setConnectTimeout(0);
            int responseCode = conn.getResponseCode();
            String responseMessage = conn.getResponseMessage();
            result.setStatusCode(conn.getResponseCode());
            result.setStatusMessage(conn.getResponseMessage());
            switch (method) {
			case DELETE:
	            if (conn.getResponseCode() != 202) {
	                throw new RuntimeException(". Failed : HTTP error code : " + conn.getResponseCode());
	            }
				break;
			case POST:
	            if (conn.getResponseCode() != HTTP_RESPONSE_CODES.CREATED.code) {
	                throw new RuntimeException(". Failed : HTTP error code : " + conn.getResponseCode());
	            }
				break;
			case PUT:
				break;
			default:
	            if (conn.getResponseCode() != 200) {
	                throw new RuntimeException(". Failed : HTTP error code : " + conn.getResponseCode());
	            }
				break;
            	
            }
 
            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            String apiOutput = br.readLine();
            System.out.println(apiOutput);

            conn.disconnect();
 
            if (apiOutput.startsWith("[")) {
            	JsonArray array = parser.parse(apiOutput).getAsJsonArray();
            	for (JsonElement e : array) {
            		result.addValue(e.getAsJsonObject());
            	}
            } else {
            	result.addValue(parser.parse(apiOutput).getAsJsonObject());
            }
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
		RestClient.gson = gson;
	}

	public static JsonParser getParser() {
		return parser;
	}

	public static void setParser(JsonParser parser) {
		RestClient.parser = parser;
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
