package org.ocmc.rest;

import java.util.Map;
import java.util.TreeMap;

import org.ocmc.ioc.liturgical.schemas.constants.HTTP_RESPONSE_CODES;
import org.ocmc.ioc.liturgical.schemas.models.ws.response.ResultJsonObjectArray;
import org.ocmc.ioc.liturgical.utils.ErrorUtils;
import org.ocmc.olw.serializer.models.GitlabGroup;
import org.ocmc.olw.serializer.models.GitlabProject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

public class GitlabRestClient extends GenericRestClient {
	private static final Logger logger = LoggerFactory.getLogger(GitlabRestClient.class);
	public static enum TOPICS  {groups, projects};
    private Map<String, GitlabGroup> groupsMap = new TreeMap<String,GitlabGroup>();
    private Map<String, GitlabProject>  projectsMap = new TreeMap<String,GitlabProject>();


    public GitlabRestClient(String url, String token) {
    	super(url, token);
    	this.loadMaps();
    }
    
    private void loadMaps() {
    	try {
    		ResultJsonObjectArray queryResult = this.get("groups","","");
    		for (JsonObject o : queryResult.values) {
    			try {
    				GitlabGroup p = GitlabRestClient.gson.fromJson(
    						o.toString()
    						, GitlabGroup.class
    						);
    				this.groupsMap.put(p.getFull_path(), p);
    			} catch (Exception e) {
    				ErrorUtils.report(logger, e);
    			}
    		}
    		queryResult = this.get("projects", "", "");
    		for (JsonObject o : queryResult.values) {
    			try {
        			GitlabProject p = GenericRestClient.gson.fromJson(
        					o.toString()
        					, GitlabProject.class
        					);
        			this.projectsMap.put(p.getPath_with_namespace(), p);
    			} catch (Exception e) {
    				ErrorUtils.report(logger, e);
    			}
    		}
    	} catch (Exception e) {
    		ErrorUtils.report(logger, e);
    	}
    }

    public ResultJsonObjectArray getGroup(String name) {
    	return this.request(METHODS.GET, TOPICS.groups.name(), name, "");
    }

    public ResultJsonObjectArray deleteProject(String name) {
    	return this.request(METHODS.DELETE, TOPICS.projects.name(), name, "");
    }

    public ResultJsonObjectArray postProject(String group, String project) {
    	ResultJsonObjectArray theGroup = this.get(TOPICS.groups.name(), group,"");
    	if (theGroup.getStatus().code == HTTP_RESPONSE_CODES.OK.code) {
        	int id = theGroup.getFirstObject().get("id").getAsInt();
        	return this.request(
        			METHODS.POST
        			, TOPICS.projects.name()
        			, ""
        			, "?name=" + project + "&namespace_id=" + id
        			);
    	} else {
    		return theGroup;
    	}
    }

}
