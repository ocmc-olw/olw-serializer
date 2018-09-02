package org.ocmc.rest;

import org.ocmc.ioc.liturgical.schemas.constants.HTTP_RESPONSE_CODES;
import org.ocmc.ioc.liturgical.schemas.models.ws.response.ResultJsonObjectArray;

public class GitlabRestClient extends GenericRestClient {
	public static enum TOPICS  {groups, projects};

    public GitlabRestClient(String url, String token) {
    	super(url, token);
    }
    
    public ResultJsonObjectArray getGroup(String name) {
    	return this.request(METHODS.DELETE, TOPICS.groups.name(), name, "");
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
