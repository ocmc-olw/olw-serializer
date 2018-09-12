package org.ocmc.rest.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import org.eclipse.jgit.util.FileUtils;
import org.ocmc.ioc.liturgical.schemas.constants.HTTP_RESPONSE_CODES;
import org.ocmc.ioc.liturgical.schemas.models.ws.response.ResultJsonObjectArray;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Provides rest api to Gitlab server.
 * Also provides ability to add, commit, push, pull local git repositories
 * @author mac002
 *
 */
public class GitlabRestClient extends GenericRestClient {
	public static enum TOPICS  {groups, projects};

    public GitlabRestClient(String url, String token) throws RestInitializationException {
    	super(
    			url.contains("api") ? url : "https://" + url + "/api/v4/"
    			, token
    			);
    }

    public boolean existsGroup(String name) {
    	boolean result = false;
    	try {
        	ResultJsonObjectArray queryResult = this.getGroup(name);
        	result = queryResult.getStatus().code == HTTP_RESPONSE_CODES.OK.code 
        			&& queryResult.getFirstObjectValueAsObject().get("full_path").getAsString().equals(name);
    	} catch (Exception e) {
    		result = false;
    	}
    	return result;
    }
    
    public boolean existsProject(String name) {
    	boolean result = false;
    	return result;
    }
    
    public ResultJsonObjectArray getGroup(String name) {
    	return this.request(METHODS.GET, TOPICS.groups.name(), name, "");
    }
    
    public ResultJsonObjectArray getGroups() {
    	return this.request(METHODS.GET, TOPICS.groups.name(), "", "");
    }

    public ResultJsonObjectArray cloneAllProjectsInGroup(String dir, String user, String group) {
    	ResultJsonObjectArray result = new ResultJsonObjectArray(false);
    	ResultJsonObjectArray groupsQuery = this.getGroups();
    	if (groupsQuery.getStatus().code == HTTP_RESPONSE_CODES.OK.code) {
    		for (JsonObject go : groupsQuery.values) {
    			String goGroup = go.get("full_path").getAsString();
    			if (goGroup.startsWith(group)) {
                	ResultJsonObjectArray groupQuery = this.getGroup(goGroup);
                	if (groupQuery.getStatus().code == HTTP_RESPONSE_CODES.OK.code) {
                		JsonObject o = groupQuery.getFirstObjectValueAsObject();
                		JsonArray projects = o.get("projects").getAsJsonArray();
                		for (JsonElement e : projects) {
                			JsonObject po = e.getAsJsonObject();
                			String project = po.get("name").getAsString();
                			this.cloneGitlabProject(dir + "/" + goGroup, goGroup, project);
                		}
                	}
    			}
    		}
    	}
    	return result;
    }
    public ResultJsonObjectArray deleteAllProjectsInGroup(String group) {
    	ResultJsonObjectArray result = new ResultJsonObjectArray(false);
    	ResultJsonObjectArray groupQuery = this.getGroup(group);
    	if (groupQuery.getStatus().code == HTTP_RESPONSE_CODES.OK.code) {
    		JsonObject o = groupQuery.getFirstObjectValueAsObject();
    		JsonArray projects = o.get("projects").getAsJsonArray();
    		for (JsonElement e : projects) {
    			JsonObject project = e.getAsJsonObject();
    			this.deleteProject(project.get("path_with_namespace").getAsString());
    		}
    	}
    	return result;
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
    
	public String gitAddCommitPush(
			String dir
			, String user
			, String project
			, String filter
			, String msg
			) {
		StringBuffer result = new StringBuffer();
		result.append(this.gitAdd(dir, project, filter));
		result.append("\n");
		result.append(this.gitCommit(dir,user,project,msg));
		result.append("\n");
		result.append(this.gitPush(dir,project));
		result.append("\n");
		return result.toString();
	}
	
	public String gitAdd(
			String dir
			, String project
			, String filter
			) {
		StringBuffer result = new StringBuffer();
		try {
				ProcessBuilder  ps = new ProcessBuilder("git",  "add", filter);
				ps.directory(new File(dir + "/" + project));
				ps.redirectErrorStream(true);

				Process pr = ps.start();  

				BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
				String line;
				while ((line = in.readLine()) != null) {
					result.append(line);
					result.append(" ");
				}
				pr.waitFor();
				
				in.close();
				result.append("\n OK");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result.toString();
	}
	
	public String gitCommit(
			String dir
			, String user
			, String project
			, String msg
			) {
		StringBuffer result = new StringBuffer();
		try {
				ProcessBuilder  ps = new ProcessBuilder("git",  "-c", "user.name=olwsys", "-c", "user.email=olw@ocmc.org", "commit", "-m", msg);
				ps.directory(new File(dir + "/" + project));
				ps.redirectErrorStream(true);

				Process pr = ps.start();  

				BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
				String line;
				while ((line = in.readLine()) != null) {
					result.append(line);
					result.append(" ");
				}
				pr.waitFor();
				
				in.close();
				result.append("\n OK");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result.toString();
	}
	
	public String gitPush(
			String dir
			, String project
			) {
		StringBuffer result = new StringBuffer();
		try {
				ProcessBuilder  ps = new ProcessBuilder("git",  "push", "origin");
				ps.directory(new File(dir + "/" + project));
				ps.redirectErrorStream(true);

				Process pr = ps.start();  

				BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
				String line;
				while ((line = in.readLine()) != null) {
					result.append(line);
					result.append(" ");
				}
				pr.waitFor();
				
				in.close();
				result.append("\n OK");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result.toString();
	}
	
	public String pullGitlabProject(
			String dir
			, String user
			, String project
			) {
		return this.pullGitlabProject(dir, this.getApiDomain(), user, project);
	}
	
	public String pullGitlabProject(
			String dir
			, String domain
			, String user
			, String project
			) {
		StringBuffer result = new StringBuffer();
		try {
			StringBuffer url = new StringBuffer();
			url.append("https://ioauth2:");
			url.append(this.getToken());
			url.append("@");
			url.append(domain);
			url.append("/");
			url.append(user);
			url.append("/");
			url.append(project);
			url.append(".git");
				ProcessBuilder  ps = new ProcessBuilder("git",  "pull", url.toString());
				File file = new File(dir);
				if (! file.exists()) {
					FileUtils.mkdirs(file);
				}
				ps.directory(file);
				ps.redirectErrorStream(true);

				Process pr = ps.start();  

				BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
				String line;
				while ((line = in.readLine()) != null) {
					result.append(line);
					result.append(" ");
				}
				pr.waitFor();
				
				in.close();
				result.append("\n OK");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result.toString();
	}

	public String cloneGitlabProject(
			String dir
			, String domain
			, String user
			, String project
			) {
		StringBuffer result = new StringBuffer();
		try {
			//https://gitlab.liml.org/serialized/db2ares/en_uk_tfm.git
			StringBuffer url = new StringBuffer();
			url.append("https://ioauth2:");
			url.append(this.getToken());
			url.append("@");
			url.append(domain);
			url.append("/");
			url.append(user);
			url.append("/");
			url.append(project);
			url.append(".git");
				ProcessBuilder  ps = new ProcessBuilder("git",  "clone", url.toString());
				File file = new File(dir);
				if (! file.exists()) {
					FileUtils.mkdirs(file);
				}
				ps.directory(file);
				ps.redirectErrorStream(true);

				Process pr = ps.start();  

				BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
				String line;
				while ((line = in.readLine()) != null) {
					result.append(line);
					result.append(" ");
				}
				pr.waitFor();
				
				in.close();
				result.append("\n OK");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result.toString();
	}

	public String cloneGitlabProject(
			String dir
			, String user
			, String project
			) {
		return this.cloneGitlabProject(dir, this.getApiDomain(), user, project);
	}
}
