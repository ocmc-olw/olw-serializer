package org.ocmc.olw.serializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.eclipse.jgit.util.FileUtils;
import org.ocmc.ioc.liturgical.schemas.constants.HTTP_RESPONSE_CODES;
import org.ocmc.ioc.liturgical.schemas.models.supers.AbstractModel;
import org.ocmc.ioc.liturgical.schemas.models.ws.response.ResultJsonObjectArray;
import org.ocmc.ioc.liturgical.utils.ErrorUtils;
import org.ocmc.olw.serializer.models.GitlabProject;
import org.ocmc.olw.serializer.models.GitlabGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;

/**
 * A set of git commands using the Java ProcessBuilder
 * 
 * The clone is based on
 * 	git clone https://ioauth2:$1@$2/$3/$4.git
 * where
 * $1 = gitlab token
 * $2 = domain for gitlab
 * $3 = gitlab user
 * $4 = gitlab project
 * e.g.
 * 	git clone https://ioauth2:iAmATopSecretToken@gitlab.liml.org/olwsys/db2json.git
 * @author mac002
 *
 */
public class GitlabUtils {
	private static final Logger logger = LoggerFactory.getLogger(GitlabUtils.class);
	private static Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
	private static JsonParser parser = new JsonParser();
	
	private String domain = "";
	private String token = "";
	private String host = "";
	private String apiGroups = "";
	private String apiProjects = "";
	private String privateToken = "?private_token=";
	private Map<String,GitlabGroup> groupsMap = new TreeMap<String,GitlabGroup>(); // key = group path
	private Map<String,GitlabProject> projectsMap = new TreeMap<String,GitlabProject>(); // key = project path with namespace
	
	public GitlabUtils(String domain, String token) {
		this.domain = domain;
		this.token = token;
		this.privateToken = privateToken + token;
		this.host = "https://" + domain + "/api/v4/";
		this.apiGroups = this.host + "groups";
		this.apiProjects = this.host + "projects";
		this.createMaps();
	}
	
	/**
	 * Provides a map of all projects with the project name as the key
	 * and the https url as the value.
	 * @return the map
	 */
	public Map<String,GitlabProject> getProjectsMap() {
		return this.projectsMap;
	}
	
	private void createMaps() {
		ResultJsonObjectArray queryResult = this.getGroups();
		for (JsonObject o : queryResult.values) {
			GitlabGroup p = GitlabUtils.gson.fromJson(o.toString(), GitlabGroup.class);
			this.groupsMap.put(p.getFull_path(), p);
		}
		queryResult = this.getProjects();
		for (JsonObject o : queryResult.values) {
			GitlabProject p = GitlabUtils.gson.fromJson(o.toString(), GitlabProject.class);
			this.projectsMap.put(p.getPath_with_namespace(), p);
		}
	}
	
	public boolean existsProjectOnServer(String project) {
		String url = this.apiProjects + "/";
		try {
			url = url + URLEncoder.encode(project, "UTF-8");
			ResultJsonObjectArray qResult = get(url, GitlabProject.class);
			if (qResult.getStatus().getCode() == 200) {
				return true;
			} else {
				return false;
			}
		} catch (UnsupportedEncodingException e) {
			return false;
		}
	}

	public boolean existsProjectInMap(String project) {
		return this.projectsMap.containsKey(project);
	}
	
	/**
	 * For each non-system library found in the database 
	 * checks to see if there is a Gitlab project for it on the server.
	 * If not, creates it.
	 * For each library, clones or pulls based on whether exists in local repo
	 * 
	 * @param path path to directory containing the local git repos.  Must end with /
	 * @param list the names of the projects as found in the database.
	 */
	public void pullAllProjects(String path, List<String> list) {
		// check for existence of all listed projects on Gitlab server.
		// If missing, create it on server
		for (String project : list) {
			if (! this.existsProjectInMap(project)) { // create it
				this.createProject(project);
			}
		}
		// clone or pull all projects
		String tempPath = path;
		if (! tempPath.endsWith("/")) {
			tempPath = tempPath + "/";
		}
		String status = "";
		for (String project : list) {
			File f = new File(tempPath + project);
			if (f.exists()) { // pull
				status = GitlabUtils.pullGitlabProject(tempPath + project, this.domain, "olwsys", project, this.token);
			} else { // clone
				status = GitlabUtils.cloneGitlabProject(tempPath, this.domain, "olwsys", project, this.token);
			}
		}
	}
	
	public static String cloneGitlabProject(
			String dir
			, String domain
			, String user
			, String project
			, String token
			) {
		StringBuffer result = new StringBuffer();
		try {
			//https://gitlab.liml.org/serialized/db2ares/en_uk_tfm.git
			StringBuffer url = new StringBuffer();
			url.append("https://ioauth2:");
			url.append(token);
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
	
	
	public static String pullGitlabProject(
			String dir
			, String domain
			, String user
			, String project
			, String token
			) {
		StringBuffer result = new StringBuffer();
		try {
			StringBuffer url = new StringBuffer();
			url.append("https://ioauth2:");
			url.append(token);
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

	public static String gitAddCommitPush(
			String dir
			, String user
			, String project
			, String filter
			, String msg
			) {
		StringBuffer result = new StringBuffer();
		result.append(gitAdd(dir, project, filter));
		result.append("\n");
		result.append(gitCommit(dir,user,project,msg));
		result.append("\n");
		result.append(gitPush(dir,project));
		result.append("\n");
		return result.toString();
	}
	
	public static String gitCommitPush(
			String dir
			, String user
			, String project
			, String filter
			, String msg
			) {
		StringBuffer result = new StringBuffer();
		result.append(gitCommit(dir,user,project,msg));
		result.append("\n");
		result.append(gitPush(dir,project));
		result.append("\n");
		return result.toString();
	}
	
	public String addCommitPushAllProjects(
			String dir
			, String msg
			) {
		StringBuffer result = new StringBuffer();
		for (GitlabProject p : this.projectsMap.values()) {
			result.append("Push for ");
			result.append(p.getName());
			result.append(": ");
			result.append(GitlabUtils.gitAddCommitPush(dir, "olwsys", p.getName(), ".", msg));
			result.append("\n");
		}
		return result.toString();
	}

	public String commitPushAllProjects(
			String dir
			, String msg
			) {
		StringBuffer result = new StringBuffer();
		for (GitlabProject p : this.projectsMap.values()) {
			result.append("Push for ");
			result.append(p.getName());
			result.append(": ");
			result.append(GitlabUtils.gitAddCommitPush(dir, "olwsys", p.getName(), ".", msg));
			result.append("\n");
		}
		return result.toString();
	}

	public static String gitAdd(
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
	
	public static String gitCommit(
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
	
	public static String gitPush(
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
	
	public ResultJsonObjectArray createProject(String name) {
		ResultJsonObjectArray result = new ResultJsonObjectArray(false);
		try {
			result = postAsJson(this.apiProjects + "/?name=" + URLEncoder.encode(name, "UTF-8"));
			JsonObject o = result.getFirstObject().get("node").getAsJsonObject();
			GitlabProject p = GitlabUtils.gson.fromJson(o.toString(), GitlabProject.class);
			this.projectsMap.put(name, p);
		} catch (Exception e) {
			ErrorUtils.report(logger, e);
		}
		return result;
	}
	
	public ResultJsonObjectArray createProjectInGroup(String group, String name) {
		ResultJsonObjectArray result = new ResultJsonObjectArray(false);
		if (this.groupsMap.containsKey(group)) {
			try {
				int groupId = this.groupsMap.get(group).id;
				result = postAsJson(this.apiProjects + "/?namespace_id=" + groupId + "&name=" + URLEncoder.encode(name, "UTF-8"));
				if (result.getStatus().code == 200) {
					JsonObject o = result.getFirstObject().get("node").getAsJsonObject();
					GitlabProject p = GitlabUtils.gson.fromJson(o.toString(), GitlabProject.class);
					this.projectsMap.put(p.getPath_with_namespace(), p);
				} else {
					logger.error("Could not create project " + group + "/" + name + ": " + result.getStatus().code + " - " + result.getStatus().getDeveloperMessage());
				}
			} catch (Exception e) {
				ErrorUtils.report(logger, e);
			}
		} else {
			result.setStatusCode(HTTP_RESPONSE_CODES.NOT_FOUND.code);
			result.setStatusUserMessage(HTTP_RESPONSE_CODES.NOT_FOUND.message + " " + group);
		}
		return result;
	}
	
	public String getIdForProject(String name) {
		if (this.projectsMap.containsKey(name)) {
			return this.projectsMap.get(name).getId();
		} else {
			return "";
		}
	}
	
	public void deleteAllProjects() {
		// create a new list because each delete will remove the project 
		// from the project map.
		List<String> names = new ArrayList<String>();
		for (GitlabProject p : this.projectsMap.values()) {
			names.add(p.getPath_with_namespace());
		}
		for (String name : names) {
			try {
				this.deleteProject(name);
				Thread.sleep(1000); // if send too quickly says OK but doesn't delete
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public ResultJsonObjectArray deleteProject(String name) {
		String id = this.getIdForProject(name);
		ResultJsonObjectArray result = new ResultJsonObjectArray(false);
		if (id.length() > 0) {
			result = deleteAsJson(this.apiProjects + "/" + id);
			if (result.getStatus().getCode() == 202) {
				this.projectsMap.remove(name);
				logger.info("Deleted Gitlab project " + name);
			} else {
				logger.error("Error deleting Gitlab project " + name + " " + result.getStatus().developerMessage);
			}
		} else {
			result.setStatusCode(HTTP_RESPONSE_CODES.NOT_FOUND.code);
			result.setStatusMessage(HTTP_RESPONSE_CODES.NOT_FOUND.message);
		}
		return result;
	}
	
	public ResultJsonObjectArray getProjects() {
		ResultJsonObjectArray result = get(this.apiProjects, GitlabProject.class);
		return result;
	}
	
	///groups
	public ResultJsonObjectArray getGroups() {
		ResultJsonObjectArray result = get(this.apiGroups, GitlabGroup.class);
		return result;
	}
	
	
	public ResultJsonObjectArray getProjectsForUser(String user) {
		//GET /users/:user_id/projects
		ResultJsonObjectArray result = get(this.host + "users/"+ user + "/projects", GitlabProject.class);
		return result;
	}
	
	public int getUserIdFor(String firstname, String lastname) {
		int result = -1;
		// GET /users?username=:username
		String name = "";
		if (firstname.length() > 0 && lastname.length() > 0) {
			name = firstname + "_" + lastname;
		} else {
			name = firstname + lastname; // one of these will = ""
		}
		ResultJsonObjectArray queryResult = this.getAsJson(this.host + "users?username="+ name);
		JsonObject first = queryResult.getFirstObject();
		if (first.has("node")) {
			for (JsonElement e : first.get("node").getAsJsonArray()) {
				JsonObject o = e.getAsJsonObject();
				result = o.get("id").getAsInt();
			}
		}
		return result;
	}
	
	public ResultJsonObjectArray getUsers() {
		ResultJsonObjectArray result = null;
		return result;
	}
	

	
	/**
	 * If project exists already, will return it.  If not, creates it.
	 * @param user the user
	 * @param project the project
	 * @return the array
	 */
	public ResultJsonObjectArray getProjectUrl(String user, String project) {
		// first see if it exists
		ResultJsonObjectArray result = this.getProjectsForUser(user);
		// if not create one using: POST /projects/user/:user_id
		// user_id is an integer
		return result;
	}
	
	public ResultJsonObjectArray get(
			String url
			, Class<? extends AbstractModel> model
			) {
		ResultJsonObjectArray result = new ResultJsonObjectArray(true);
		result.setQuery(url);
		try {
			ResultJsonObjectArray getResult = getAsJson(url);
			if (getResult.status.getCode() == 200) {
				JsonArray responseArray = getResult.getFirstObject().get("node").getAsJsonArray();
				for (JsonElement e : responseArray) {
					JsonObject o = e.getAsJsonObject();
					AbstractModel details = gson.fromJson(
							e.getAsJsonObject().toString()
							, model
						);
					result.addValue(details.toJsonObject());
				}
			} else {
				result.setStatus(getResult.getStatus());
			}
		} catch (Exception e) {
			ErrorUtils.report(logger, e);
		}
		return result;
	}
	
	public ResultJsonObjectArray getAsJson(
			String url
			) {
		ResultJsonObjectArray result = new ResultJsonObjectArray(true);
		result.setQuery(url);
		HttpResponse<JsonNode> responseGet = null;
		JsonObject o = new JsonObject();
		JsonElement je = null;
		JsonNode node = null;
		try {
			String privateToken = "";
			if (url.contains("?")) {
				privateToken = "&private_token=" + this.token + "&simple=true";
			} else {
				privateToken = "?private_token=" + this.token + "&simple=true";
			}
			boolean hasNextPage = true;
			int pageNbr = 0;
			while (hasNextPage) {
				pageNbr++;
				List<String> xNextPage = new ArrayList<String>();
				String requestUrl = url + privateToken + "&page=" + pageNbr;
				responseGet = Unirest.get(requestUrl).asJson();
				int statusCode = responseGet.getStatus();
				String statusBody = responseGet.getStatusText();
				JsonNode body = responseGet.getBody();
				if (responseGet.getStatus() == 200) {
					xNextPage = responseGet.getHeaders().get("X-Next-Page");
					node = responseGet.getBody();
					je = parser.parse(node.toString());
					o.add("node", je);
					result.addValue(o);
				} else {
					result.getStatus().code = responseGet.getStatus();
					result.getStatus().setMessage(responseGet.getStatusText());
				}
				if (xNextPage.isEmpty() && xNextPage.size() > 0) {
					try {
						hasNextPage = xNextPage.get(0) != null && xNextPage.get(0).length() > 0;
					} catch (Exception e) {
						hasNextPage = false;
					}
				} else {
					hasNextPage = false;
				}
			}
		} catch (Exception e) {
			ErrorUtils.report(logger, e);
		}
		return result;
	}


	public ResultJsonObjectArray postAsJson(
			String url
			) {
		ResultJsonObjectArray result = new ResultJsonObjectArray(true);
		result.setQuery(url);
		HttpResponse<JsonNode> responseGet = null;
		JsonObject o = new JsonObject();
		JsonElement je = null;
		JsonNode node = null;
		try {
			String privateToken = "";
			if (url.contains("?")) {
				privateToken = "&private_token=" + this.token + "&simple=true";
			} else {
				privateToken = "?private_token=" + this.token + "&simple=true";
			}
			responseGet = Unirest.post(url + privateToken).asJson();
			if (responseGet.getStatus() == 201) {
				node = responseGet.getBody();
				je = parser.parse(node.toString());
				o.add("node", je);
				result.addValue(o);
			} else {
				result.getStatus().code = responseGet.getStatus();
				result.getStatus().setMessage(responseGet.getStatusText());
			}
		} catch (Exception e) {
			ErrorUtils.report(logger, e);
		}
		return result;
	}

	public ResultJsonObjectArray deleteAsJson(
			String url
			) {
		ResultJsonObjectArray result = new ResultJsonObjectArray(true);
		result.setQuery(url);
		HttpResponse<JsonNode> responseGet = null;
		JsonObject o = new JsonObject();
		JsonElement je = null;
		JsonNode node = null;
		try {
			String privateToken = "";
			if (url.contains("?")) {
				privateToken = "&private_token=" + this.token;
			} else {
				privateToken = "?private_token=" + this.token;
			}
			responseGet = Unirest.delete(url + privateToken).asJson();
			result.setStatusCode(responseGet.getStatus());
			result.setStatusMessage(responseGet.getStatusText());
			if (responseGet.getStatus() == 202) {
				node = responseGet.getBody();
				je = parser.parse(node.toString());
				o.add("node", je);
				result.addValue(o);
			}
		} catch (Exception e) {
			ErrorUtils.report(logger, e);
		}
		return result;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public String printPretty(JsonObject o) {
		try {
			return new GsonBuilder().setPrettyPrinting().create().toJson(o);
		} catch (Exception e) {
			return o.toString();
		}
	}
}
