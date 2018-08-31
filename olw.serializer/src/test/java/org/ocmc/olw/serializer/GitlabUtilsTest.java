package org.ocmc.olw.serializer;

import static org.junit.Assert.*;

import java.io.File;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ocmc.ioc.liturgical.schemas.models.ws.response.ResultJsonObjectArray;

import com.google.gson.JsonObject;

public class GitlabUtilsTest {
	
	private static String domain = "";
	private static String token = "";

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		token = System.getenv("GIT_TOKEN");
		domain = System.getenv("GIT_DOMAIN");
	}

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void getProjects() {
		GitlabUtils utils = new GitlabUtils(domain, token);
		ResultJsonObjectArray result = utils.getProjects();
		System.out.println(result.getStatus().getDeveloperMessage());
		for (JsonObject o : result.getValues()) {
			System.out.println(utils.printPretty(o));
		}
		assertTrue(result.valueCount > 0);
	}
	
	@Test
	public void getProjectsMap() {
		GitlabUtils utils = new GitlabUtils(domain, token);
		assertTrue(utils.existsProject("db2json"));
	}

	@Test
	public void getUserId() {
		GitlabUtils utils = new GitlabUtils(domain, token);
		int result = utils.getUserIdFor("olwsys", "");
		assertTrue(result == 4);
	}

	@Test
	public void createProject() {
		GitlabUtils utils = new GitlabUtils(domain, token);
		ResultJsonObjectArray result = utils.createProject("test2");
		assertTrue(result.getStatus().code == 201); // created
		result = utils.deleteProject("test");
		assertTrue(result.getStatus().code == 200); 
	}

//	@Test
//	public void deleteProject() {
//		GitlabUtils utils = new GitlabUtils(domain, token);
//		ResultJsonObjectArray result = utils.deleteProject("test2");
//		assertTrue(result.getStatus().code == 200);
//	}

	@Test
	public void pullProject() {
		File f = new File("/Users/mac002/canBeRemoved/repoTest/db2json");
		String result = GitlabUtils.cloneGitlabProject(f.getParent(), domain, "olwsys", "db2json", token);
		System.out.println(result);
		result = GitlabUtils.pullGitlabProject(f.getPath(), domain, "olwsys", "db2json", token);
		System.out.println(result);
		assertTrue(f.exists());
	}

}
