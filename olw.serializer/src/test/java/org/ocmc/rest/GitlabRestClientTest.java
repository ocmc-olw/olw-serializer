package org.ocmc.rest;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.ocmc.ioc.liturgical.schemas.constants.HTTP_RESPONSE_CODES;
import org.ocmc.ioc.liturgical.schemas.models.ws.response.ResultJsonObjectArray;

public class GitlabRestClientTest {
	private static String token = "";
	private static String url = "https://gitlab.liml.org/api/v4/";
	
	@Before
	public void setUp() throws Exception {
		token = System.getenv("TOKEN");
		assertTrue(token != null);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetGroups() {
		GitlabRestClient rc = new GitlabRestClient(GitlabRestClientTest.url, GitlabRestClientTest.token);
		ResultJsonObjectArray result = rc.get(GitlabRestClient.TOPICS.groups.name(),"","");
		result.setPrettyPrint(true);
		System.out.println(result.toJsonString());
		assertTrue(result.getStatus().code == HTTP_RESPONSE_CODES.OK.code);
	}
	@Test
	public void testGetGroupSerializedDb2JsonNodes() {
		RestClient rc = new RestClient(GitlabRestClientTest.url, GitlabRestClientTest.token);
		ResultJsonObjectArray result = rc.get(
				RestClient.TOPICS.groups
				, "serialized/db2json/nodes"
				, ""
				);
		result.setPrettyPrint(true);
		System.out.println(result.toJsonString());
		assertTrue(result.getStatus().code == HTTP_RESPONSE_CODES.OK.code);
	}

	
//	@Test
//	public void testGetProjectSerializedDb2aresTest() {
//		GitlabRestClient rc = new GitlabRestClient(GitlabRestClientTest.url, GitlabRestClientTest.token);
//		ResultJsonObjectArray result = rc.get(
//				GitlabRestClient.TOPICS.projects
//				, "serialized/db2ares/test"
//				,""
//				);
//		result.setPrettyPrint(true);
//		System.out.println(result.toJsonString());
//		assertTrue(result.getStatus().code == HTTP_RESPONSE_CODES.OK.code);
//	}
	@Test
	public void testGetProjects() {
		GitlabRestClient rc = new GitlabRestClient(GitlabRestClientTest.url, GitlabRestClientTest.token);
		ResultJsonObjectArray result = rc.get(
				GitlabRestClient.TOPICS.projects.name()
				,""
				, ""
				);
		result.setPrettyPrint(true);
		System.out.println(result.toJsonString());
		assertTrue(result.getStatus().code == HTTP_RESPONSE_CODES.OK.code);
	}

	@Test
	public void testPostAndDeleteProject() {
		GitlabRestClient rc = new GitlabRestClient(
				GitlabRestClientTest.url
				, GitlabRestClientTest.token
				);
		String group = "serialized/db2json/nodes"; 
		String project = "test";
		String fullPathProject = group + "/" + project;
		ResultJsonObjectArray result = rc.postProject(group, project);
		result.setPrettyPrint(true);
		System.out.println(result.toJsonString());
		int putCode = result.getStatus().code;
		result = rc.deleteProject(fullPathProject);
		result.setPrettyPrint(true);
		System.out.println(result.toJsonString());
		int deleteCode = result.getStatus().code;
		assertTrue(putCode == 201 && deleteCode == 202);
	}
}
