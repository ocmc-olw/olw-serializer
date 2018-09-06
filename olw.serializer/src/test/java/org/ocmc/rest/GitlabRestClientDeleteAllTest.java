package org.ocmc.rest;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.ocmc.ioc.liturgical.schemas.constants.HTTP_RESPONSE_CODES;
import org.ocmc.ioc.liturgical.schemas.models.ws.response.ResultJsonObjectArray;

public class GitlabRestClientDeleteAllTest {
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
	public void testDeleteAllProjects() {
		try {
			String group = "test";
			GitlabRestClient rc = new GitlabRestClient(GitlabRestClientDeleteAllTest.url, GitlabRestClientDeleteAllTest.token);
			ResultJsonObjectArray result = null;
			result = rc.postProject(group, "test1");
			result = rc.postProject(group, "test2");
			result = rc.deleteAllProjectsInGroup(group);
			assertTrue(result.getStatus().code == HTTP_RESPONSE_CODES.OK.code);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
}
