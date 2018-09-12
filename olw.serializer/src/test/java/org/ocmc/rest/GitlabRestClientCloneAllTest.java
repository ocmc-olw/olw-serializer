package org.ocmc.rest;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.ocmc.ioc.liturgical.schemas.constants.HTTP_RESPONSE_CODES;
import org.ocmc.ioc.liturgical.schemas.models.ws.response.ResultJsonObjectArray;
import org.ocmc.rest.client.GitlabRestClient;

public class GitlabRestClientCloneAllTest {
	private static String user = "";
	private static String group = "";
	private static String token = "";
	private static String dir = "";
	private static String url = "https://gitlab.liml.org/api/v4/";
	
	@Before
	public void setUp() throws Exception {
		dir = System.getenv("DIR");
		group = System.getenv("GROUP");
		token = System.getenv("TOKEN");
		user = System.getenv("UID");
		url = System.getenv("URL");
		assertTrue(token != null);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testCloneAllProjects() {
		try {
			GitlabRestClient rc = new GitlabRestClient(GitlabRestClientCloneAllTest.url, GitlabRestClientCloneAllTest.token);
			ResultJsonObjectArray result = null;
			result = rc.cloneAllProjectsInGroup(dir, user, group);
			assertTrue(result.getStatus().code == HTTP_RESPONSE_CODES.OK.code);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
}
