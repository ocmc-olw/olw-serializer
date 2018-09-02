package org.ocmc.rest;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.ocmc.ioc.liturgical.schemas.constants.HTTP_RESPONSE_CODES;
import org.ocmc.ioc.liturgical.schemas.models.ws.response.ResultJsonObjectArray;

public class RestClientTest {

	private static String token = "";
	private static String url = "https://gitlab.liml.org/api/v4/";
	
	@Before
	public void setUp() throws Exception {
		token = System.getenv("TOKEN");
	}


	@Test
	public void testGetGroupSerializedDb2JsonNodes() {
		try {
			RestClient rc = new RestClient(RestClientTest.url, RestClientTest.token);
			ResultJsonObjectArray result = rc.get(
					RestClient.TOPICS.groups
					, "serialized/db2json/nodes"
					, ""
					);
			result.setPrettyPrint(true);
			System.out.println(result.toJsonString());
			assertTrue(result.getStatus().code == HTTP_RESPONSE_CODES.OK.code);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

}
