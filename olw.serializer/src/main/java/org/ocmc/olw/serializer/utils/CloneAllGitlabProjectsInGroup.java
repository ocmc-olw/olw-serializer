package org.ocmc.olw.serializer.utils;

import org.ocmc.rest.client.GitlabRestClient;
import org.ocmc.rest.client.RestInitializationException;

public class CloneAllGitlabProjectsInGroup {

	
	/**
	 * If the directory to contain the pulled projects exists,
	 * it will first be cleaned (i.e. all subdirectories and files deleted)
	 * @param args not used
	 */
	public static void main(String[] args) {
		String domain = System.getenv("DOMAIN");
		String user = System.getenv("UID");
		String token = System.getenv("TOKEN");
		String group = System.getenv("GROUP");
		String dir = System.getenv("DIR");
		GitlabRestClient gitUtils;
		try {
			gitUtils = new GitlabRestClient(domain,token);
			gitUtils.cloneAllProjectsInGroup(dir, user, group);
		} catch (RestInitializationException e) {
			e.printStackTrace();
		}
	}

}
