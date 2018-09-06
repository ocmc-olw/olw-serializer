package org.ocmc.olw.serializer.utils;

import org.ocmc.olw.serializer.GitlabUtils;
import org.ocmc.rest.GitlabRestClient;
import org.ocmc.rest.RestInitializationException;

public class DeleteAllGitlabProjects {

	
	/**
	 * DANGER!!!  This deletes all repos in the specified domain
	 * @param args not used
	 */
	public static void main(String[] args) {
		String repoDomain = System.getenv("REPO_DOMAIN");
		String repoToken = System.getenv("REPO_TOKEN");
		GitlabRestClient gitUtils;
		try {
			gitUtils = new GitlabRestClient(repoDomain,repoToken);
	         gitUtils.deleteAllProjectsInGroup("serialized/db2ares");
			 gitUtils.deleteAllProjectsInGroup("serialized/db2json/gr_gr_cog");
			 gitUtils.deleteAllProjectsInGroup("serialized/db2json/links");
			 gitUtils.deleteAllProjectsInGroup("serialized/db2json/linkprops");
			 gitUtils.deleteAllProjectsInGroup("serialized/db2json/nodes");
		} catch (RestInitializationException e) {
			e.printStackTrace();
		}
	}

}
