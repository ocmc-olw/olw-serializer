package org.ocmc.olw.serializer.utils;

import org.ocmc.olw.serializer.GitlabUtils;

public class DeleteAllGitlabProjects {

	
	/**
	 * DANGER!!!  This deletes all repos in the specified domain
	 * @param args not used
	 */
	public static void main(String[] args) {
		String repoDomain = System.getenv("REPO_DOMAIN");
		String repoToken = System.getenv("REPO_TOKEN");
		GitlabUtils utils = new GitlabUtils(repoDomain,repoToken);
		utils.deleteAllProjects();
	}

}
