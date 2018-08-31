package org.ocmc.olw.serializer;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import org.eclipse.jgit.util.FileUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ProcessBuilderTest {

	private static String dir = "";
	private static String token = "";
	private static String domain = "";
	private static String user = "";
	private static String project = "";
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		dir = System.getenv("REPO_DIR");
		token = System.getenv("GIT_TOKEN");
		user = System.getenv("GIT_USER");
		project = System.getenv("GIT_PROJECT");
		domain = System.getenv("GIT_DOMAIN");
	}

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() {
		StringBuffer result = new StringBuffer();
		result.append(ProcessBuilderTest.cloneGitlabProject(dir, token, domain, user, project));
		result.append("\n");
		result.append(ProcessBuilderTest.cloneGitlabProject(dir, token, domain, user, project));
		org.ocmc.ioc.liturgical.utils.FileUtils.writeFile(dir + "/" + project + "/test.txt", "Greetings");
		result.append(ProcessBuilderTest.gitAddCommitPush(dir, domain, "*.txt", "test"));
		System.out.println(result);
		assertTrue(result.length() > 0);
	}
	

	private static String cloneGitlabProject(
			String dir
			, String token
			, String domain
			, String user
			, String project
			) {
		StringBuffer result = new StringBuffer();
		try {
			//git clone https://ioauth2:$1@gitlab.liml.org/olwsys/db2json.git
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
				}
				pr.waitFor();
				
				in.close();
				result.append("\n OK");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result.toString();
	}
	
	
	private static String gitAddCommitPush(
			String dir
			, String domain
			, String filter
			, String msg
			) {
		StringBuffer result = new StringBuffer();
		result.append(gitAdd(dir, project, filter));
		result.append("\n");
		result.append(gitCommit(dir,project,msg));
		result.append("\n");
		result.append(gitPush(dir,project));
		result.append("\n");
		return result.toString();
	}

	private static String gitAdd(
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
				}
				pr.waitFor();
				
				in.close();
				result.append("\n OK");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result.toString();
	}
	
	private static String gitCommit(
			String dir
			, String domain
			, String msg
			) {
		StringBuffer result = new StringBuffer();
		try {
				ProcessBuilder  ps = new ProcessBuilder("git",  "commit", "-m", msg);
				ps.directory(new File(dir + "/" + domain));
				ps.redirectErrorStream(true);

				Process pr = ps.start();  

				BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
				String line;
				while ((line = in.readLine()) != null) {
					result.append(line);
				}
				pr.waitFor();
				
				in.close();
				result.append("\n OK");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result.toString();
	}
	private static String gitPush(
			String dir
			, String domain
			) {
		StringBuffer result = new StringBuffer();
		try {
				ProcessBuilder  ps = new ProcessBuilder("git",  "push", "origin");
				ps.directory(new File(dir + "/" + domain));
				ps.redirectErrorStream(true);

				Process pr = ps.start();  

				BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
				String line;
				while ((line = in.readLine()) != null) {
					result.append(line);
				}
				pr.waitFor();
				
				in.close();
				result.append("\n OK");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result.toString();
	}
	
}
