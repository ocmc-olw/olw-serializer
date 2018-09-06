package org.ocmc.olw.serializer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.ocmc.ioc.liturgical.utils.FileUtils;

public class Compare {
	Map<String,String> github = new TreeMap<String,String>();
	Map<String,String> gitlab = new TreeMap<String,String>();
	Map<String,String> pathMap = new TreeMap<String,String>();
	String pathMapPath = "";
	String reportPath = "";
	String githubPath = "";
	String gitlabPath = "";

	public Compare(String githubPath, String gitlabPath, String reportPath, String pathMapPath) {
		this.githubPath = githubPath;
		this.gitlabPath = gitlabPath;
		this.reportPath = reportPath;
		this.pathMapPath = pathMapPath;
		this.github = this.createTopicKeyListFromAres(this.githubPath);
		this.gitlab = this.createTopicKeyListFromAres(this.gitlabPath);
		this.compareGithubToGitlab();
		this.compareGitlabToGithub();
		this.createPathMapFromAres(this.pathMapPath);
	}
	
	private void compareGithubToGitlab() {
		StringBuffer sb = new StringBuffer();
		for (Entry<String,String> entry : this.github.entrySet()) {
			if (entry.getKey().equals("he.h.m3~LatreveinZontiTheo.mode")) {
				System.out.println("");
			}
			if (! this.gitlab.containsKey(entry.getKey())) {
				sb.append(entry.getKey());
				sb.append(" = ");
				sb.append(entry.getValue());
				sb.append("\n");
			}
		}
		FileUtils.writeFile(this.reportPath + "/NotInGitlab.txt", sb.toString());
	}
	private void compareGitlabToGithub() {
		StringBuffer sb = new StringBuffer();
		for (Entry<String,String> entry : this.gitlab.entrySet()) {
			if (entry.getKey().equals("he.h.m3~LatreveinZontiTheo.mode")) {
				System.out.println("");
			}
			if (! this.github.containsKey(entry.getKey())) {
				sb.append(entry.getKey());
				sb.append(" = ");
				sb.append(entry.getValue());
				sb.append("\n");
			}
		}
		FileUtils.writeFile(this.reportPath + "/NotInGithub.txt", sb.toString());
	}

	private Map<String,String> createTopicKeyListFromAres(String path) {
		Map<String,String> list = new TreeMap<String,String>();
		for (File f : FileUtils.getFilesFromSubdirectories(path, "ares")) {
			String [] fileParts = f.getName().split("_");
			String topic = fileParts[0].trim();
			for (String line : FileUtils.linesFromFile(f)) {
				if (line.trim().length() > 0 && ! line.startsWith("A_Resource")) {
					String [] lineParts = line.split(" = ");
					if (lineParts.length == 2) {
						String key = lineParts[0].trim();
						list.put(topic + "~" + key,lineParts[1].trim());
					}
				}
			}
		}
		return list;
	}
	private Map<String,String> createPathMapFromAres(String path) {
		Map<String,String> list = new TreeMap<String,String>();
		for (File f : FileUtils.getFilesFromSubdirectories(path, "ares")) {
			try {
				String [] fileParts = f.getName().split("_");
				String topic = fileParts[0].trim();
				String subPath = f.getParent().split("_gr_GR_cog/")[1];
				list.put(topic, subPath);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		StringBuffer sb = new StringBuffer();
		sb.append("package org.ocmc.olw.serializer;\n\n");

		sb.append("import java.util.Map;\n");
		sb.append("import java.util.TreeMap;\n\n");

		sb.append("public class PathMap {\n");
		sb.append("\tprivate Map<String,String> pathForTopic = new TreeMap<String,String>();\n\n");
			
		sb.append("\tpublic PathMap() {\n");
		sb.append("\t\tthis.initializeMap();\n");
		sb.append("}\n\n");
			
		sb.append("\tprivate void initializeMap() {\n");
		for (Entry<String,String> entry : list.entrySet()) {
			sb.append("\t\tthis.pathForTopic.put(\"");
			sb.append(entry.getKey());
			sb.append("\",\"");
			sb.append(entry.getValue());
			sb.append("\");\n");
		}
		sb.append("\t}\n");
		sb.append("}\n");
		FileUtils.writeFile("PathMap.java", sb.toString());
		return list;
	}

	public static void main(String [] args) {
		String githubPath = "/Users/mac002/git/ages/ares/dcs/ages-alwb-library-en-us-repass";
		String gitlabPath = "/private/var/lib/gitlab/serialized/db2ares/en_us_repass";
		String pathMapPath  = "/Users/mac002/git/ages/ares/dcs/ages-alwb-library-gr-gr-cog";
		String reportPath = "/Users/mac002/canBeRemoved/reports";
		Compare c = new Compare(githubPath, gitlabPath, reportPath, pathMapPath);
	}
}
