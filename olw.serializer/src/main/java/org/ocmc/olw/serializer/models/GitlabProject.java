package org.ocmc.olw.serializer.models;

import java.util.ArrayList;
import java.util.List;

import org.ocmc.ioc.liturgical.schemas.models.supers.AbstractModel;

import com.google.gson.annotations.Expose;

public class GitlabProject extends AbstractModel {
	@Expose public String ssh_url_to_repo = ""; // "git@gitlab.liml.org:olwsys/db2json.git"
	@Expose public String path_with_namespace = ""; // "olwsys/db2json"
	@Expose public String description = ""; // "Dump of Neo4j nodes ....
	@Expose public String created_at = ""; // "2018-08-29T16:21:13.076Z"
	@Expose public String http_url_to_repo = ""; // "https://gitlab.liml.org/olwsys/db2json.git"
	@Expose public String readme_url = ""; //
	@Expose public String path = ""; // "db2json"
	@Expose public String web_url = ""; //  "https://gitlab.liml.org/olwsys/db2json"
	@Expose public String avatar_url = ""; // 
	@Expose public List<String> tag_list = new ArrayList<String>(); 
	@Expose public String last_activity_at = ""; // "2018-08-30T18:07:31.174Z"
	@Expose public String name = ""; // "db2json"
	@Expose public String default_branch = ""; // "master"
	@Expose public String id = ""; // "2"
	@Expose public String name_with_namespace = ""; // "olwsys / db2json"
	@Expose public int star_count = 0; //
	@Expose public int forks_count = 0;
	
	public String getSsh_url_to_repo() {
		return ssh_url_to_repo;
	}
	public void setSsh_url_to_repo(String ssh_url_to_repo) {
		this.ssh_url_to_repo = ssh_url_to_repo;
	}
	public String getPath_with_namespace() {
		return path_with_namespace;
	}
	public void setPath_with_namespace(String path_with_namespace) {
		this.path_with_namespace = path_with_namespace;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getCreated_at() {
		return created_at;
	}
	public void setCreated_at(String created_at) {
		this.created_at = created_at;
	}
	public String getHttp_url_to_repo() {
		return http_url_to_repo;
	}
	public void setHttp_url_to_repo(String http_url_to_repo) {
		this.http_url_to_repo = http_url_to_repo;
	}
	public String getReadme_url() {
		return readme_url;
	}
	public void setReadme_url(String readme_url) {
		this.readme_url = readme_url;
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public String getWeb_url() {
		return web_url;
	}
	public void setWeb_url(String web_url) {
		this.web_url = web_url;
	}
	public String getAvatar_url() {
		return avatar_url;
	}
	public void setAvatar_url(String avatar_url) {
		this.avatar_url = avatar_url;
	}
	public String getLast_activity_at() {
		return last_activity_at;
	}
	public void setLast_activity_at(String last_activity_at) {
		this.last_activity_at = last_activity_at;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDefault_branch() {
		return default_branch;
	}
	public void setDefault_branch(String default_branch) {
		this.default_branch = default_branch;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName_with_namespace() {
		return name_with_namespace;
	}
	public void setName_with_namespace(String name_with_namespace) {
		this.name_with_namespace = name_with_namespace;
	}
	public int getStar_count() {
		return star_count;
	}
	public void setStar_count(int star_count) {
		this.star_count = star_count;
	}
	public int getForks_count() {
		return forks_count;
	}
	public void setForks_count(int forks_count) {
		this.forks_count = forks_count;
	}
	public List<String> getTag_list() {
		return tag_list;
	}
	public void setTag_list(List<String> tag_list) {
		this.tag_list = tag_list;
	}
}
