package org.ocmc.olw.serializer.models;

import org.ocmc.ioc.liturgical.schemas.models.supers.AbstractModel;

import com.google.gson.annotations.Expose;

public class GitlabGroup extends AbstractModel {
   @Expose public int id = 0;
   @Expose public   String name = "";
   @Expose public    String path = "";
   @Expose public    String description = "";
   @Expose public    String visibility = "";
   @Expose public    boolean lfs_enabled = false;
   @Expose public    String avatar_url = "";
   @Expose public    String web_url = "";
   @Expose public    boolean request_access_enabled = false;
   @Expose public    String full_name = "";
   @Expose public    String full_path = "";
   @Expose public    Integer parent_id = null;
public int getId() {
	return id;
}
public void setId(int id) {
	this.id = id;
}
public String getName() {
	return name;
}
public void setName(String name) {
	this.name = name;
}
public String getPath() {
	return path;
}
public void setPath(String path) {
	this.path = path;
}
public String getDescription() {
	return description;
}
public void setDescription(String description) {
	this.description = description;
}
public String getVisibility() {
	return visibility;
}
public void setVisibility(String visibility) {
	this.visibility = visibility;
}
public boolean isLfs_enabled() {
	return lfs_enabled;
}
public void setLfs_enabled(boolean lfs_enabled) {
	this.lfs_enabled = lfs_enabled;
}
public String getAvatar_url() {
	return avatar_url;
}
public void setAvatar_url(String avatar_url) {
	this.avatar_url = avatar_url;
}
public String getWeb_url() {
	return web_url;
}
public void setWeb_url(String web_url) {
	this.web_url = web_url;
}
public boolean isRequest_access_enabled() {
	return request_access_enabled;
}
public void setRequest_access_enabled(boolean request_access_enabled) {
	this.request_access_enabled = request_access_enabled;
}
public String getFull_name() {
	return full_name;
}
public void setFull_name(String full_name) {
	this.full_name = full_name;
}
public String getFull_path() {
	return full_path;
}
public void setFull_path(String full_path) {
	this.full_path = full_path;
}
public Integer getParent_id() {
	return parent_id;
}
public void setParent_id(Integer parent_id) {
	this.parent_id = parent_id;
}
}
