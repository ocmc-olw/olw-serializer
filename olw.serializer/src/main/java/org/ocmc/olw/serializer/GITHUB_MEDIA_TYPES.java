package org.ocmc.olw.serializer;

public enum GITHUB_MEDIA_TYPES {
	DIFF("application/vnd.github.v3+diff")
	, HTML("application/vnd.github.v3+html")
	, JSON("application/vnd.github.v3+json")
	, PATCH("application/vnd.github.v3+patch")
	, RAW("application/vnd.github.v3+raw")
	;
	
	public String name;
	
	private GITHUB_MEDIA_TYPES(String name) {
		this.name = name;
	}
	
}
