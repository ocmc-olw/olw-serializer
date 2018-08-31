package net.ages.alwb.utils.core.datastores.neo4j;

import com.google.gson.annotations.Expose;

import org.ocmc.ioc.liturgical.schemas.models.supers.AbstractModel;

public class NodePair extends AbstractModel {
	@Expose public String start = ""; // starting node for a relationship
	@Expose public String end = ""; // ending node for a relationship
	
	public NodePair() {
		super();
	}
	
	public NodePair(String start, String end) {
		super();
		this.start = start;
		this.end = end;
	}

	public String getStart() {
		return start;
	}
	public void setStart(String start) {
		this.start = start;
	}
	public String getEnd() {
		return end;
	}
	public void setEnd(String end) {
		this.end = end;
	}
}
