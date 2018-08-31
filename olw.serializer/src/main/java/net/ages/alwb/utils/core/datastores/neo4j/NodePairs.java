package net.ages.alwb.utils.core.datastores.neo4j;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;

import org.ocmc.ioc.liturgical.schemas.models.supers.AbstractModel;

public class NodePairs extends AbstractModel {
	@Expose public List<NodePair> pairs = new ArrayList<NodePair>();

	public List<NodePair> getPairs() {
		return pairs;
	}

	public void setPairs(List<NodePair> pairs) {
		this.pairs = pairs;
	}
	
	public void add(String start, String end) {
		pairs.add(new NodePair(start, end));
	}
	public void add(NodePair pair) {
		pairs.add(pair);
	}
}
