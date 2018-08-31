package net.ages.alwb.utils.core.datastores.neo4j;

import com.google.gson.annotations.Expose;

import org.ocmc.ioc.liturgical.schemas.models.supers.AbstractModel;

public class NodePairParameters extends AbstractModel {
	@Expose NodePairs parameters = new NodePairs();
	
	public NodePairParameters (NodePairs pairs) {
		super();
		this.parameters = pairs;
	}

	public NodePairs getParameters() {
		return parameters;
	}

	public void setParameters(NodePairs parameters) {
		this.parameters = parameters;
	}
}
