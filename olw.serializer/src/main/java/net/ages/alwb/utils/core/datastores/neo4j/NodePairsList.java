package net.ages.alwb.utils.core.datastores.neo4j;

import java.util.ArrayList;
import java.util.List;

public class NodePairsList {
	private List<NodePairParameters> list = new ArrayList<NodePairParameters>();
	private int blockSize = 1000;
	
	public NodePairsList(NodePairs pairs, int blockSize) {
		NodePairs block = new NodePairs();
		this.blockSize = blockSize;
		int c = 0;
		for (NodePair pair : pairs.pairs) {
			block.add(pair);
			c++;
			if (c == blockSize) {
				list.add(new NodePairParameters(block));
				block = new NodePairs();
				c = 0;
			}
		}
	}

	public List<NodePairParameters> getList() {
		return list;
	}

	public void setList(List<NodePairParameters> list) {
		this.list = list;
	}

	
}
