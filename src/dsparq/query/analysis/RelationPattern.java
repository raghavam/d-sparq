package dsparq.query.analysis;

public class RelationPattern {
	private ConnectingRelation connectingRelation;
	private QueryPattern connectingPattern;
	
	public RelationPattern(ConnectingRelation connectingRelation, 
			QueryPattern connectingPattern) {
		this.setConnectingRelation(connectingRelation);
		this.setConnectingPattern(connectingPattern);
	}

	public ConnectingRelation getConnectingRelation() {
		return connectingRelation;
	}

	public void setConnectingRelation(ConnectingRelation connectingRelation) {
		this.connectingRelation = connectingRelation;
	}

	public QueryPattern getConnectingPattern() {
		return connectingPattern;
	}

	public void setConnectingPattern(QueryPattern connectingPattern) {
		this.connectingPattern = connectingPattern;
	}
}

