package dsparq.query.analysis;

public class RelationPattern {
	private ConnectingRelation connectingRelation;
	private QueryPattern connectingPattern;
	private String connectingVariable;
	
	public RelationPattern(ConnectingRelation connectingRelation, 
			QueryPattern connectingPattern, String connectingVariable) {
		this.connectingRelation = connectingRelation;
		this.connectingPattern = connectingPattern;
		this.connectingVariable = connectingVariable;
	}

	public ConnectingRelation getConnectingRelation() {
		return connectingRelation;
	}

	public QueryPattern getConnectingPattern() {
		return connectingPattern;
	}

	public String getConnectingVariable() {
		return connectingVariable;
	}
}

