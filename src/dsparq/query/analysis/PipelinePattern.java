package dsparq.query.analysis;

import java.util.ArrayList;
import java.util.List;


/**
 * Represents the pipeline pattern, such as
 * s1 p1 o1 joined with o1 p2 s2. The second 
 * connecting pattern can be either a triple or another
 * pipeline or a star pattern.
 *  
 * @author Raghava
 *
 */
public class PipelinePattern extends QueryPattern {
	private NumericalTriplePattern triple;
	//can be either a triple/star/pipeline
	private List<RelationPattern> relationPatterns;
	
	public PipelinePattern() {
		relationPatterns = new ArrayList<RelationPattern>();
	}
	
	public NumericalTriplePattern getTriple() {
		return triple;
	}
	
	public void setTriple(NumericalTriplePattern triple) {
		this.triple = triple;
	}
	
	public void addRelationTriple(ConnectingRelation connectingRelation, 
			QueryPattern connectingPattern, String connectingVariable) {
		RelationPattern relationPattern = new RelationPattern(
				connectingRelation, connectingPattern, connectingVariable);
		relationPatterns.add(relationPattern);
	}
	
	public List<RelationPattern> getRelationPatterns() {
		return relationPatterns;
	}
	
	@Override
	public String toString() {
		StringBuilder msg = new StringBuilder(" Pipeline -- ");
		msg.append(triple.toString());
		msg.append("\n\t");
		for(RelationPattern rpattern : relationPatterns)
			msg.append(rpattern.getConnectingRelation().toString()).append("  ").
			append(rpattern.getConnectingPattern().toString()).append("\n\t");
		return msg.toString();
	}
}

