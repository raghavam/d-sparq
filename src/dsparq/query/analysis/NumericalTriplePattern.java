package dsparq.query.analysis;

public class NumericalTriplePattern extends QueryPattern {
	private RDFVertex subject;
	private RelationshipEdge predicate;
	private RDFVertex object;
	
	public NumericalTriplePattern(RDFVertex subject, RelationshipEdge predicate, 
			RDFVertex object) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}
	
	public String getSubject() {
		return subject.toString();
	}
	
	public RelationshipEdge getPredicate() {
		return predicate;
	}
	
	public String getObject() {
		return object.toString();
	}
	
	@Override
	public String toString() {
		StringBuilder triple = new StringBuilder(subject.toString());
		triple.append(" ").append(predicate.toString()).
			append(" ").append(object.toString());
		return triple.toString();
	}
}
