package dsparq.query.analysis;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a star query pattern i.e., queries
 * of the form, s1 p1 o1; s1 p2 o2; s1 p3 o3.
 * 
 * @author Raghava
 *
 */
public class StarPattern extends QueryPattern {
	//each element can be a NumericalTriplePattern or a PipelinePattern
	private List<QueryPattern> queryPatterns;
	
	public StarPattern() {
		queryPatterns = new ArrayList<QueryPattern>();
	}
	
	public void addQueryPattern(QueryPattern pattern) {
		queryPatterns.add(pattern);
	}
	
	public List<QueryPattern> getQueryPatterns() {
		return queryPatterns;
	}
	
	/**
	 * Searches for the given vertex in object position among all the patterns
	 * held by this StarPattern and adds the given pattern if the given vertex
	 * is found.
	 * @param vertex vertex to search in object position
	 * @param pattern pattern to insert if the given vertex is found
	 */
	public void searchAndAdd(String vertex, ConnectingRelation relation, 
			QueryPattern connectingPattern) {
		boolean replacePattern = false;
		int replacementIndex = -1;
		PipelinePattern replacementPattern = null;
		for(int i=0; i<queryPatterns.size(); i++) {
			QueryPattern queryPattern = queryPatterns.get(i);
			if(queryPattern instanceof PipelinePattern) {
				PipelinePattern ppattern = (PipelinePattern)queryPattern;
				if(ppattern.getTriple().getObject().equals(vertex.toString())) {
					ppattern.addRelationTriple(relation, connectingPattern);
				}
			}
			else if(queryPattern instanceof NumericalTriplePattern) {
				NumericalTriplePattern ntpattern = 
						(NumericalTriplePattern)queryPattern;
				if(ntpattern.getObject().equals(vertex.toString())) {
					PipelinePattern ppattern = new PipelinePattern();
					ppattern.setTriple(ntpattern);
					ppattern.addRelationTriple(relation, connectingPattern);
					replacePattern = true;
					replacementIndex = i;
					replacementPattern = ppattern;
				}
			}
		}
		if(replacePattern) 
			queryPatterns.set(replacementIndex, replacementPattern);
	}
	
	@Override
	public String toString() {
		StringBuilder s = new StringBuilder(" Star -- ");
		for(QueryPattern pattern : queryPatterns)
			s.append(pattern.toString()).append("\n\t");
		return s.toString();
	}
}
