package dsparq.query.opt;

import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;

import dsparq.query.QueryVisitor;
import dsparq.query.analysis.NumericalTriplePattern;
import dsparq.query.analysis.PipelinePattern;
import dsparq.query.analysis.QueryGraphAnalyzer;
import dsparq.query.analysis.QueryPattern;
import dsparq.query.analysis.StarPattern;

/**
 * Receives the query patterns from QueryGraphAnalyzer and
 * then processes the query according to the patterns.
 * 
 * @author Raghava
 *
 */
public class QueryProcessor {

	public QueryProcessor() {
		
	}
	
	public void processQuery(String query) throws Exception {
		Query queryObj = QueryFactory.create(query);
		QueryVisitor queryVisitor = new QueryVisitor(queryObj);
		Op op = Algebra.compile(queryObj);
		op.visit(queryVisitor);
		List<Triple> basicGraphPatterns = 
				queryVisitor.getBasicGraphPatterns();
		QueryGraphAnalyzer queryGraphAnalyzer = new QueryGraphAnalyzer();
		List<QueryPattern> queryPatterns = 
				queryGraphAnalyzer.getAnalyzedPatterns(basicGraphPatterns);
		//assign each queryPattern to one thread
		
		int queueID = 0;
		QueryPattern queryPattern = queryPatterns.get(0);
		if(queryPattern instanceof NumericalTriplePattern) {
			
		}
		else if(queryPattern instanceof PipelinePattern) {
/*			PipelinePattern p = (PipelinePattern) queryPattern;
			NumericalTriplePattern triple = p.getTriple();
			System.out.println("Queue ID: " + queueID);
			queueID++;
			QueryPattern patternToCheck = p.getConnectedTriple();
			boolean isTriple = false;
			while(!isTriple) {
				if(patternToCheck instanceof NumericalTriplePattern) {
					System.out.println("Queue ID: " + queueID);
					queueID++;
					isTriple = true;
				}
				else if(patternToCheck instanceof PipelinePattern) {
					System.out.println("Queue ID: " + queueID);
					queueID++;
					patternToCheck = 
							((PipelinePattern) patternToCheck).
								getConnectedTriple();
				}
				else if(patternToCheck instanceof StarPattern) {
					System.out.println("Queue ID: " + queueID);
					queueID++;
					List<QueryPattern> patterns = 
							((StarPattern) patternToCheck).getQueryPatterns();
					//if the entire list is NumericalTriplePattern then done
					
				}
			}
*/			
		}
		else if(queryPattern instanceof StarPattern) {
			
		}
		else
			throw new Exception("Type of pattern unknown \n" + 
					queryPattern.toString());
	}
	
	public static void main(String[] args) {

	}

}
