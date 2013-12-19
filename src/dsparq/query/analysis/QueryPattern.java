package dsparq.query.analysis;

/**
 * Represents an empty query pattern. Other query
 * patterns like the Star, Pipeline extend this class.
 * Used to preserve the generality.
 * 
 * @author Raghava
 *
 */
public class QueryPattern {

/*	
	public void checkTypeAndAddPattern(QueryPattern pattern) throws Exception {
		if(this instanceof NumericalTriplePattern) {
			throw new Exception("Add a triple directly. " +
					"Don't invoke this method");
		}
		else if(this instanceof PipelinePattern) {
			((PipelinePattern)this).setConnectedTriple(pattern);
		}
		else if(this instanceof StarPattern) {
			((StarPattern)this).addQueryPattern(pattern);
		}
		else 
			throw new Exception("Unknown type: " + 
					this.getClass().getCanonicalName());
	}
*/	
	@Override
	public String toString() {
		if(this instanceof NumericalTriplePattern)
			return ((NumericalTriplePattern)this).toString();
		else if(this instanceof PipelinePattern)
			return ((PipelinePattern)this).toString();
		else
			return ((StarPattern)this).toString();
	}
}
