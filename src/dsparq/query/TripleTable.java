package dsparq.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class represents a set of triples along with the 
 * labels of subject, predicate and object.
 * 
 * @author Raghava
 *
 */
public class TripleTable implements TriplesHolder {

	private Set<String> bgpVars;
	private List<Map<String, Long>> tripleRows;
	
	public TripleTable() {
		bgpVars = new HashSet<String>();
		tripleRows = new ArrayList<Map<String, Long>>();
	}
	
	public TripleTable(TripleTreeTable treeTable) {
		bgpVars = new HashSet<String>(treeTable.getBgpVars());
		tripleRows = new ArrayList<Map<String, Long>>(treeTable.getTripleTreeRows());
	}

	/* (non-Javadoc)
	 * @see dsparq.query.TriplesHolder#setBgpVars(java.util.Set)
	 */
	public void setBgpVars(Set<String> bgpVars) {
		this.bgpVars = bgpVars;
	}

	/* (non-Javadoc)
	 * @see dsparq.query.TriplesHolder#getBgpVars()
	 */
	public Set<String> getBgpVars() {
		return bgpVars;
	}
	
	/* (non-Javadoc)
	 * @see dsparq.query.TriplesHolder#getTripleRows()
	 */
	public List<Map<String, Long>> getTripleRows() {
		return tripleRows;
	}

	/* (non-Javadoc)
	 * @see dsparq.query.TriplesHolder#addTriple(java.util.Map)
	 */
	public boolean addTriple(Map<String, Long> varIDMap) {
		return tripleRows.add(varIDMap);
	}		
	
	/* (non-Javadoc)
	 * @see dsparq.query.TriplesHolder#addBGPVar(java.lang.String)
	 */
	public boolean addBGPVar(String var) {
		return bgpVars.add(var);
	}
	
	/* (non-Javadoc)
	 * @see dsparq.query.TriplesHolder#addAllBGPVars(java.util.Set)
	 */
	public boolean addAllBGPVars(Set<String> vars) {
		return bgpVars.addAll(vars);
	}
	
	/* (non-Javadoc)
	 * @see dsparq.query.TriplesHolder#clear()
	 */
	public void clear() {
		tripleRows.clear();
		bgpVars.clear();
	}
}

