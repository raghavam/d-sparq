package dsparq.query;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class TripleTreeTable implements TriplesHolder {

	private Set<String> bgpVars;
	private Set<Map<String, Long>> tripleRows;
	private String compareVar;
	
	public TripleTreeTable(String compareVar) {
		bgpVars = new HashSet<String>();
		this.compareVar = compareVar;
		TripleComparator comparator = new TripleComparator(compareVar);
		tripleRows = new TreeSet<Map<String, Long>>(comparator);
	}
	
	public TripleTreeTable(String compareVar, TriplesHolder tripleTable) {
		bgpVars = new HashSet<String>();
		this.compareVar = compareVar;
		TripleComparator comparator = new TripleComparator(compareVar);
		tripleRows = new TreeSet<Map<String, Long>>(comparator);
		tripleRows.addAll(tripleTable.getTripleRows());
		bgpVars.addAll(tripleTable.getBgpVars());
	}
	
	@Override
	public boolean addAllBGPVars(Set<String> vars) {
		return bgpVars.addAll(vars);
	}

	@Override
	public boolean addBGPVar(String var) {
		return bgpVars.add(var);
	}

	@Override
	public boolean addTriple(Map<String, Long> varIDMap) {
		return tripleRows.add(varIDMap);
	}

	@Override
	public void clear() {
		tripleRows.clear();
		bgpVars.clear();
	}

	@Override
	public Set<String> getBgpVars() {
		return bgpVars;
	}

	@Override
	public List<Map<String, Long>> getTripleRows() {
		throw new UnsupportedOperationException(
				"Use getTripleTreeRows() instead");
	}
	
	public Set<Map<String, Long>> getTripleTreeRows() {
		return tripleRows;
	}

	@Override
	public void setBgpVars(Set<String> bgpVars) {
		this.bgpVars = bgpVars;
	}
}

