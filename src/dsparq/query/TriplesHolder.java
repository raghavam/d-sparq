package dsparq.query;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface TriplesHolder {

	public abstract void setBgpVars(Set<String> bgpVars);

	public abstract Set<String> getBgpVars();

	public abstract List<Map<String, Long>> getTripleRows();

	public abstract boolean addTriple(Map<String, Long> varIDMap);

	public abstract boolean addBGPVar(String var);

	public abstract boolean addAllBGPVars(Set<String> vars);

	public abstract void clear();

}