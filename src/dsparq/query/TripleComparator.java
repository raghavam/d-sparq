package dsparq.query;

import java.util.Comparator;
import java.util.Map;

public class TripleComparator implements Comparator<Map<String, Long>> {

	private String compareVar;
	
	public TripleComparator(String var) {
		compareVar = var;
	}
	
	@Override
	public int compare(Map<String, Long> arg0, Map<String, Long> arg1) {
		return (int) (arg0.get(compareVar) - arg1.get(compareVar));
	}
	
}
