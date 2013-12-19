package dsparq.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class represents a basic graph pattern along with
 * its data.
 * 
 * @author Raghava
 *
 */
public class TriplePatternTable {

	private String subjectLabel;
	private String predicateLabel;
	private String objectLabel;
	private List<Long> subject;
	private List<Long> predicate;
	private List<Long> object;
	
	public void setSubjectLabel(String label) {
		subjectLabel = label;
	}
	
	public void setPredicateLabel(String label) {
		predicateLabel = label;
	}
	
	public void setObjectLabel(String label) {
		objectLabel = label;
	}
	
	public void subIsVar(boolean isVar, Long id) {
		if(isVar)
			subject = new ArrayList<Long>();	//ignore id
		else
			subject = Collections.singletonList(id);
	}
	
	public void predIsVar(boolean isVar, Long id) {
		// should there be only 1 method with sub,pred, obj as params
		if(isVar)
			predicate = new ArrayList<Long>();	//ignore id
		else
			predicate = Collections.singletonList(id);
	}
	
	public void objIsVar(boolean isVar, Long id) {
		if(isVar)
			object = new ArrayList<Long>();	//ignore id
		else
			object = Collections.singletonList(id);
	}
	
	public void addSubject(Long subID) {
		subject.add(subID);
	}
	
	public void addPredicate(Long predID) {
		predicate.add(predID);
	}
	
	public void addObject(Long objID) {
		object.add(objID);
	}
}
