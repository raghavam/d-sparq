package dsparq.query.opt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import dsparq.misc.Constants;
import dsparq.query.QueryExecutorWithoutOp;
import dsparq.query.QueryVisitor;
import dsparq.query.TripleTable;
import dsparq.query.TriplesHolder;
import dsparq.util.Util;

/**
 * This class reorders the BGPs based on
 * selectivity estimates of each BGP
 * 
 * @author Raghava
 *
 */
public class SelectivityBasedOrdering extends QueryExecutorWithoutOp {
	
	public SelectivityBasedOrdering() {
		super();
	}
	
	public void processQuery(String query) throws Exception {
		long startTime = System.nanoTime();
		Query queryObj = QueryFactory.create(query);
		QueryVisitor queryVisitor = new QueryVisitor(queryObj);
		Op op = Algebra.compile(queryObj);
		op.visit(queryVisitor);
		List<Triple> basicGraphPatterns = queryVisitor.getBasicGraphPatterns();
		List<Triple> reOrderedBGPs = reOrderAndGetBGPs(basicGraphPatterns);
		List<TripleTable> resultTriplesLst = new ArrayList<TripleTable>(
												reOrderedBGPs.size());
		TripleTable resultTriples = fetchDataFromDB(reOrderedBGPs.get(0));
		if(resultTriples.getTripleRows().isEmpty()) {
			System.out.println("Empty result");
			System.out.println(Util.getElapsedTime(startTime));
			System.exit(0);
		}
		resultTriplesLst.add(resultTriples);
		for(int i=1; i<reOrderedBGPs.size(); i++) {
			JoinVarPos varPos = checkJoinVariable(reOrderedBGPs, i); 
			// perform the join based on this position
			if(varPos.getJoinVariable() == JoinVariable.NONE) {
				TripleTable tripleTable = fetchDataFromDB(reOrderedBGPs.get(i));
				if(tripleTable.getTripleRows().isEmpty()) {
					System.out.println("Empty result");
					Util.getElapsedTime(startTime);
					System.exit(0);
				}
				resultTriplesLst.add(tripleTable);
			}
			else if(varPos.getJoinVariable() == JoinVariable.SUBJECT) {
				resultTriplesLst.add(i, calculateJoin(
						reOrderedBGPs.get(i),
						resultTriplesLst.get(varPos.getSubPos()),
						varPos.getJoinVariable()));
				// now empty the prev BGP join, the latest one is all
				// that is needed
				resultTriplesLst.get(varPos.getSubPos()).clear();				
			}
			else if(varPos.getJoinVariable() == JoinVariable.OBJECT) {
				resultTriplesLst.add(i, calculateJoin(
						reOrderedBGPs.get(i),
						resultTriplesLst.get(varPos.getObjPos()),
						varPos.getJoinVariable()));
				resultTriplesLst.get(varPos.getObjPos()).clear();	
			}
			else if(varPos.getJoinVariable() == JoinVariable.BOTH) {
				// join with subject first and then with object
				TripleTable tempTable = calculateJoin(
						reOrderedBGPs.get(i),
						resultTriplesLst.get(varPos.getSubPos()),
						JoinVariable.SUBJECT);
				System.out.println("TempTable1 size: " + tempTable.getTripleRows().size());
/*				
				TripleTreeTable tempTreeTable = new TripleTreeTable(
						reOrderedBGPs.get(i).getObject().toString(), tempTable);
				tempTable.clear();
				TripleTreeTable prevTreeTable = new TripleTreeTable(
						reOrderedBGPs.get(i).getObject().toString(),
						resultTriplesLst.get(varPos.getObjPos()));
*/				
				System.out.println("TempTable size: " + 
						tempTable.getTripleRows().size());
				System.out.println("Prev result size: " + 
						resultTriplesLst.get(varPos.getObjPos()).getTripleRows().size());
				resultTriplesLst.add(i, calculateJoin(
						tempTable,
						resultTriplesLst.get(varPos.getObjPos()),
						reOrderedBGPs.get(i).getObject().toString()));
				
				tempTable.clear();
				resultTriplesLst.get(varPos.getSubPos()).clear();
				resultTriplesLst.get(varPos.getObjPos()).clear();
//				prevTreeTable.clear();
			}
		}
		// print the last of the result triples
		TriplesHolder finalResult = resultTriplesLst.get(resultTriplesLst.size()-1);
		System.out.println("No of results: " + finalResult.getTripleRows().size());
		
		for(Map<String, Long> resultIDMap : finalResult.getTripleRows()) {
			for(String var : finalResult.getBgpVars())
				System.out.print(var + " : " + resultIDMap.get(var) + "  ");
			System.out.println();
		}
		
		Util.getElapsedTime(startTime);
	}
	
	private List<Triple> reOrderAndGetBGPs(List<Triple> bgps) {
		// TODO: for now, returning the same BGPs in the same order
		// Later on, get the statistics for each BGP, sort them and return
		// them in the ascending order of selectivities.
		return bgps;
	}
	
	private JoinVarPos checkJoinVariable(List<Triple> bgps, int pos) {		
		Triple currBGP = bgps.get(pos);
		Node currBGPSubject = currBGP.getSubject();
		Node currBGPObject = currBGP.getObject();
		JoinVarPos joinVarPos = new JoinVarPos();
		boolean isJoinVarSub = false;
		if(currBGPSubject.isVariable()) {
			int subPos = compareJoinVars(bgps, pos, currBGPSubject.toString());
			if(subPos >= 0) {
				isJoinVarSub = true;
				joinVarPos.setSubPos(subPos);
			}
			else
				joinVarPos.setSubPos(subPos);
		}
		else
			joinVarPos.setSubPos(-1);
		boolean isJoinVarObj = false;
		if(currBGPObject.isVariable()) {
			int objPos = compareJoinVars(bgps, pos, currBGPObject.toString());
			if(objPos >= 0) {
				isJoinVarObj = true;
				joinVarPos.setObjPos(objPos);
			}
			else
				joinVarPos.setObjPos(objPos);
		}
		else
			joinVarPos.setObjPos(-1);
		
		if(isJoinVarSub && isJoinVarObj) {
			joinVarPos.setJoinVariable(JoinVariable.BOTH);
			return joinVarPos;
		}
		else {
			if(isJoinVarSub) 
				joinVarPos.setJoinVariable(JoinVariable.SUBJECT);
			else if(isJoinVarObj)
				joinVarPos.setJoinVariable(JoinVariable.OBJECT);
			else {
				joinVarPos.setJoinVariable(JoinVariable.NONE);
				joinVarPos.setSubPos(-1);
				joinVarPos.setObjPos(-1);
			}
			return joinVarPos;
		}
	}
	
	private int compareJoinVars(List<Triple> bgps, int pos, String varName) {
		for(int i=pos-1; i >= 0; i--) {
			Triple prevBGP = bgps.get(i);
			Node prevBGPSubject = prevBGP.getSubject();
			Node prevBGPObject = prevBGP.getObject();
			if(prevBGPSubject.isVariable()) {
				if(prevBGPSubject.toString().equals(varName)) 
					return i;				
			}
			if(prevBGPObject.isVariable()) {
				if(prevBGPObject.toString().equals(varName))
					return i;				
			}
		}
		return -1;
	}
	
	private TripleTable calculateJoin(Triple currBGP, 
			TriplesHolder prevBGPResult, JoinVariable joinVar) throws Exception {
		String var = null;
		BasicDBObject queryDoc = new BasicDBObject();
		Node currBGPSubject = currBGP.getSubject();
		Node currBGPPredicate = currBGP.getPredicate();
		Node currBGPObject = currBGP.getObject();
		TripleTable resultTable = new TripleTable();
		
		if(!currBGPPredicate.isVariable())
			queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, Util.getIDFromStrVal(
					idValCollection, eidValCollection, 
					currBGPPredicate.toString(), true));
		if(joinVar == JoinVariable.SUBJECT) {
			var = currBGPSubject.toString();
			if(!currBGPObject.isVariable())
				queryDoc.put(Constants.FIELD_TRIPLE_OBJECT, 
						Util.getIDFromStrVal(idValCollection, eidValCollection, 
								currBGPObject.toString(), false));
			else
				resultTable.addBGPVar(currBGPObject.toString());
		}
		else if(joinVar == JoinVariable.OBJECT) {
			var = currBGPObject.toString();
			if(!currBGPSubject.isVariable())
				queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, 
						Util.getIDFromStrVal(idValCollection, eidValCollection, 
								currBGPSubject.toString(), false));
			else
				resultTable.addBGPVar(currBGPSubject.toString());
			
		}
		resultTable.addBGPVar(var);
		Set<String> remVars = new HashSet<String>(prevBGPResult.getBgpVars());
		remVars.remove(var);
		resultTable.addAllBGPVars(remVars);
		for(Map<String, Long> varIDMap : prevBGPResult.getTripleRows()) {
			Long varID = varIDMap.get(var);
			if(joinVar == JoinVariable.SUBJECT)
				queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, varID);
			else
				queryDoc.put(Constants.FIELD_TRIPLE_OBJECT, varID);
			DBCursor cursor = tripleCollection.find(queryDoc);
			while(cursor.hasNext()) {
				DBObject resultDoc = cursor.next();
				Map<String, Long> resultMap = new HashMap<String, Long>();
				if(currBGPSubject.isVariable())
					resultMap.put(currBGPSubject.toString(), 
							(Long)resultDoc.get(Constants.FIELD_TRIPLE_SUBJECT));
				if(currBGPObject.isVariable())	
					resultMap.put(currBGPObject.toString(), 
							(Long)resultDoc.get(Constants.FIELD_TRIPLE_OBJECT));
				// add the rest of vars in prevBGPResult to this map
				for(String remVar : remVars)
					resultMap.put(remVar, varIDMap.get(remVar));
				resultTable.addTriple(resultMap);
			}
			if(joinVar == JoinVariable.SUBJECT)
				queryDoc.remove(Constants.FIELD_TRIPLE_SUBJECT);
			else
				queryDoc.remove(Constants.FIELD_TRIPLE_OBJECT);
		}
		return resultTable;
	}
	
	private TripleTable calculateJoin(TripleTable currTable, 
			TripleTable prevTable, String key) {
		TripleTable resultTable = new TripleTable();
		resultTable.addAllBGPVars(currTable.getBgpVars());
//		currTable.getTripleTreeRows().retainAll(prevTable.getTripleTreeRows());
		
		int count = 0;
		Iterator<Map<String, Long>> currIt = currTable.getTripleRows().iterator();
		while(currIt.hasNext()) {
			Long currID = currIt.next().get(key);
			int i = 0;
			for(; i<prevTable.getTripleRows().size(); i++) {
				Map<String, Long> prevIDMap = prevTable.getTripleRows().get(i);
				if(currID.longValue() == prevIDMap.get(key).longValue()) 
					break;
			}
			if(i == prevTable.getTripleRows().size())
				currIt.remove();
			count++;
			if(count%10000 == 0)
				System.out.println("Reached " + count);
		}
		return currTable;
/*		
		for(Map<String, Long> currIDMap : currTable.getTripleRows()) {
			Long currID = currIDMap.get(key);
			for(Map<String, Long> prevIDMap : prevTable.getTripleRows()) {
				if(currID.longValue() == prevIDMap.get(key).longValue()) 
					resultTable.addTriple(currIDMap);
			}
			count++;
			if(count >= (currTable.getTripleRows().size()%0.1))
				System.out.println("Reached " + count);
		}
		
		for(Map<String, Long> prevIDMap : prevTable.getTripleRows()) {
			Long prevID = prevIDMap.get(key);
			for(Map<String, Long> currIDMap : currTable.getTripleRows()) {
				if(prevID.longValue() == currIDMap.get(key).longValue()) 
					resultTable.addTriple(currIDMap);
			}
		}
*/		
//		return resultTable;
	}
 	
	public static void main(String[] args) throws Exception {
		
		String testQuery1 = "SELECT ?x WHERE { " +
		"?x <http://lubm.example.org#takesCourse> " +
		"<http://www.Department0.University0.edu/GraduateCourse0> . " + // 2
		"?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
			"<http://lubm.example.org#GraduateStudent> . " + 			// 208669
		"} ";
		
		// query-2 in LUBM
		String testQuery2 = "SELECT ?x ?y ?z WHERE { " +
		"?z <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
		"<http://lubm.example.org#Department> . " + 						// 2074	
		"?z <http://lubm.example.org#subOrganizationOf> ?y . " + 			// 31971
		"?y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
		"<http://lubm.example.org#University> . " +						// 32939
	"?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
	"<http://lubm.example.org#GraduateStudent> . " + 				// 208669
	"?x <http://lubm.example.org#undergraduateDegreeFrom> ?y . " +		// 253463
	"?x <http://lubm.example.org#memberOf> ?z . " + 					// 1030499
	"} ";
		
		// changed the order of BGPs based on selectivity	
		String testQuery3_1 = "select ?x ?y ?c where { " + 
		"?x <http://lubm.example.org#teachingAssistantOf> " +
		"<http://www.Department0.University0.edu/Course30> . " +
		"?x <http://lubm.example.org#advisor> " +
		"<http://www.Department0.University0.edu/AssistantProfessor1> . " +
		"?x <http://lubm.example.org#memberOf> <http://www.Department0.University0.edu> . " +
		"?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
			"<http://lubm.example.org#GraduateStudent> . " +
		"?x <http://lubm.example.org#undergraduateDegreeFrom> ?y . " +
		"?x <http://lubm.example.org#takesCourse> ?c . " +
		"} ";
		
		String largeResultQuery = "select ?x ?y ?c where { " + 
		"?x <http://lubm.example.org#undergraduateDegreeFrom> ?y . " +
		"?x <http://lubm.example.org#takesCourse> ?c . " +
		"} ";
		
		String query2 = "select ?x ?y ?c where { " + 
		"?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
		"<http://lubm.example.org#GraduateStudent> . " +
		"?x <http://lubm.example.org#undergraduateDegreeFrom> ?y . " +
		"?x <http://lubm.example.org#takesCourse> ?c . " +
		"} ";
		
//		"?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
//		"<http://lubm.example.org#GraduateStudent> . " +
		
		// testQuery3_1 runs in 0.598 secs on 0 result nodes	
		// testQuery3_1 runs in 0.623 secs on 2 result node (nimbus4) -- this matches the worst time
		// of RDF-3X. When it warms up, after 2-3 runs, it comes down to 0.025 secs
		
		new SelectivityBasedOrdering().processQuery(query2);
	}
}

class JoinVarPos {
	private JoinVariable var;
	private int subPos;
	private int objPos;
	
	JoinVarPos() {
		
	}
	
	JoinVarPos(JoinVariable var, int subPos, int objPos) {
		this.var = var;
		this.subPos = subPos;
		this.objPos = objPos;
	}
	
	void setJoinVariable(JoinVariable var) {
		this.var = var;
	}
	
	JoinVariable getJoinVariable() {
		return var;
	}
	
	void setSubPos(int subPos) {
		this.subPos = subPos;
	}
	
	int getSubPos() {
		return subPos;
	}
	
	void setObjPos(int objPos) {
		this.objPos = objPos;
	}
	
	int getObjPos() {
		return objPos;
	}
}

enum JoinVariable {
	SUBJECT,
	OBJECT,
	BOTH,
	NONE
}
