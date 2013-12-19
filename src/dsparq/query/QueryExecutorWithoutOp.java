package dsparq.query;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpAsQuery;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.util.FmtUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import dsparq.misc.Constants;
import dsparq.misc.HostInfo;
import dsparq.misc.PropertyFileHandler;
import dsparq.util.Util;

/**
 * This class executes the given SPARQL query without any
 * optimizations. The BGPs are taken one by one, in the order
 * given in the query and executed against the underlying MongoDB.
 * 
 * @author Raghava
 *
 */
public class QueryExecutorWithoutOp {
	
	private Mongo mongoS;
	private Mongo localMongo;
	protected DBCollection tripleCollection; 
	protected DBCollection idValCollection;
	protected DBCollection eidValCollection;
	
	public QueryExecutorWithoutOp() {
		try {
			mongoS = new Mongo("nimbus2", 27017);
			localMongo = new Mongo(Constants.MONGO_LOCAL_HOST, 10000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		DB localDB = localMongo.getDB(Constants.MONGO_RDF_DB);
		DB sdb = mongoS.getDB(Constants.MONGO_RDF_DB);
		tripleCollection = localDB.getCollection(Constants.MONGO_PARTITIONED_VERTEX_COLLECTION);
		idValCollection = sdb.getCollection(Constants.MONGO_IDVAL_COLLECTION);
		eidValCollection = sdb.getCollection(Constants.MONGO_EIDVAL_COLLECTION);
	}

	public void processQuery(String query) throws Exception {
		Query queryObj = QueryFactory.create(query);
		QueryVisitor queryVisitor = new QueryVisitor(queryObj);
		Op op = Algebra.compile(queryObj);
		op.visit(queryVisitor);
		List<Triple> basicGraphPatterns = queryVisitor.getBasicGraphPatterns();
		List<String> queryVars = queryVisitor.getQueryVariables();
		
		TriplesHolder answerTable;
		
		// processing the BGPs backwards, starting from the last - it is easy
		// to get the joins going this way.
		
		// start with the last 2 BGPs and then continue.
		if(basicGraphPatterns.size() >= 2) {
			TriplesHolder lastBGPData = fetchDataFromDB(
					basicGraphPatterns.get(basicGraphPatterns.size()-1));
//			System.out.println("BGP-" + (basicGraphPatterns.size()-1) + ": " + 
//					lastBGPData.getTripleRows().size());
			
			TriplesHolder lastButOneBGPData = fetchDataFromDB(
					basicGraphPatterns.get(basicGraphPatterns.size()-2));
//			System.out.println("BGP-" + (basicGraphPatterns.size()-2) + ": " + 
//					lastButOneBGPData.getTripleRows().size());
			
			answerTable = checkAndPerformJoins(
					lastBGPData, lastButOneBGPData, queryVars);
			
			for(int i=basicGraphPatterns.size()-3; i>=0; i--) {
				TriplesHolder currResultBGP = fetchDataFromDB(
						basicGraphPatterns.get(i));
//				System.out.println("BGP-" + i + ": " + 
//						currResultBGP.getTripleRows().size());
				answerTable = checkAndPerformJoins(
						answerTable, currResultBGP, queryVars);
			}
		}
		else {
			// there is only 1 BGP
			answerTable = fetchDataFromDB(basicGraphPatterns.get(0));
		}
		// print answer
		
		// convert the IDs to Strings
		System.out.println("Query answer\n");
		for(String var : answerTable.getBgpVars())
			System.out.print(var + "  ");
		System.out.println("\n");
		// TODO: setting isPredicate to false for query3 (for most queries 
		//		 this should be true)
		boolean isPredicate = false;
		System.out.println("No of results: " + 
							answerTable.getTripleRows().size());
/*		
		for(Map<String, Long> varIDMap : answerTable.getTripleRows()) {
			Set<Entry<String, Long>> entries = varIDMap.entrySet();
			for(Entry<String, Long> entry : entries) {
//				System.out.println(entry.getKey() + " : " + 
//						getStrValFromID(entry.getValue(), isPredicate));
				System.out.println(entry.getKey() + " : " + entry.getValue());
			}
			System.out.println();
		}
*/		
		mongoS.close();
		localMongo.close();
	}
	
	private TriplesHolder checkAndPerformJoins(TriplesHolder prevBGPResults, 
			TriplesHolder currResultBGP, List<String> queryVars) {
		TriplesHolder filteredTripleTable = new TripleTable();
		Set<String> commonVars = prevBGPResults.getBgpVars();
		commonVars.retainAll(currResultBGP.getBgpVars());
		
		for(Map<String, Long> varIDMap1 : prevBGPResults.getTripleRows())
			for(Map<String, Long> varIDMap2 : currResultBGP.getTripleRows()) {				
				int i = 0;
				for(String var : commonVars) {
					if(varIDMap1.get(var).longValue() == 
						varIDMap2.get(var).longValue()) {
						i++;
						continue;
					}
					else {
						// all common variables should match(be equal) for the join.
						break;
					}
				}
				if(i == commonVars.size()) {
					// all common variables matched. So add this record to the
					// answer table, along with the required variables
					
					// get the values of whatever select clause vars are present 
					// in these two tables.
					Map<String, Long> resultIDMap = new HashMap<String, Long>();
					Set<String> resultBGPVars = new HashSet<String>();
					// assumption: if the common vars have to be checked against
					//			   the next BGP then they should be in the select
					//			   clause.
					for(String v : queryVars) {
						if(prevBGPResults.getBgpVars().contains(v)) {
							resultIDMap.put(v, varIDMap1.get(v));
							resultBGPVars.add(v);
						}
						else if(currResultBGP.getBgpVars().contains(v)) {
							resultIDMap.put(v, varIDMap2.get(v));
							resultBGPVars.add(v);
						}
					}
					filteredTripleTable.addTriple(resultIDMap);
					filteredTripleTable.setBgpVars(resultBGPVars);
				}					
			}
		
		return filteredTripleTable;
	}
	
	protected TripleTable fetchDataFromDB(Triple bgp) throws Exception {
		Node subject = bgp.getSubject();
		Node predicate = bgp.getPredicate();
		Node object = bgp.getObject();
		
		TripleTable tripleTable = new TripleTable();		
		BasicDBObject queryDoc = new BasicDBObject();
		boolean subVar = true;
		boolean isPredicate = false;
		if(!subject.isVariable()) { 
//			queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, 
//					getIDFromStrVal(FmtUtils.stringForNode(subject), isPredicate));
			queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, 
					getIDFromStrVal(subject.toString(), isPredicate));
			subVar = false;
		}
		
		boolean predVar = true;
		if(!predicate.isVariable()) {
			isPredicate = true;
			queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, 
					getIDFromStrVal(predicate.toString(), isPredicate));
			predVar = false;
		}
		
		boolean objVar = true;
		if(!object.isVariable()) {
			isPredicate = false;
			queryDoc.put(Constants.FIELD_TRIPLE_OBJECT, 
					getIDFromStrVal(object.toString(), isPredicate));
			objVar = false;
		}
		
		Set<String> bgpVars = new HashSet<String>();
		DBCursor resultCursor;
		if(subVar && predVar && objVar) {
			// this means all the 3 are variables in the form ?s ?p ?o
			// so fetch all the data
			resultCursor = tripleCollection.find();
			
			// fill the map and populate TripleTable
			bgpVars.add(subject.toString());
			bgpVars.add(predicate.toString());
			bgpVars.add(object.toString());
			
			while(resultCursor.hasNext()) {
				DBObject tripleRow = resultCursor.next();
				Map<String, Long> varIDMap = new HashMap<String, Long>();
				varIDMap.put(subject.toString(), 
						(Long)tripleRow.get(Constants.FIELD_TRIPLE_SUBJECT));
				varIDMap.put(predicate.toString(), 
						(Long)tripleRow.get(Constants.FIELD_TRIPLE_PREDICATE));
				varIDMap.put(object.toString(), 
						(Long)tripleRow.get(Constants.FIELD_TRIPLE_OBJECT));
			}
		}
		else {
			
			// assumption: predicates as variable is not considered here. 
			// This information is used to perform joins. So it is not 
			// considered and if predicates are in the select clause variables 
			// (to be displayed) then it is ignored.
			
			BasicDBObject fields = new BasicDBObject();
			if(subVar) {
				bgpVars.add(subject.toString());
				fields.put(Constants.FIELD_TRIPLE_SUBJECT, 1);
				if(objVar) {
					bgpVars.add(object.toString());
					fields.put(Constants.FIELD_TRIPLE_OBJECT, 1);
					resultCursor = tripleCollection.find(queryDoc, fields);
					while(resultCursor.hasNext()) {
						DBObject tripleRow = resultCursor.next();
						Map<String, Long> varIDMap = new HashMap<String, Long>();
						varIDMap.put(subject.toString(), 
								(Long)tripleRow.get(Constants.FIELD_TRIPLE_SUBJECT));
						varIDMap.put(object.toString(), 
								(Long)tripleRow.get(Constants.FIELD_TRIPLE_OBJECT));
						tripleTable.addTriple(varIDMap);
					}
				}
				else {
					resultCursor = tripleCollection.find(queryDoc, fields);
					while(resultCursor.hasNext()) {
						DBObject tripleRow = resultCursor.next();
						Map<String, Long> varIDMap = new HashMap<String, Long>();
						varIDMap.put(subject.toString(), 
								(Long)tripleRow.get(Constants.FIELD_TRIPLE_SUBJECT));
						tripleTable.addTriple(varIDMap);
					}
				}
			}
			else {
				if(objVar) {
					bgpVars.add(object.toString());
					fields.put(Constants.FIELD_TRIPLE_OBJECT, 1);
					resultCursor = tripleCollection.find(queryDoc, fields);
					while(resultCursor.hasNext()) {
						DBObject tripleRow = resultCursor.next();
						Map<String, Long> varIDMap = new HashMap<String, Long>();
						varIDMap.put(subject.toString(), 
								(Long)tripleRow.get(Constants.FIELD_TRIPLE_OBJECT));
						tripleTable.addTriple(varIDMap);
					}
				}
			}
			tripleTable.setBgpVars(bgpVars);
		}
		return tripleTable;
	}
	
	public void testTripleRetrieval(String query) throws Exception {
		Query queryObj = QueryFactory.create(query);
		QueryVisitor queryVisitor = new QueryVisitor(queryObj);
		Op op = Algebra.compile(queryObj);
		op.visit(queryVisitor);
		List<Triple> basicGraphPatterns = queryVisitor.getBasicGraphPatterns();
		List<String> queryVars = queryVisitor.getQueryVariables();
		
		if(basicGraphPatterns.size() != 2)
			throw new Exception("Testing for only 2 BGPs");
		
		// fetch first bgp from DB and query other using the subject id
		GregorianCalendar start = new GregorianCalendar();
		TriplesHolder lastBGPData = fetchDataFromDB(
				basicGraphPatterns.get(basicGraphPatterns.size()-1));
		System.out.print("Time to retrieve first BGP: ");
		Util.getElapsedTime(start);
		start = new GregorianCalendar();
		System.out.println("Size of first BGP: " + lastBGPData.getTripleRows().size());
		BasicDBObject queryDoc = new BasicDBObject();
		TriplesHolder answerTable = new TripleTable();
		long count = 0;
		for(Map<String, Long> tripleRow : lastBGPData.getTripleRows()) {
			Long subjectID = tripleRow.get("?x");
			queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, subjectID);
			DBCursor cursor = tripleCollection.find(queryDoc);
			while(cursor.hasNext()) {
				DBObject resultDoc = cursor.next();
				Map<String, Long> varIDMap = new HashMap<String, Long>();
				varIDMap.put("subject", 
						(Long)resultDoc.get(Constants.FIELD_TRIPLE_SUBJECT));
				varIDMap.put("predicate", 
						(Long)resultDoc.get(Constants.FIELD_TRIPLE_PREDICATE));
				varIDMap.put("object", 
						(Long)resultDoc.get(Constants.FIELD_TRIPLE_OBJECT));
				answerTable.addTriple(varIDMap);
			}
			queryDoc.clear();
			count++;
			if(count%10000 == 0) {
				System.out.print("Processed 10k, took: ");
				Util.getElapsedTime(start);
				break;
			}
		}
		System.out.println("No of results: " + answerTable.getTripleRows().size());
		mongoS.close();
		localMongo.close();
	}
	
	private long getIDFromStrVal(String value, boolean isPredicate) throws Exception {
		// get hash digest of this value and then query DB
		String digestValue = Util.generateMessageDigest(value);
		DBObject resultID = null;
		if(isPredicate) {
			if(value.equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") ||
					value.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") ||
					value.equals("rdf:type"))
				return new Long(2);
			resultID = eidValCollection.findOne(new BasicDBObject(
					Constants.FIELD_HASH_VALUE, digestValue));
		}
		else 
			resultID = idValCollection.findOne(new BasicDBObject(
								Constants.FIELD_HASH_VALUE, digestValue));
		if(resultID == null)
			throw new Exception("ID not found for: " + value + 
					" and its digest value: " + digestValue);
		return (Long)resultID.get(Constants.FIELD_ID);
	}
	
	private String getStrValFromID(long id, boolean isPredicate) throws Exception {
		DBObject resultDoc = null;
		if(isPredicate)
			resultDoc = eidValCollection.findOne(new BasicDBObject(
					Constants.FIELD_ID, id));
		else
			resultDoc = idValCollection.findOne(new BasicDBObject(
								Constants.FIELD_ID, id));
		if(resultDoc == null)
			throw new Exception("Str value not found for: " + id);
		return (String)resultDoc.get(Constants.FIELD_STR_VALUE);
	}
	
	public static void main(String[] args) throws Exception {
		
		if(args.length != 1) 
			throw new Exception("Pass either true or false");
		
		// query-1 in LUBM
		String testQuery1 = "SELECT ?x WHERE { " +
			"?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
				"<http://lubm.example.org#GraduateStudent> . " + 			// 208669
			"?x <http://lubm.example.org#takesCourse> " +
				"<http://www.Department0.University0.edu/GraduateCourse0> " + // 2
			"} ";
		
		
		// query-2 in LUBM
		String testQuery2 = "SELECT ?x ?y ?z WHERE { " +
	"?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
	"	<http://lubm.example.org#GraduateStudent> . " + 				// 208669
	"?y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
	"	<http://lubm.example.org#University> . " +						// 32939
	"?z <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
	"	<http://lubm.example.org#Department> . " + 						// 2074
	"?x <http://lubm.example.org#memberOf> ?z . " + 					// 1030499
	"?z <http://lubm.example.org#subOrganizationOf> ?y . " + 			// 31971
	"?x <http://lubm.example.org#undergraduateDegreeFrom> ?y " +		// 253463
	"} ";
		
		// query-3 in LUBM 
		String testQuery3 = "SELECT ?x WHERE { " +
			"?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
				"<http://lubm.example.org#Publication> . " + 
			"?x <http://lubm.example.org#publicationAuthor> " +
				"<http://www.Department0.University0.edu/AssistantProfessor0> " +
			"} ";
		
		
		String testQuery3_1 = "select ?x ?y ?c where { " + 
			"?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
				"<http://lubm.example.org#GraduateStudent> . " +
			"?x <http://lubm.example.org#undergraduateDegreeFrom> ?y . " +
			"?x <http://lubm.example.org#takesCourse> ?c . " +
			"?x <http://lubm.example.org#advisor> " +
				"<http://www.Department0.University0.edu/AssistantProfessor1> . " +
			"?x <http://lubm.example.org#memberOf> <http://www.Department0.University0.edu> . " +
			"?x <http://lubm.example.org#teachingAssistantOf> " +
				"<http://www.Department0.University0.edu/Course30> . " +
			"} ";
		
		// largest first 
		String testQuery3_1_reordered1 = "select ?x ?y ?c where { " + 
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
		
		// smallest first
		String testQuery3_1_reordered2 = "select ?x ?y ?c where { " + 
		"?x <http://lubm.example.org#takesCourse> ?c . " +
		"?x <http://lubm.example.org#undergraduateDegreeFrom> ?y . " +	
		"?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
			"<http://lubm.example.org#GraduateStudent> . " +	
		"?x <http://lubm.example.org#memberOf> <http://www.Department0.University0.edu> . " +
		"?x <http://lubm.example.org#advisor> " +
			"<http://www.Department0.University0.edu/AssistantProfessor1> . " +
		"?x <http://lubm.example.org#teachingAssistantOf> " +
			"<http://www.Department0.University0.edu/Course30> . " +
		"} ";
		
		// query-4 in LUBM -- this requires some inferencing. "Professor" is
		// not found in the triples
		String testQuery4 = "SELECT ?x ?y1 ?y2 ?y3 WHERE { " +
			"?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
				"<http://lubm.example.org#Professor> . " + 
			"?x <http://lubm.example.org#worksFor> " +
				"<http://www.Department0.University0.edu> . " + 
			"?x <http://lubm.example.org#name> ?y1 . " + 
			"?x <http://lubm.example.org#emailAddress> ?y2 . " +
			"?x <http://lubm.example.org#telephone> ?y3 " +
			"} ";
		
		String shortQuery = "select ?x ?y ?c where { " + 
		"?x <http://lubm.example.org#undergraduateDegreeFrom> ?y . " +
		"?x <http://lubm.example.org#takesCourse> ?c . " +
		"} ";
		
		if(Boolean.parseBoolean(args[0])) {		
			GregorianCalendar start = new GregorianCalendar();
			new QueryExecutorWithoutOp().processQuery(shortQuery);
			Util.getElapsedTime(start);
		}
		else {
			GregorianCalendar start = new GregorianCalendar();
			new QueryExecutorWithoutOp().testTripleRetrieval(shortQuery);
			Util.getElapsedTime(start);
		}			
	}
}
