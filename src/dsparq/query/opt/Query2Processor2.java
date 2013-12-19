package dsparq.query.opt;

import java.util.Collection;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import dsparq.misc.Constants;
import dsparq.query.QueryVisitor;
import dsparq.query.TripleTable;
import dsparq.util.BPlusTree;
import dsparq.util.Util;

/**
 * Processes LUBM query-2 using HashTables
 * 
 * @author Raghava
 *
 */
public class Query2Processor2 {
	
	private Mongo mongoS;
	private Mongo localMongo;
	protected DBCollection tripleCollection; 
	protected DBCollection idValCollection;
	protected DBCollection eidValCollection;
	private GregorianCalendar start;
	
	public Query2Processor2() {
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
		start = new GregorianCalendar();
		GregorianCalendar start1 = new GregorianCalendar();
		Query queryObj = QueryFactory.create(query);
		QueryVisitor queryVisitor = new QueryVisitor(queryObj);
		Op op = Algebra.compile(queryObj);
		op.visit(queryVisitor);
		List<Triple> basicGraphPatterns = queryVisitor.getBasicGraphPatterns();
		
		Triple bgp1 = basicGraphPatterns.get(0);
		Set<Long> bgp1Result = fetchDataForBGP(
				bgp1.getSubject().toString(),
				getIDFromStrVal(bgp1.getPredicate().toString(), true), 
				getIDFromStrVal(bgp1.getObject().toString(), false));
		
		Triple bgp2 = basicGraphPatterns.get(1);
		Map<Long, Set<Long>> bgp2Result = fetchDataForBGP2(bgp1Result, 
				bgp2.getSubject().toString(),
				getIDFromStrVal(bgp2.getPredicate().toString(), true),
				bgp2.getObject().toString());
		bgp1Result.clear();
		
		Triple bgp3 = basicGraphPatterns.get(2);
		Set<Long> bgp3Result = fetchDataForBGP3(bgp2Result, 
				bgp3.getSubject().toString(),
				getIDFromStrVal(bgp3.getPredicate().toString(), true), 
				getIDFromStrVal(bgp3.getObject().toString(), false));
		
		Triple bgp4 = basicGraphPatterns.get(3);
		Set<Long> bgp4Result = fetchDataForBGP(
				bgp4.getSubject().toString(),
				getIDFromStrVal(bgp4.getPredicate().toString(), true), 
				getIDFromStrVal(bgp4.getObject().toString(), false));
		System.out.println("Num results for BGP-4: " + bgp4Result.size());
		
		Triple bgp5 = basicGraphPatterns.get(4);
		fetchDataForBGP4(bgp4Result, bgp3Result, 
				bgp5.getSubject().toString(),
				bgp5.getObject().toString(),
				getIDFromStrVal(bgp5.getPredicate().toString(), true));
		bgp4Result.clear();
		bgp3Result.clear();
		bgp2Result.clear();
		System.out.println("Total time");
		Util.getElapsedTime(start1);
		localMongo.close();
		mongoS.close();
	}
	
	private void fetchDataForBGP4(Set<Long> prevResult1, 
			Set<Long> prevResult2, String subVar, String objVar, 
			Long predicateID) {
		GregorianCalendar start4 = new GregorianCalendar();
		Set<Long> queryResults = new HashSet<Long>();
		BasicDBObject queryDoc = new BasicDBObject();
		queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, predicateID);
		long numResults1 = 0;
		for(Long prevVal : prevResult1) {
			queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, prevVal);
			DBCursor cursor = tripleCollection.find(queryDoc);
			while(cursor.hasNext()) {
				DBObject resultDoc = cursor.next();
				queryResults.add((Long) resultDoc.get(Constants.FIELD_TRIPLE_OBJECT));
				numResults1++;
			}
			queryDoc.remove(Constants.FIELD_TRIPLE_SUBJECT);
		}
		System.out.println("first join done: " + numResults1 + 
				" Set size: " + queryResults.size());
		Util.getElapsedTime(start4);
		start = new GregorianCalendar();
		
		queryResults.retainAll(prevResult2);
		System.out.println("Second join done, results: " + queryResults.size());
		Util.getElapsedTime(start);
	}
	
	private Set<Long> fetchDataForBGP(String subjectVar, 
			Long predicateID, Long objectID) {
		Set<Long> queryResults = new HashSet<Long>();
		BasicDBObject queryDoc = new BasicDBObject();
		queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, predicateID);
		queryDoc.put(Constants.FIELD_TRIPLE_OBJECT, objectID);
		DBCursor cursor = tripleCollection.find(queryDoc, 
				new BasicDBObject(Constants.FIELD_TRIPLE_SUBJECT, 1));
		while(cursor.hasNext()) {
			DBObject resultDoc = cursor.next();
			queryResults.add(
					(Long) resultDoc.get(Constants.FIELD_TRIPLE_SUBJECT));
		}	
		return queryResults;
	}
	
	private Set<Long> fetchDataForBGP3(Map<Long, Set<Long>> prevResult, 
			String subjectVar, 
			Long predicateID, Long objectID) {
		Set<Long> queryResults = new HashSet<Long>();
		Collection<Set<Long>> prevValues = prevResult.values();
		BasicDBObject queryDoc = new BasicDBObject();
		queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, predicateID);
		queryDoc.put(Constants.FIELD_TRIPLE_OBJECT, objectID);
		for(Set<Long> vals : prevValues) {
			for(Long prevID : vals) {
				queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, prevID);
				DBCursor cursor = tripleCollection.find(queryDoc);
				if(cursor.count() > 0)
					queryResults.add(prevID);	
				queryDoc.remove(Constants.FIELD_TRIPLE_SUBJECT);
			}
		}
		return queryResults;
	}
	
	private Map<Long, Set<Long>> fetchDataForBGP2(Set<Long> prevResult, 
			String subjectVar, 
			Long predicateID, String objectVar) {
		Map<Long, Set<Long>> queryResults = new HashMap<Long, Set<Long>>();
		BasicDBObject queryDoc = new BasicDBObject();
		queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, predicateID);
		for(Long prevID : prevResult) {
			queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, prevID);
			DBCursor cursor = tripleCollection.find(queryDoc);
			while(cursor.hasNext()) {
				DBObject resultDoc = cursor.next();
				Long resultObjID = (Long) resultDoc.get(Constants.FIELD_TRIPLE_OBJECT);
				if(queryResults.containsKey(resultObjID)) {
					queryResults.get(resultObjID).add(prevID);
				}
				else {
					Set<Long> valueSet = new HashSet<Long>();
					valueSet.add(prevID);
					queryResults.put(resultObjID, valueSet);
				}
			}	
			queryDoc.remove(Constants.FIELD_TRIPLE_SUBJECT);
		}
		return queryResults;
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
	
	public static void main(String[] args) throws Exception {
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
		
		// until bgp5, it takes 62 secs. Use threading to improve speed
		// use diff. data structures to save space -- analysis of query
		
		new Query2Processor2().processQuery(testQuery2);
	}
}

