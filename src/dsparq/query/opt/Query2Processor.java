package dsparq.query.opt;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.mongodb.MongoClient;

import dsparq.misc.Constants;
import dsparq.query.QueryVisitor;
import dsparq.query.TripleTable;
import dsparq.util.BPlusTree;
import dsparq.util.Util;

/**
 * Processes LUBM query-2 using B+ tree for storing data
 * (Test class for checking B+ tree)
 * @author Raghava
 *
 */
public class Query2Processor {
	
	private Mongo mongoS;
	private Mongo localMongo;
	protected DBCollection tripleCollection; 
	protected DBCollection idValCollection;
	protected DBCollection eidValCollection;
	private long startTime;
	
	public Query2Processor() {
		try {
			mongoS = new MongoClient("nimbus2", 27017);
			localMongo = new MongoClient(Constants.MONGO_LOCAL_HOST, 10000);
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
		startTime = System.nanoTime();
		Query queryObj = QueryFactory.create(query);
		QueryVisitor queryVisitor = new QueryVisitor(queryObj);
		Op op = Algebra.compile(queryObj);
		op.visit(queryVisitor);
		List<Triple> basicGraphPatterns = queryVisitor.getBasicGraphPatterns();
		
		Triple bgp1 = basicGraphPatterns.get(0);
		TripleTable bgp1Result = fetchDataForBGP(
				bgp1.getSubject().toString(),
				getIDFromStrVal(bgp1.getPredicate().toString(), true), 
				getIDFromStrVal(bgp1.getObject().toString(), false));
		
		Triple bgp2 = basicGraphPatterns.get(1);
		TripleTable bgp2Result = fetchDataForBGP2(bgp1Result, 
				bgp2.getSubject().toString(),
				getIDFromStrVal(bgp2.getPredicate().toString(), true),
				bgp2.getObject().toString());
		bgp1Result.clear();
		
		Triple bgp3 = basicGraphPatterns.get(2);
		TripleTable bgp3Result = fetchDataForBGP3(bgp2Result, 
				bgp3.getSubject().toString(),
				getIDFromStrVal(bgp3.getPredicate().toString(), true), 
				getIDFromStrVal(bgp3.getObject().toString(), false));
		
		Triple bgp4 = basicGraphPatterns.get(3);
		TripleTable bgp4Result = fetchDataForBGP(
				bgp4.getSubject().toString(),
				getIDFromStrVal(bgp4.getPredicate().toString(), true), 
				getIDFromStrVal(bgp4.getObject().toString(), false));
		System.out.println("Num results for BGP-4: " + bgp4Result.getTripleRows().size());
		
		Triple bgp5 = basicGraphPatterns.get(4);
		fetchDataForBGP4(bgp4Result, bgp3Result, 
				bgp5.getSubject().toString(),
				bgp5.getObject().toString(),
				getIDFromStrVal(bgp5.getPredicate().toString(), true));
		bgp4Result.clear();
		bgp3Result.clear();
		bgp2Result.clear();
		System.out.println("Time taken (secs): " + Util.getElapsedTime(startTime));
	}
	
	private void fetchDataForBGP4(TripleTable prevResult1, 
			TripleTable prevResult2, String subVar, String objVar, 
			Long predicateID) {
		BPlusTree<Vars> bgpDataTree = new BPlusTree<Vars>(51, new VarComparator());
		BasicDBObject queryDoc = new BasicDBObject();
		queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, predicateID);
		long numResults1 = 0;
		for(Map<String, Long> varMapID : prevResult1.getTripleRows()) {
			queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, varMapID.get(subVar));
			DBCursor cursor = tripleCollection.find(queryDoc);
			while(cursor.hasNext()) {
				DBObject resultDoc = cursor.next();
				Vars vars = new Vars(varMapID.get(subVar), 
						(Long) resultDoc.get(Constants.FIELD_TRIPLE_OBJECT));
				bgpDataTree.insert(vars);
				numResults1++;
			}
			queryDoc.remove(Constants.FIELD_TRIPLE_SUBJECT);
		}
		System.out.println("Tree constructed with " + numResults1 + " leaves");
		Util.getElapsedTime(startTime);
		startTime = System.nanoTime();
		
		boolean isPresent = false;
		long numResults2 = 0;
		System.out.println("Comparing all leaves with " + prevResult2.getTripleRows().size());
		for(Map<String, Long> varMapID : prevResult2.getTripleRows()) {
			Vars prevVars = new Vars(null, varMapID.get(objVar));
			isPresent = bgpDataTree.isPresent(prevVars);
			if(isPresent)
				numResults2++;
		}
		System.out.println("Searching done, results: " + numResults2);
		System.out.println("Time taken (secs): " + Util.getElapsedTime(startTime));
		startTime = System.nanoTime();
	}
	
	private TripleTable fetchDataForBGP(String subjectVar, 
			Long predicateID, Long objectID) {
		TripleTable tripleTable = new TripleTable();
		tripleTable.addBGPVar(subjectVar);
		BasicDBObject queryDoc = new BasicDBObject();
		queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, predicateID);
		queryDoc.put(Constants.FIELD_TRIPLE_OBJECT, objectID);
		DBCursor cursor = tripleCollection.find(queryDoc, 
				new BasicDBObject(Constants.FIELD_TRIPLE_SUBJECT, 1));
		while(cursor.hasNext()) {
			DBObject resultDoc = cursor.next();
			Map<String, Long> map = new HashMap<String, Long>();
			map.put(subjectVar, 
					(Long) resultDoc.get(Constants.FIELD_TRIPLE_SUBJECT));
			tripleTable.addTriple(map);
		}	
		return tripleTable;
	}
	
	private TripleTable fetchDataForBGP3(TripleTable prevResult, 
			String subjectVar, 
			Long predicateID, Long objectID) {
		TripleTable tripleTable = new TripleTable();
		tripleTable.addBGPVar(subjectVar);
		BasicDBObject queryDoc = new BasicDBObject();
		queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, predicateID);
		queryDoc.put(Constants.FIELD_TRIPLE_OBJECT, objectID);
		for(Map<String, Long> varMapID : prevResult.getTripleRows()) {
			queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, varMapID.get(subjectVar));
			DBCursor cursor = tripleCollection.find(queryDoc);
			while(cursor.hasNext()) {
				DBObject resultDoc = cursor.next();
				Map<String, Long> map = new HashMap<String, Long>();
				map.put(subjectVar, 
						(Long) resultDoc.get(Constants.FIELD_TRIPLE_SUBJECT));
				tripleTable.addTriple(map);
			}	
			queryDoc.remove(Constants.FIELD_TRIPLE_SUBJECT);
		}
		return tripleTable;
	}
	
	private TripleTable fetchDataForBGP2(TripleTable prevResult, 
			String subjectVar, 
			Long predicateID, String objectVar) {
		TripleTable tripleTable = new TripleTable();
		tripleTable.addBGPVar(subjectVar);
		tripleTable.addBGPVar(objectVar);
		BasicDBObject queryDoc = new BasicDBObject();
		queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, predicateID);
		for(Map<String, Long> varMapID : prevResult.getTripleRows()) {
			queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, varMapID.get(subjectVar));
			DBCursor cursor = tripleCollection.find(queryDoc);
			while(cursor.hasNext()) {
				DBObject resultDoc = cursor.next();
				Map<String, Long> map = new HashMap<String, Long>();
				map.put(subjectVar, 
						(Long) resultDoc.get(Constants.FIELD_TRIPLE_SUBJECT));
				map.put(objectVar, 
						(Long) resultDoc.get(Constants.FIELD_TRIPLE_OBJECT));
				tripleTable.addTriple(map);
			}	
			queryDoc.remove(Constants.FIELD_TRIPLE_SUBJECT);
		}
		return tripleTable;
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
		
		new Query2Processor().processQuery(testQuery2);
	}
}

class Vars {
	Long var1;
	Long var2;
	
	Vars(Long var1, Long var2) {
		this.var1 = var1;
		this.var2 = var2;
	}
}

class VarComparator implements Comparator<Vars> {
	
	@Override
	public int compare(Vars arg0, Vars arg1) {
		long diff = arg0.var2.longValue() - arg1.var2.longValue();
		if(diff == 0)
			return 0;
		else if(diff < 0)
			return -1;
		else
			return 1;
	}
}
