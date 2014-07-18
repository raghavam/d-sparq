package dsparq.query.opt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import jsr166y.Phaser;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import dsparq.misc.Constants;
import dsparq.query.QueryVisitor;
import dsparq.util.Util;

/**
 * Implementation of 2 BGP query which follows the
 * producer-consumer model
 * @author Raghava
 *
 */
public class ThreadedQueryExecutor {

	private ExecutorService threadPool;
//	private Mongo mongos;
	private Mongo localMongo;
	private DBCollection tripleCollection; 
	private DBCollection idValCollection;
	private DBCollection eidValCollection;
	private final int THREAD_WAIT_TIME = 3;
	private QueueHandler queueHandler;
	private List<Triple> basicGraphPatterns;
	private Phaser synchPhaser;
	private String bulkReadScript;
	private DB localDB;
	private DBCollection starSchemaCollection;
	
	public ThreadedQueryExecutor() {
		// check out cachedThreadPool as well
		threadPool = Executors.newFixedThreadPool(32);
		try {
//			mongos = new Mongo("nimbus2", 27017);
			localMongo = new Mongo("nimbus5", 10000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		localDB = localMongo.getDB(Constants.MONGO_RDF_DB);
//		DB sdb = mongos.getDB(Constants.MONGO_RDF_DB);
		tripleCollection = localDB.getCollection(Constants.MONGO_PARTITIONED_VERTEX_COLLECTION);
		idValCollection = localDB.getCollection(Constants.MONGO_IDVAL_COLLECTION);
		eidValCollection = localDB.getCollection(Constants.MONGO_EIDVAL_COLLECTION);
		starSchemaCollection = localDB.getCollection(Constants.MONGO_STAR_SCHEMA);
		// register self - current thread
		synchPhaser = new Phaser(1);
		
		bulkReadScript = 
			"function () { " +
				"var collection = db.getCollection('ptriples'); " +
				"var pred = arguments[arguments.length-1]; " +
				"var result = new Array(); " +
				"var i = 0; " +
				"for(var j=0; j<arguments.length-1; j++) { " +
					"var cursor = collection.find({ subject : arguments[j], predicate : pred }); " +
					"while(cursor.hasNext()) { " +
						"var doc = cursor.next(); " +
						"result[i] = doc['subject']; " +
						"i++; " +
						"result[i] = doc['object']; " +
						"i++; " +
					"} " +
				"} " +				
				"return result; " +
			"} ";
	}
	
	public void processQuery(String query) throws Exception {
		Query queryObj = QueryFactory.create(query);
		QueryVisitor queryVisitor = new QueryVisitor(queryObj);
		Op op = Algebra.compile(queryObj);
		op.visit(queryVisitor);
		basicGraphPatterns = queryVisitor.getBasicGraphPatterns();
		
//		testAllRetrieval(basicGraphPatterns);
				
		queueHandler = new QueueHandler();
//		List<String> queryVars = queryVisitor.getQueryVariables();
			
		System.out.println("Started fetching data...");
		// start getting data for the first triple pattern
//		fetchDataFromDB(basicGraphPatterns.get(0));
//		testFindOnStarSchema();
		sp2Query4();
		
//		synchPhaser.arriveAndAwaitAdvance();
		threadPool.shutdown();	
		if(!threadPool.isTerminated()) {
			// wait for the tasks to complete
			boolean isTerminated = threadPool.awaitTermination(
					THREAD_WAIT_TIME, TimeUnit.MINUTES);
			System.out.println("isTerminated: " + isTerminated);
		}
		System.out.println("Final result size: " + 
				queueHandler.getFinalResult().size());

				
/*		
		for(Map<String,Long> m : queueHandler.getFinalResult()) {
			Set<Entry<String,Long>> entries = m.entrySet();
			for(Entry<String,Long> entry : entries)
				System.out.print(entry.getKey() + "  " + entry.getValue() + "  ; ");
			System.out.println();
		}	
*/		
		localMongo.close();
//		mongos.close();
	}
	
	private void fetchDataFromDB(Triple bgp) throws Exception {
		BasicDBObject queryDoc = new BasicDBObject();
		if(!bgp.getSubject().isVariable()) 
			queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, 
					getIDFromStrVal(bgp.getSubject().toString(), false));
		if(!bgp.getPredicate().isVariable())
			queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, 
					getIDFromStrVal(bgp.getPredicate().toString(), true));
		if(!bgp.getObject().isVariable())
			queryDoc.put(Constants.FIELD_TRIPLE_OBJECT, 
					getIDFromStrVal(bgp.getObject().toString(), false));
		DBCursor cursor = tripleCollection.find(queryDoc);
		while(cursor.hasNext()) {
			DBObject resultDoc = cursor.next();
			Map<String, Long> result = new HashMap<String, Long>();
			if(bgp.getSubject().isVariable())
				result.put(bgp.getSubject().toString(), 
						(Long) resultDoc.get(Constants.FIELD_TRIPLE_SUBJECT)); 
			if(bgp.getPredicate().isVariable())
				result.put(bgp.getPredicate().toString(), 
						(Long) resultDoc.get(Constants.FIELD_TRIPLE_PREDICATE)); 
			if(bgp.getObject().isVariable())
				result.put(bgp.getObject().toString(), 
						(Long) resultDoc.get(Constants.FIELD_TRIPLE_OBJECT)); 
			// add to queueHandler
			queueHandler.add(0, result);
		}
		queueHandler.doneAdding(0);
	}
	
	private void testFindOnStarSchema() {
		BasicDBList conditions = new BasicDBList();
		BasicDBObject elemMatchValue = new BasicDBObject();
		elemMatchValue.put("predicate", new Long(2));
		elemMatchValue.put("object", new Long(-9));
		BasicDBObject elemMatchOp = new BasicDBObject("$elemMatch", elemMatchValue);
		BasicDBObject clause1 = new BasicDBObject("predobj", elemMatchOp);
		BasicDBObject clause2 = new BasicDBObject("predobj.predicate", new Long(17));
		BasicDBObject clause3 = new BasicDBObject("predobj.predicate", new Long(11));
		conditions.add(clause1);
		conditions.add(clause2);
		conditions.add(clause3);
		BasicDBObject andOp = new BasicDBObject("$and", conditions);
		DBCursor cursor = starSchemaCollection.find(andOp);
		int resultCount = 0;
		Map<String, Long> tripleMap;
		List<Map<String, Long>> tripleList = new ArrayList<Map<String, Long>>();
		while(cursor.hasNext()) {
			// there could be multiple ?c for a given ?x & ?y
			DBObject result = cursor.next();
			tripleMap = new HashMap<String, Long>();
			tripleMap.put(Constants.FIELD_TRIPLE_SUBJECT, 
					(Long) result.get(Constants.FIELD_TRIPLE_SUBJECT));
			BasicDBList predObjList = 
				(BasicDBList) result.get(Constants.FIELD_TRIPLE_PRED_OBJ);
			for(Object predObjItem : predObjList) {
				DBObject item = (DBObject) predObjItem;
				if((Long)item.get(Constants.FIELD_TRIPLE_PREDICATE) == 11) {
					resultCount++;
				}
				else if((Long)item.get(Constants.FIELD_TRIPLE_PREDICATE) == 17) {
			
				}
			}
		}
		System.out.println("Total results: " + resultCount);
	}
	
	private void sp2Query2() throws Exception {
/*		
		for(Triple t : basicGraphPatterns) {
			if(!t.getSubject().isVariable())
				System.out.println(t.getSubject().toString() + "  " +
						getIDFromStrVal(t.getSubject().toString(), false));
			if(!t.getPredicate().isVariable())
				System.out.println(t.getPredicate().toString() + "  " +
						getIDFromStrVal(t.getPredicate().toString(), true));
			if(!t.getObject().isVariable())
				System.out.println(t.getObject().toString() + "  " +
						getIDFromStrVal(t.getObject().toString(), false));
		}
*/		
		// db.starschema.find( { $and : [ { "predobj.predicate" : 14 }, 
		// { "predobj.predicate" : 50 }, { "predobj" : {$elemMatch : { predicate : 75, object : 15397572  } } }, 
		// { "predobj.predicate" : 8 }, { "predobj.predicate" : 24 }, { "predobj.predicate" : 13 }, 
		// { "predobj.predicate" : 23 }, { "predobj.predicate" : 71 }, { "predobj.predicate" : 15 } ] } )
		
		BasicDBList conditions = new BasicDBList();
//		BasicDBObject elemMatchValue1 = new BasicDBObject();
//		elemMatchValue1.put("predicate", new Long(82));
//		elemMatchValue1.put("object", new Long(40573065));
//		BasicDBObject elemMatchOp1 = new BasicDBObject("$elemMatch", elemMatchValue1);
//		BasicDBObject clause2 = new BasicDBObject("predobj", elemMatchOp1);
		
//		BasicDBObject clause1 = new BasicDBObject("predobj.predicate", new Long(6));
//		BasicDBObject clause2 = new BasicDBObject("predobj.predicate", new Long(58));
		BasicDBObject elemMatchValue = new BasicDBObject();
		elemMatchValue.put("predicate", new Long(82));
		elemMatchValue.put("object", new Long(40573065));
		BasicDBObject elemMatchOp = new BasicDBObject("$elemMatch", elemMatchValue);
		BasicDBObject clause3 = new BasicDBObject("predobj", elemMatchOp);
//		BasicDBObject clause4 = new BasicDBObject("predobj.predicate", new Long(11));
		conditions.add(clause3);
		BasicDBObject andOp = new BasicDBObject("$and", conditions);
		DBCursor cursor = starSchemaCollection.find(andOp);
		int count = 0;
		Map<String, Long> tripleMap = new HashMap<String, Long>();
		while(cursor.hasNext()) {
			DBObject result = cursor.next();
			tripleMap.clear();
			tripleMap.put(Constants.FIELD_TRIPLE_SUBJECT, 
					(Long) result.get(Constants.FIELD_TRIPLE_SUBJECT));
			BasicDBList predObjList = 
				(BasicDBList) result.get(Constants.FIELD_TRIPLE_PRED_OBJ);
			for(Object predObjItem : predObjList) {
				DBObject item = (DBObject) predObjItem;
				count++;
			}
		}
		System.out.println("Done. Results: " + count);
		tripleMap.clear();
	}
	
	private void sp2Query4() throws Exception {
		BasicDBList conditions = new BasicDBList();
		BasicDBObject elemMatchValue = new BasicDBObject();
		elemMatchValue.put("predicate", new Long(82));
		elemMatchValue.put("object", new Long(40573065));
		BasicDBObject elemMatchOp = new BasicDBObject("$elemMatch", elemMatchValue);
		BasicDBObject clause1 = new BasicDBObject("predobj", elemMatchOp);
		BasicDBObject clause2 = new BasicDBObject("predobj.predicate", new Long(26));
		BasicDBObject clause3 = new BasicDBObject("predobj.predicate", new Long(19));
		conditions.add(clause1);
		conditions.add(clause2);
		conditions.add(clause3);
		BasicDBObject andOp = new BasicDBObject("$and", conditions);
		DBCursor cursor = starSchemaCollection.find(andOp);
		int count = 0;
		List<Long> author1List = new ArrayList<Long>();
		while(cursor.hasNext()) {
			DBObject result = cursor.next();
			BasicDBList predObjList = 
				(BasicDBList) result.get(Constants.FIELD_TRIPLE_PRED_OBJ);
			for(Object predObjItem : predObjList) {
				DBObject item = (DBObject) predObjItem;
				if((Long)item.get(Constants.FIELD_TRIPLE_PREDICATE) == 19) 
					author1List.add((Long)item.get(Constants.FIELD_TRIPLE_OBJECT));
			}
		}
		System.out.println("Done. Results: " + author1List.size());
		// now query for foaf:name(83) on author1List
		for(Long author : author1List) {
			BasicDBObject doc = new BasicDBObject();
			// need index on subject + predob.predicate combination
			// remove index on idvals, eidvals.
		}
	}
	
	private long getIDFromStrVal(String value, boolean isPredicate) throws Exception {
		// get hash digest of this value and then query DB
		String digestValue = Util.generateMessageDigest(value);
		DBObject resultID = null;
		if(isPredicate) {
//			if(value.equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") ||
//					value.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") ||
//					value.equals("rdf:type"))
//				return new Long(2);
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
	
	public List<Triple> getBasicGraphPatterns() {
		return basicGraphPatterns;
	}
	
	public DBCollection getTripleCollection() {
		return tripleCollection;
	}
	
	public ExecutorService getThreadPool() {
		return threadPool;
	}
	
	public Phaser getSynchPhaser() {
		return synchPhaser;
	}
	
	private void testAllRetrieval(List<Triple> bgps) throws Exception {
		BasicDBObject queryDoc = new BasicDBObject();
/*		
		Triple triple0 = bgps.get(0);
		queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, 
				getIDFromStrVal(triple0.getPredicate().toString(), true));
		queryDoc.put(Constants.FIELD_TRIPLE_OBJECT, 
				getIDFromStrVal(triple0.getObject().toString(), false));
		DBCursor cursor = tripleCollection.find(queryDoc);
		Set<Long> result0 = new HashSet<Long>();
		while(cursor.hasNext()) {
			DBObject resultDoc = cursor.next();
			result0.add((Long) resultDoc.get(Constants.FIELD_TRIPLE_SUBJECT));
		}
		cursor.close();
		queryDoc.clear();
		System.out.println("Result0: " + result0.size());
*/		
		Triple triple1 = bgps.get(0);
		queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, 
				getIDFromStrVal(triple1.getPredicate().toString(), true));
		DBCursor cursor1 = tripleCollection.find(queryDoc);
		Set<Long> result1 = new HashSet<Long>();
		while(cursor1.hasNext()) {
			DBObject resultDoc = cursor1.next();
			result1.add((Long) resultDoc.get(Constants.FIELD_TRIPLE_SUBJECT));
		}
		cursor1.close();
		queryDoc.clear();
		System.out.println("Result1: " + result1.size());
//		result0.retainAll(result1);
//		System.out.println("After intersection: " + result0.size());
		
		//TODO: check bgp-1, not getting all results
		
		Triple triple2 = bgps.get(1);
		queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, 
				getIDFromStrVal(triple2.getPredicate().toString(), true));
		DBCursor cursor2 = tripleCollection.find(queryDoc);
		Set<Long> result2 = new HashSet<Long>();
		System.out.println("Cursor2 size:" + cursor2.count());
		while(cursor2.hasNext()) {
			DBObject resultDoc = cursor2.next();
			result2.add((Long) resultDoc.get(Constants.FIELD_TRIPLE_SUBJECT));
		}
		cursor2.close();
		queryDoc.clear();
		System.out.println("Result2: " + result2.size());
		result1.retainAll(result2);
		System.out.println("After intersection: " + result1.size());
		
	}

	public static void main(String[] args) throws Exception {
		String shortQuery = "select ?x ?y ?c where { " + 
		"?x <http://lubm.example.org#undergraduateDegreeFrom> ?y . " +
		"?x <http://lubm.example.org#takesCourse> ?c . " +
		"} ";
		
		String query2 = "select ?x ?y ?c where { " + 
		"?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
		"<http://lubm.example.org#GraduateStudent> . " +
		"?x <http://lubm.example.org#undergraduateDegreeFrom> ?y . " +
		"?x <http://lubm.example.org#takesCourse> ?c . " +
		"} ";
		
		String testQuery = "select ?x where { " + 
		"?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
		"<http://lubm.example.org#GraduateStudent> . " +
		"} ";
		
		String sp2Query2 = "SELECT ?inproc ?author ?booktitle ?title " + 
			"?proc ?ee ?page ?url ?yr WHERE { " +
			"?inproc <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
						"<http://localhost/vocabulary/bench/Inproceedings> . " + 
			"?inproc <http://purl.org/dc/elements/1.1/creator> ?author . " +
			"?inproc <http://localhost/vocabulary/bench/booktitle> ?booktitle . " +
			"?inproc <http://purl.org/dc/elements/1.1/title> ?title . " +
			"?inproc <http://purl.org/dc/terms/partOf> ?proc . " +
			"?inproc <http://www.w3.org/2000/01/rdf-schema#seeAlso> ?ee . " +
			"?inproc <http://swrc.ontoware.org/ontology#pages> ?page . " +
			"?inproc <http://xmlns.com/foaf/0.1/homepage> ?url . " +
			"?inproc <http://purl.org/dc/terms/issued> ?yr " +
			"} ";
		
		String sp2Query3 = "SELECT ?article " +
			"WHERE { " +
			"?article <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
					"<http://localhost/vocabulary/bench/Article> . " +
			"?article ?property ?value " +
			"} ";
		
		String sp2Query4 = "SELECT ?name1 ?name2 " +
			"WHERE { " +
			"?article1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
					"<http://localhost/vocabulary/bench/Article> . " +
			"?article2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
					"<http://localhost/vocabulary/bench/Article> . " +
			"?article1 <http://purl.org/dc/elements/1.1/creator> ?author1 . " +
			"?author1 <http://xmlns.com/foaf/0.1/name> ?name1 . " +
			"?article2 <http://purl.org/dc/elements/1.1/creator> ?author2 . " +
			"?author2 <http://xmlns.com/foaf/0.1/name> ?name2 . " +
			"?article1 <http://swrc.ontoware.org/ontology#journal> ?journal . " +
			"?article2 <http://swrc.ontoware.org/ontology#journal> ?journal " +
			"} ";
		
		String sp2Query5a = "SELECT DISTINCT ?person ?name " +
			"WHERE { " +
			"?article <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
					"<http://localhost/vocabulary/bench/Article> . " +
			"?article <http://purl.org/dc/elements/1.1/creator> ?person . " +
			"?inproc <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
					"<http://localhost/vocabulary/bench/Inproceedings> . " +
			"?inproc <http://purl.org/dc/elements/1.1/creator> ?person2 . " +
			"?person <http://xmlns.com/foaf/0.1/name> ?name . " +
			"?person2 <http://xmlns.com/foaf/0.1/name> ?name2 " +
			"} ";
		
		String sp2Query5b = "SELECT ?person ?name " +
			"WHERE { " +
			"?article <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
					"<http://localhost/vocabulary/bench/Article> . " +
			"?article <http://purl.org/dc/elements/1.1/creator> ?person . " +
			"?inproc <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
					"<http://localhost/vocabulary/bench/Inproceedings> . " +
			"?inproc <http://purl.org/dc/elements/1.1/creator> ?person . " +
			"?person <http://xmlns.com/foaf/0.1/name> ?name " +
			"} ";
		
		long startTime = System.nanoTime();
		new ThreadedQueryExecutor().processQuery(sp2Query4);
		Util.getElapsedTime(startTime);
	}
	
	
	class JoinProcessor extends Thread {
		
		private List<Map<String, Long>> elementsToProcess;
		private DBCollection tripleCollection;
		private Triple bgp;
		private int bgpIndex;

		public JoinProcessor(List<Map<String, Long>> triples, 
					DBCollection tripleCollection, 
					Triple bgp, int bgpIndex) {
			elementsToProcess = new ArrayList<Map<String, Long>>(triples.size());
			elementsToProcess.addAll(triples);
			this.tripleCollection = tripleCollection;
			this.bgp = bgp;
			this.bgpIndex = bgpIndex;
		}
		
		@Override
		public void run() {
			synchPhaser.register();
//TODO: here only subject based joins are considered for now
		//	BasicDBObject queryDoc = new BasicDBObject();
		//	Map<String,Long> varID = new HashMap<String,Long>();
		//	int i = 0;
			try {
/*				
				for(; i<elementsToProcess.size(); i++) {
					varID = elementsToProcess.get(i);
					queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, varID.get(
											bgp.getSubject().toString()));
					if(!bgp.getPredicate().isVariable())
						queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, 
								Util.getIDFromStrVal(idValCollection, eidValCollection, 
										bgp.getPredicate().toString(), true));
					if(!bgp.getObject().isVariable())
						queryDoc.put(Constants.FIELD_TRIPLE_OBJECT, 
								Util.getIDFromStrVal(idValCollection, eidValCollection, 
								bgp.getObject().toString(), false));
					DBCursor cursor = tripleCollection.find(queryDoc);
					
					while(cursor.hasNext()) {
						DBObject resultDoc = cursor.next();
						Map<String, Long> triple = new HashMap<String, Long>();
						triple.put(bgp.getSubject().toString(), 
								(Long) resultDoc.get(Constants.FIELD_TRIPLE_SUBJECT));
						if(bgp.getPredicate().isVariable())
							triple.put(bgp.getPredicate().toString(), 
									(Long) resultDoc.get(Constants.FIELD_TRIPLE_PREDICATE));
						if(bgp.getObject().isVariable())
							triple.put(bgp.getObject().toString(), 
									(Long) resultDoc.get(Constants.FIELD_TRIPLE_OBJECT));
						queueHandler.add(bgpIndex, triple);
					}
					queryDoc.clear();
				}
*/				
//				pipelinedRead();
				
				queueHandler.doneAdding(bgpIndex);
				synchPhaser.arrive();
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}	
/*		
		private void pipelinedRead() throws Exception {
				Long[] joinIDs = new Long[elementsToProcess.size() + 1];
				int i = 0;
				for(; i<elementsToProcess.size(); i++) { 
					Map<String, Long> varID = elementsToProcess.get(i);
					joinIDs[i] = varID.get(bgp.getSubject().toString()); 
				}
				joinIDs[i] = 
						Util.getIDFromStrVal(idValCollection, eidValCollection, 
						bgp.getPredicate().toString(), true);
				CommandResult result = localRdfDB.doEval(bulkReadScript, joinIDs);
				
				BasicDBList resultDoc = (BasicDBList) result.get("retval");
				for(int j = 0; j<resultDoc.size(); j++) {
					Long longID = (Long) resultDoc.get(j);
					Map<String, Long> triple = new HashMap<String, Long>();
					if(j%2 == 0)
						triple.put(bgp.getSubject().toString(), longID);
					else {
						triple.put(bgp.getObject().toString(), longID);
						queueHandler.add(bgpIndex, triple);
					}
				}
		}
*/		
	}
}
