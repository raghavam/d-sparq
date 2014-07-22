package dsparq.query.opt;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.ClassBasedEdgeFactory;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.graph.SimpleGraph;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import dsparq.misc.Constants;
import dsparq.query.QueryVisitor;
import dsparq.query.analysis.BiConFinder;
import dsparq.query.analysis.NumericalTriplePattern;
import dsparq.query.analysis.PipelinePattern;
import dsparq.query.analysis.QueryPattern;
import dsparq.query.analysis.RDFVertex;
import dsparq.query.analysis.RelationshipEdge;
import dsparq.query.analysis.StarPattern;
import dsparq.util.Util;

/**
 * Processes the query according to the patterns involved in it.
 * @author Raghava
 *
 */
public class ThreadedQueryExecutor2 extends PatternHandler {

	private Map<String, String> idCache;
	private SimpleGraph<RDFVertex, DefaultEdge> undirectedQueryGraph;
	private SimpleDirectedGraph<RDFVertex, RelationshipEdge> directedQueryGraph;
	private DBCollection idValCollection;
//	private static String outputFileName;
	
	public ThreadedQueryExecutor2() {
		idCache = new HashMap<String, String>();
		idValCollection = localRdfDB.getCollection(Constants.MONGO_IDVAL_COLLECTION);	
	}
	
	public void processQuery(String query) throws Exception {
		try {
			Query queryObj = QueryFactory.create(query);
			QueryVisitor queryVisitor = new QueryVisitor(queryObj);
			Op op = Algebra.compile(queryObj);
			op.visit(queryVisitor);
			List<Triple> basicGraphPatterns = 
					queryVisitor.getBasicGraphPatterns();
			constructGraphs(basicGraphPatterns, 
					queryVisitor.getQueryVariables());
			//find if there are any independent pieces
			ConnectivityInspector<RDFVertex, RelationshipEdge> 
				connectivityInspector = 
					new ConnectivityInspector<RDFVertex, RelationshipEdge>(
							directedQueryGraph);
			List<Set<RDFVertex>> disconnectedSets = 
					connectivityInspector.connectedSets();
			List<QueryPattern> queryPatterns = 
					new ArrayList<QueryPattern>(disconnectedSets.size());
			BiConFinder biconFinder = new BiConFinder();
			if(disconnectedSets.size() == 1) {
				queryPatterns.add(biconFinder.getQueryPatterns(
						undirectedQueryGraph, directedQueryGraph));
			}
			else {
				//get the graphs associated with each connected vertex set.
				for(Set<RDFVertex> disconnectedSet : disconnectedSets) {
					SimpleDirectedGraph<RDFVertex, RelationshipEdge> 
						dirGraph = 
						new SimpleDirectedGraph<RDFVertex, RelationshipEdge>(
							new ClassBasedEdgeFactory<RDFVertex, RelationshipEdge>(
									RelationshipEdge.class));
					SimpleGraph<RDFVertex, DefaultEdge> 
							undirGraph = new SimpleGraph<RDFVertex, DefaultEdge>(
									DefaultEdge.class);
					//construct a graph using these vertices
					for(RDFVertex v1 : disconnectedSet) {
						dirGraph.addVertex(v1);
						undirGraph.addVertex(v1);
						for(RDFVertex v2 : disconnectedSet) {
							RelationshipEdge e = directedQueryGraph.getEdge(v1, v2);
							if(e != null) {
								dirGraph.addVertex(v2);
								undirGraph.addVertex(v2);
								dirGraph.addEdge(v1, v2, e);
								undirGraph.addEdge(v1, v2);
							}
						}
					}
					queryPatterns.add(biconFinder.getQueryPatterns(
							undirGraph, dirGraph));
				}
			}
//			System.out.println("\nQuery Patterns: ");
			for(QueryPattern queryPattern : queryPatterns) {
//				System.out.println(queryPattern.toString() + "\n");
				synchPhaser.register();
				threadPool.execute(new PatternExecutor(queryPattern));
			}
//			executePattern(queryPattern);
			synchPhaser.arriveAndAwaitAdvance();
			threadPool.shutdown();	
			if(!threadPool.isTerminated()) {
				// wait for the tasks to complete
				boolean isTerminated = threadPool.awaitTermination(
						5, TimeUnit.SECONDS);
//				System.out.println("isTerminated: " + isTerminated);
			}
		}
		finally {
//			System.out.println(">>>>>>>Closing Mongo now...");
			localMongo.close();
		}
	}
	
	private void executePattern(QueryPattern queryPattern) throws Exception {	
		DBObject queryDoc;
		if(queryPattern instanceof NumericalTriplePattern) {
			/*
			 * If a query has only one pattern which is a NumericalTriplePattern
			 * then it is considered as a PipelinePattern by BiConFinder. So a
			 * check for NumericalTriplePattern should never happen.
			 */
			throw new Exception("Cannot be a NumericalTriplePattern. " + 
						queryPattern.toString());
		}
		else if(queryPattern instanceof StarPattern) {
			queryDoc = handleStarPattern((StarPattern) queryPattern, 
					null, null, null);
		}
		else if(queryPattern instanceof PipelinePattern) {
			queryDoc = handlePipelinePattern((PipelinePattern) queryPattern);
		}
		else
			throw new Exception("Unexpected type " + 
						queryPattern.toString());
		DBCursor cursor; 
//		System.out.println("Limiting results to " + LIMIT_RESULTS + 
//				" for testing.....");
//		if(queryDoc != null)
//			System.out.println("queryDoc: " + queryDoc.toString());
//		else
//			System.out.println("queryDoc is null");
		cursor = (queryDoc == null) ? starSchemaCollection.find() : 
			starSchemaCollection.find(queryDoc);
		
//		System.out.println("No of docs: " + cursor.itcount());
//		PrintWriter writer = new PrintWriter(new BufferedWriter
//				(new FileWriter(new File(outputFileName))));

		long resultCount = 0;
		long docCount = 0;
//		int i = 0;
		while(cursor.hasNext()) {
			DBObject result = cursor.next();
			//store result somewhere; print/count for now
//			writer.print("Sub: " + result.get(Constants.FIELD_TRIPLE_SUBJECT));
//			System.out.print( 
//					result.get(Constants.FIELD_TRIPLE_SUBJECT));
			docCount++;
			Long sub = (Long) result.get(Constants.FIELD_TRIPLE_SUBJECT);
			BasicDBList predObjList = 
					(BasicDBList) result.get(Constants.FIELD_TRIPLE_PRED_OBJ);
			for(Object predObj : predObjList) {
				BasicDBObject item = (BasicDBObject) predObj;
				Long pred = (Long) item.get(Constants.FIELD_TRIPLE_PREDICATE);
				Long obj = (Long) item.get(Constants.FIELD_TRIPLE_OBJECT);
				SubObj subObj = predSubObjMap.get(pred);
/*				
				if(subObj != null) {
					//check if subject is a constant, compare it and retrieve
					//the queue associated with the object.
					if(subObj.subject != null) {
						QueueHandler2 queueHandler = 
								dependentQueueMap.get(subObj.object);
						if(subObj.subject.charAt(0) != '?') {
							//if sub is a constant, it should match
							if(Long.parseLong(subObj.subject) == sub.longValue())
								queueHandler.addToQueue(obj);
						}
						else {
							//no need to check whether subjects are the same
							queueHandler.addToQueue(obj);
						}
					}
				}
*/				
//				writer.print("  Pred: " + item.get(Constants.FIELD_TRIPLE_PREDICATE));
//				writer.println("  Obj: " + item.get(Constants.FIELD_TRIPLE_OBJECT));
//				System.out.print("  " + 
//						item.get(Constants.FIELD_TRIPLE_PREDICATE));
//				System.out.println("  " + 
//						item.get(Constants.FIELD_TRIPLE_OBJECT));
				resultCount++;
			}		
		}
//		System.out.println("Checking queues for remaining items...");
		//Handle any remaining items in the queues
		Collection<QueueHandler2> queueHandlers = dependentQueueMap.values();
		for(QueueHandler2 queueHandler : queueHandlers)
			queueHandler.addToQueue(null);
//		System.out.println("#Docs: " + docCount);
		System.out.println("Results: " + resultCount);
		cursor.close();
//		writer.close();
	}
	
	private void printGraph() {
		System.out.println();
		Set<RelationshipEdge> edges = directedQueryGraph.edgeSet();
		for(RelationshipEdge e : edges)
			System.out.println(e.getEdgeSource() + "  " + e.getEdgeLabel() + 
					"  " + e.getEdgeTarget());
		System.out.println();
	}
	
	private void constructGraphs(List<Triple> bgps, List<String> queryVars) 
			throws Exception {
		undirectedQueryGraph = 
				new SimpleGraph<RDFVertex, DefaultEdge>(DefaultEdge.class);
		directedQueryGraph = new SimpleDirectedGraph<RDFVertex, RelationshipEdge>(
				new ClassBasedEdgeFactory<RDFVertex, RelationshipEdge>(
						RelationshipEdge.class));
		int count = 0;
		for(Triple bgp : bgps) {
			Node subNode = bgp.getSubject();
			String sub = subNode.toString().intern();
			RDFVertex subVertex = new RDFVertex();
			if(!subNode.isVariable()) {
				sub = getIDFromStrVal(sub);
				count = checkAndAddVertex(sub, subVertex, count);
			}
			else {
				subVertex.setLabel(sub);
				undirectedQueryGraph.addVertex(subVertex);
				directedQueryGraph.addVertex(subVertex);
			}
			Node predNode = bgp.getPredicate();
			String pred = predNode.toString().intern();
			if(!predNode.isVariable()) 
				pred = getIDFromStrVal(pred);
			Node objNode = bgp.getObject();
			String obj = objNode.toString().intern();
			RDFVertex objVertex = new RDFVertex();
			if(!objNode.isVariable()) {
				obj = getIDFromStrVal(obj);
				count = checkAndAddVertex(obj, objVertex, count);
			}
			else {
				objVertex.setLabel(obj);
				undirectedQueryGraph.addVertex(objVertex);
				directedQueryGraph.addVertex(objVertex);
			}			
			undirectedQueryGraph.addEdge(subVertex, objVertex);
			directedQueryGraph.addEdge(subVertex, objVertex, 
					new RelationshipEdge(subVertex, objVertex, pred));
		}
	}
	
	private int checkAndAddVertex(String label, RDFVertex vertex, 
			int count) throws Exception {
		vertex.setLabel(label);
		boolean isNewVertex = undirectedQueryGraph.addVertex(vertex);
		directedQueryGraph.addVertex(vertex);
		if(!isNewVertex) {
			//rename the constant
			vertex.setRealLabel(false);
			vertex.setOriginalLabel(label);
			vertex.setLabel(label+count);
			count++;
			isNewVertex = undirectedQueryGraph.addVertex(vertex);
			directedQueryGraph.addVertex(vertex);
			if(!isNewVertex) //this cannot happen
				throw new Exception("Vertex repeated: " + vertex); //something wrong here
		}
		return count;
	}
	
	private String getIDFromStrVal(String value) throws Exception {
		// get hash digest of this value and then query DB
		String id = idCache.get(value);
		if(id != null)
			return id;
		String digestValue = Util.generateMessageDigest(value);
		DBObject resultID = null;
		resultID = idValCollection.findOne(new BasicDBObject(
								Constants.FIELD_HASH_VALUE, digestValue));
		if(resultID == null)
			throw new Exception("ID not found for: " + value + 
					" and its digest value: " + digestValue);
		Double numID = (Double) resultID.get(Constants.FIELD_NUMID);
		idCache.put(value, Long.toString(numID.longValue()));
		return idCache.get(value);
	}
	
	public static void main(String[] args) throws Exception {
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
		
		if(args.length != 2) {
			System.out.println("query file (only one query) and " +
					"number of times to execute query");
			System.exit(-1);
		}
		long startTime = System.nanoTime();
		File queryFile = new File(args[0]);
//		outputFileName = queryFile.getName() + "-opt";
		int numTimes = Integer.parseInt(args[1]);
		for(int i = 0; i < numTimes; i++) {
			Scanner scanner = new Scanner(queryFile);
			StringBuilder query = new StringBuilder();
			while(scanner.hasNext()) 
				query.append(scanner.nextLine());
			scanner.close();
			new ThreadedQueryExecutor2().processQuery(query.toString());
		}
		double secs = Util.getElapsedTime(startTime);
		System.out.println("Total time taken (secs): " + secs + 
				"  Avg time across " + numTimes + " runs: " + secs/numTimes);
	}
	
	class PatternExecutor implements Runnable {

		QueryPattern queryPattern;
		PatternExecutor(QueryPattern queryPattern) {
			this.queryPattern = queryPattern;
		}
		
		@Override
		public void run() {
			try {
				executePattern(queryPattern);
			}catch (Exception e) {
				e.printStackTrace();
			}
			finally {
				synchPhaser.arrive();
			}
		}		
	}
}
