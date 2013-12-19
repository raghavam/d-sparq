package dsparq.query.opt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
	private DBCollection predicateSelectivityCollection;
//	private static String outputFileName;
	
	public ThreadedQueryExecutor2() {
		idCache = new HashMap<String, String>();
		idValCollection = localDB.getCollection(Constants.MONGO_IDVAL_COLLECTION);	
		predicateSelectivityCollection = localDB.getCollection(
				Constants.MONGO_PREDICATE_SELECTIVITY);
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
//			System.out.println("DirectedGraph: " + directedQueryGraph.toString());
			BiConFinder biconFinder = new BiConFinder();
			QueryPattern queryPattern = biconFinder.getQueryPatterns(
						undirectedQueryGraph, directedQueryGraph);
			System.out.println(queryPattern.toString());
			executePattern(queryPattern);
		}
		finally {
			localMongo.close();
		}
	}
	
	private void executePattern(QueryPattern queryPattern) throws Exception {	
		DBObject queryDoc;
		if(queryPattern instanceof NumericalTriplePattern) {
			queryDoc = handleNumericalTriplePattern(
					(NumericalTriplePattern) queryPattern);
		}
		else if(queryPattern instanceof StarPattern) {
			queryDoc = handleStarPattern((StarPattern) queryPattern);
		}
		else if(queryPattern instanceof PipelinePattern) {
			PipelinePattern pipelinePattern = 
					(PipelinePattern) queryPattern;
			queryDoc = handlePipelinePattern(pipelinePattern);
		}
		else
			throw new Exception("Unexpected type " + 
						queryPattern.toString());
		DBCursor cursor; 
//		System.out.println(queryDoc.toString());
		cursor = (queryDoc == null) ? starSchemaCollection.find() : 
			starSchemaCollection.find(queryDoc);
		
//		System.out.println("No of docs: " + cursor.itcount());
//		PrintWriter writer = new PrintWriter(new BufferedWriter
//				(new FileWriter(new File(outputFileName))));

		long resultCount = 0;
		int i = 0;
		while(cursor.hasNext()) {
			DBObject result = cursor.next();
			//store result somewhere; print/count for now
//			writer.print("Sub: " + result.get(Constants.FIELD_TRIPLE_SUBJECT));
//			System.out.print( 
//					result.get(Constants.FIELD_TRIPLE_SUBJECT));
			BasicDBList predObjList = 
					(BasicDBList) result.get(Constants.FIELD_TRIPLE_PRED_OBJ);
			for(i=0; i<predObjList.size(); i++) {
//				writer.print("  Pred: " + item.get(Constants.FIELD_TRIPLE_PREDICATE));
//				writer.println("  Obj: " + item.get(Constants.FIELD_TRIPLE_OBJECT));
//				System.out.print("  " + 
//						item.get(Constants.FIELD_TRIPLE_PREDICATE));
//				System.out.println("  " + 
//						item.get(Constants.FIELD_TRIPLE_OBJECT));
				resultCount++;
			}		
		}
		System.out.println("Total results: " + resultCount);
		cursor.close();
//		writer.close();
		synchPhaser.arriveAndAwaitAdvance();
		threadPool.shutdown();	
		if(!threadPool.isTerminated()) {
			// wait for the tasks to complete
			boolean isTerminated = threadPool.awaitTermination(
					5, TimeUnit.SECONDS);
			System.out.println("isTerminated: " + isTerminated);
		}
	}
	
	/**
	 * Based on selectivity of the predicates, the patterns are reordered.
	 * @param starPattern
	 */
	private void reorderPatterns(StarPattern starPattern) throws Exception {
		StarPattern reOrderedStarPattern = new StarPattern();
		List<QueryPattern> patterns = starPattern.getQueryPatterns();
		List<Integer> countList = new ArrayList<Integer>(patterns.size());
		for(QueryPattern pattern : patterns) {
			if(pattern instanceof NumericalTriplePattern) {
				NumericalTriplePattern ntp = (NumericalTriplePattern) pattern;
				String predID = ntp.getPredicate().getEdgeLabel();
				if(predID.charAt(0) != '?') {
					DBObject countDoc = predicateSelectivityCollection.findOne(
							new BasicDBObject(Constants.FIELD_HASH_VALUE, predID));
					int count = (Integer) countDoc.get(
							Constants.FIELD_PRED_SELECTIVITY);
					//determine the position of this count in the countList
				}
				else {
					//make this the largest count predicate
				}
			}
			else if(pattern instanceof PipelinePattern) {
				
			}
			else
				throw new Exception("Unrecognised type");
		}
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
		Long longID = (Long) resultID.get(Constants.FIELD_ID);
		idCache.put(value, longID.toString());
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
		GregorianCalendar start = new GregorianCalendar();
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
		double secs = Util.getElapsedTime(start);
		System.out.println("Total Secs: " + secs + "  For " + 
				numTimes + ": " + secs/numTimes);
	}
}
