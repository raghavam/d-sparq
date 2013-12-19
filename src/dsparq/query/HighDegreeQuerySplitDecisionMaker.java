package dsparq.query;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.alg.DijkstraShortestPath;

import com.hp.hpl.jena.graph.Triple;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import dsparq.misc.Constants;
import dsparq.util.Util;


/**
 * Similar to QuerySplitDecisionMaker but also considers high degree vertices
 * when finding the core vertex.
 * 
 * @author Raghava
 *
 */
public class HighDegreeQuerySplitDecisionMaker extends QuerySplitDecisionMaker {
	
	private Set<String> markedVertices;
	private Set<String> unMarkedVertices;
	private String highDegreeTypeVerticesPath;
	
	public HighDegreeQuerySplitDecisionMaker(String hdTypePath) {
		this.highDegreeTypeVerticesPath = hdTypePath;
	}

	@Override
	protected int findDOFE(String vertex, Set<String> vertices) {
		
		int longestPathDistance = -1;
		try {
			findHighDegreeVerticesFromQuery();
			if(markedVertices.isEmpty() && unMarkedVertices.isEmpty()) {
				return super.findDOFE(vertex, vertices);
			}
				
			List<Integer> shortestPaths = new ArrayList<Integer>();
			for(String adjVertex : vertices) {
				if(vertex.equals(adjVertex))
					continue;
				shortestPaths.add(
						getShortestDistanceBetweenVertices(vertex, adjVertex));
			}
			Collections.sort(shortestPaths);
			longestPathDistance = shortestPaths.get(shortestPaths.size()-1);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return longestPathDistance;
	}
	
	private int getShortestDistanceBetweenVertices(String v1, String v2) {
		if(unMarkedVertices.contains(v1)) {
			if(queryGraph.containsEdge(v1, v2))
				return 1;
			else
				return Integer.MAX_VALUE;
		}
		else {
			List<String> pathEdges = 
				DijkstraShortestPath.findPathBetween(queryGraph, v1, v2);
			int pathLength = 1;
			for(int i=1; i < pathEdges.size(); i++) {
				// skipping the first edge because source of first edge
				// is v1 which might not be marked (or unmarked)
				
				// I am following this to check shortest path: vertices in the
				// path cannot be unmarked except the last vertex.
				
				// target of this edge is same as source of next edge - so not
				// checking target vertices here.
				String edgeSource = queryGraph.getEdgeSource(pathEdges.get(i));
				if(i == pathEdges.size()-1) {
					// it doesn't matter if the last edge is an unmarked vertex
					pathLength++;
					continue;
				}
				if(unMarkedVertices.contains(edgeSource)) {
					pathLength = Integer.MAX_VALUE;
					break;			
				}
				else {
					pathLength++;	
				}
			}
			return pathLength;
		}
	}
	
	private void findHighDegreeVerticesFromQuery() throws Exception {
		
		Set<Long> highDegreeTypeIDs = readHighDegreeTypeIDs();
		
		Mongo mongo = new Mongo(Constants.MONGO_LOCAL_HOST, Constants.MONGO_PORT);
		DB db = mongo.getDB(Constants.MONGO_DB);
		DBCollection idValCollection = db.getCollection(
				Constants.MONGO_IDVAL_COLLECTION);
		Set<Long> typeIDs = new HashSet<Long>();
		Map<Long, String> vertexIDTripleSubjectMap = new HashMap<Long, String>();
		
		for(Triple t : basicGraphPatterns) {
			String predicate = t.getPredicate().toString();
			if(predicate.equals(Constants.RDF_TYPE_URI)) {
				String object = t.getObject().toString();
				long vertexID = getID(object, idValCollection);
				typeIDs.add(vertexID);
				vertexIDTripleSubjectMap.put(vertexID, predicate);
			}
		}
		for(Long typeID : typeIDs) {
			if(highDegreeTypeIDs.contains(typeID))
				unMarkedVertices.add(vertexIDTripleSubjectMap.get(typeID));
			else
				markedVertices.add(vertexIDTripleSubjectMap.get(typeID));
		}
	}
	
	private Set<Long> readHighDegreeTypeIDs() throws Exception {
		Set<Long> hdTypeIDs = new HashSet<Long>();
		BufferedReader reader = new BufferedReader(new FileReader(
				new File(highDegreeTypeVerticesPath)));
		String line;
		while((line = reader.readLine()) != null) 
			hdTypeIDs.add(Long.parseLong(line.trim()));
		reader.close();		
		return hdTypeIDs;
	}
	
	private long getID(String token, DBCollection collection) throws Exception {
		BasicDBObject queryDoc = new BasicDBObject();
		String digestValue = Util.generateMessageDigest(token);
		queryDoc.put(Constants.FIELD_HASH_VALUE, digestValue);
		DBObject resultDoc = collection.findOne(queryDoc);
		long id = (Long) resultDoc.get(Constants.FIELD_ID);
		return id;
	}
	
	public static void main(String[] args) {
		if(args.length != 1) {
			try {
				throw new Exception("Specify the path to high degree type IDs");
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
		List<String> queries = 
			new HighDegreeQuerySplitDecisionMaker(args[0]).splitQuery("", 1);
	}
}
