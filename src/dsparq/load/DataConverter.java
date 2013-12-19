package dsparq.load;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import dsparq.misc.Constants;

// Not required now - use IDLoader and UndirectedGraphPartitioner
// @author Raghava
@Deprecated
public class DataConverter {
	
	public void constructAdjList() throws Exception {
		// read IDs from the saved bi-di-map
		ObjectInputStream mapReader = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream(Constants.SER_BI_MAP)));
		BiMap<String, Integer> tripleIDMap = (BiMap<String, Integer>) mapReader.readObject();
		mapReader.close();
		
		System.out.println("done loading ID Map");
		System.out.println("Size of map: " + tripleIDMap.size());
		
		PrintWriter writer
		   = new PrintWriter(new BufferedWriter(new FileWriter(Constants.ADJ_LIST)));
		
		Mongo mongo = new Mongo(Constants.MONGO_LOCAL_HOST, Constants.MONGO_PORT);
		DB db = mongo.getDB(Constants.MONGO_DB);
		DBCollection collection = db.getCollection(Constants.MONGO_TRIPLE_COLLECTION);
		
		Set<Integer> vertexIDs = tripleIDMap.inverse().keySet();
		List<Integer> vertexIDlst = new ArrayList<Integer>(vertexIDs);
		Collections.sort(vertexIDlst);
		int numVertices = vertexIDlst.get(vertexIDlst.size()-1);
		final int numEdges = 54440096;	// lines in the infobox.nt file (find other method to get this)
		BasicDBObject vertexDoc = new BasicDBObject();
		vertexDoc.put("total_vertices", numVertices);
		collection.insert(vertexDoc);
		writer.println(numVertices + " " + numEdges);
		
		BasicDBObject queryDoc = new BasicDBObject();
		BasicDBObject queryFields = new BasicDBObject();
		System.out.println("Computing adjacency list...");
		System.out.println("No of vertices: " + numVertices);
		int vcount = 0;
		double i = 0.1;
		Set<Integer> uniqueIDs = new HashSet<Integer>();
		for(int vid : vertexIDlst) {
			
			// get the out-edges of this vertex
			queryDoc.clear();
			queryFields.clear();
			queryDoc.put("subject", vid);
			queryFields.put("object", 1);
/*			
			List<Integer> adjVertexIDs = collection.distinct("object", queryDoc);
			collection.find(queryDoc, queryFields);
			for(int adjID : adjVertexIDs)
				writer.print(adjID + " ");
*/
			DBCursor resultCursor = collection.find(queryDoc, queryFields);
			while(resultCursor.hasNext()) {
				DBObject resultObj = resultCursor.next();
//				int adjVertexID = (Integer) resultObj.get("object");
				uniqueIDs.add((Integer) resultObj.get("object"));
//				writer.print(adjVertexID + " ");
			}
			
			// get the in-edges of this vertex
			queryDoc.clear();
			queryFields.clear();
			queryDoc.put("object", vid);
			queryFields.put("subject", 1);
			resultCursor = collection.find(queryDoc, queryFields);
			
			while(resultCursor.hasNext()) {
				DBObject resultObj = resultCursor.next();
//				int adjVertexID = (Integer) resultObj.get("subject");
				uniqueIDs.add((Integer) resultObj.get("subject"));
//				writer.print(adjVertexID + " ");
			}
			for(int id : uniqueIDs)
				writer.print(id + " ");
/*			
			adjVertexIDs = collection.distinct("subject", queryDoc);
			for(int adjID : adjVertexIDs)
				writer.print(adjID + " ");
*/
			writer.println();
			uniqueIDs.clear();
			
			vcount++;
			if(vcount >= (numVertices*i))
				System.out.println("Completed " + vcount + " vertices");
			i += 0.1;
		}
		System.out.println("Done");
		writer.flush();
		writer.close();
		mongo.close();
	}
	
	public void loadTriples(String path) throws Exception {
		// read IDs from the saved bi-di-map
		ObjectInputStream mapReader = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream(Constants.SER_BI_MAP)));
		BiMap<String, Integer> tripleIDMap = (BiMap<String, Integer>) mapReader.readObject();
		mapReader.close();
		
		System.out.println("done loading ID Map");
		System.out.println("Size of map: " + tripleIDMap.size());
		
		Mongo mongo = new Mongo(Constants.MONGO_LOCAL_HOST, Constants.MONGO_PORT);
		DB db = mongo.getDB(Constants.MONGO_DB);
		DBCollection collection = db.getCollection(Constants.MONGO_TRIPLE_COLLECTION);
		BasicDBObject document;
		
		File inputData = new File(path);
		FileReader fileReader = new FileReader(inputData);
		BufferedReader tripleReader = new BufferedReader(fileReader);
		
		String triple;
		System.out.println("Reading triples from nt file");
		while((triple = tripleReader.readLine()) != null) {
			String[] spo = triple.split(" ");
			Integer tripleID1 = tripleIDMap.get(spo[0]);
			Integer tripleID2 = tripleIDMap.get(spo[2]);
			
			document = new BasicDBObject();
			document.put("subject", tripleID1.intValue());
			document.put("predicate", spo[1]);
			document.put("object", tripleID2.intValue());
			collection.insert(document);
		}
		System.out.println("All the triples inserted into MongoDB");
		System.out.println("Creating index on subject of the triples");
		
		BasicDBObject index = new BasicDBObject();		
		index.put("subject", 1);
		collection.ensureIndex(index);
		
		tripleReader.close();
		mongo.close();
	}

	public void convertToIDs(String path) throws Exception {
		File inputData = new File(path);
		FileReader fileReader = new FileReader(inputData);
		BufferedReader reader = new BufferedReader(fileReader);
		PrintWriter writer
		   = new PrintWriter(new BufferedWriter(new FileWriter(Constants.EDGE_LIST)));
		
		BiMap<String, Integer> tripleIDMap = HashBiMap.create();
		
		String triple;
		int idCount = 1;
		int tripleCount = 0;
		System.out.println("Started...");
		while((triple = reader.readLine()) != null) {
			String[] spo = triple.split(" ");
			Integer tripleID1 = tripleIDMap.get(spo[0]);
			Integer tripleID2 = tripleIDMap.get(spo[2]);
			if(tripleID1 == null) {
				tripleID1 = idCount;
				tripleIDMap.put(spo[0], tripleID1);
				idCount++;
			}
			if(tripleID2 == null) {
				tripleID2 = idCount;
				tripleIDMap.put(spo[2], tripleID2);
				idCount++;
			}
			writer.println(tripleID1.intValue() + " " + tripleID2.intValue());
			tripleCount++;
			if(tripleCount%100000 == 0)
				System.out.println("processed " + tripleCount + " triples");
		}
		
		// saving bi-directional map to disk
		System.out.println("saving bi-directional map to disk");
		ObjectOutputStream mapWriter = new ObjectOutputStream(new FileOutputStream(Constants.SER_BI_MAP));
		mapWriter.writeObject(tripleIDMap);
		System.out.println("Done");
		
		writer.flush();
		writer.close();
		fileReader.close();
		reader.close();
	}
	
	public static void main(String[] args) throws Exception {
/*
		if(args.length != 1) {
			System.out.println("Provide the triple file (nt format)");
			System.exit(-1);
		}
*/
		
//		new DataConverter().convertToIDs(args[0]);
		
		DataConverter converter = new DataConverter();
		converter.constructAdjList();
//		converter.loadTriples(args[0]);
	}

}
