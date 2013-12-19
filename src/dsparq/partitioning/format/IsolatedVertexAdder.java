package dsparq.partitioning.format;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * Adds missing vertices as isolated vertices for the sake of METIS.
 * keeps track of these newly added vertices by putting them in Redis.
 * 
 * @author Raghava
 *
 */
public class IsolatedVertexAdder {

	public void addIsolatedVertices(String vpath, String adjPath) throws Exception {
		File inputData1, inputData2;
		FileReader fileReader1, fileReader2;
		BufferedReader vertexIDReader = null, adjListReader = null;
		PrintWriter writer1 = null;
		PrintWriter writer2 = null;
		Jedis idStore = null;
		Jedis isolatedVertexStore = null;
		
		try {
			idStore = new Jedis("nimbus2", 6479);
			isolatedVertexStore = new Jedis("nimbus3", 6479);
			isolatedVertexStore.select(1);
			Pipeline p = isolatedVertexStore.pipelined();
			
			inputData1 = new File(vpath);
			fileReader1 = new FileReader(inputData1);
			vertexIDReader = new BufferedReader(fileReader1);	
			
			inputData2 = new File(adjPath);
			fileReader2 = new FileReader(inputData2);
			adjListReader = new BufferedReader(fileReader2);	
			
			writer1 = new PrintWriter(new BufferedWriter(
					new FileWriter(vpath + "-new")));
			writer2 = new PrintWriter(new BufferedWriter(
					new FileWriter(adjPath + "-new")));
			String vertexID;
			String adjList;
			long seqVertexID = 1;
			long MAX_VERTEX_ID = Long.parseLong(idStore.get("subCount"));
			double progress = 0.0001;
			
			while(seqVertexID <= MAX_VERTEX_ID) {
				vertexID = vertexIDReader.readLine();
				adjList = adjListReader.readLine();
				
				if((vertexID != null) && (adjList != null)) {
					vertexID = vertexID.trim();
					long vertexIDL = Long.parseLong(vertexID);
					while(seqVertexID < vertexIDL) {
						String seqVertexIDStr = Long.toString(seqVertexID);
						writer1.println(seqVertexIDStr);
						writer2.println();
						p.sadd("isv", seqVertexIDStr);
						seqVertexID++;
					}
					writer1.println(vertexID);
					writer2.println(adjList);
					seqVertexID++;
					p.sync();
				}
				else {
					// reached end-of-file, so keep writing until MAX.
					String seqVertexIDStr = Long.toString(seqVertexID);
					writer1.println(seqVertexIDStr);
					writer2.println();
					p.sadd("isv", seqVertexIDStr);
					seqVertexID++;
				}
				if(seqVertexID == (long)(MAX_VERTEX_ID*progress)) {
					p.sync();
					System.out.println("Reached " + seqVertexID);
					progress += 0.0001;
				}
			}
			p.sync();
			System.out.println("done writing to new vertex & adjList files");
		}
		finally {
			if(writer1 != null)
				writer1.close();
			if(writer2 != null)
				writer2.close();
			if(vertexIDReader != null)
				vertexIDReader.close();
			if(adjListReader != null)
				adjListReader.close();
			if(idStore != null)
				idStore.disconnect();
			if(isolatedVertexStore != null)
				isolatedVertexStore.disconnect();
		}
	}
	
	//TODO: 1) add isolated vertices to Redis
 	
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			throw new Exception("provide vertexID file and " +
					"adjList file in that order");
		}
		new IsolatedVertexAdder().addIsolatedVertices(args[0], args[1]);
	}
}
