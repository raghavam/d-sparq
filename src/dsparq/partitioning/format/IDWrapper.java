package dsparq.partitioning.format;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;


import dsparq.misc.Constants;
import dsparq.util.LRUCache;


/**
 * After removing "rdf:type" triples, there would be lot of 
 * isolated vertices which Metis finds hard to deal with. Metis
 * cannot handle the condition #vertices > #edges. So this class
 * wraps the existing IDs with sequential IDs.
 * 
 * @author Raghava
 *
 */
public class IDWrapper {

	private BufferedReader vertexIDReader; 
	private BufferedReader adjListReader;
	private PrintWriter writer1;
//	private PrintWriter writer2;
	private ShardedJedis shardedJedis;
	private LRUCache<String, String> cache;
	private long seqVertexID = 1;
	
	public IDWrapper(String vpath, String adjPath) throws Exception {
		cache = new LRUCache<String, String>(100000);
		File inputData1, inputData2;
		FileReader fileReader1, fileReader2;
		inputData1 = new File(vpath);
		try {
		fileReader1 = new FileReader(inputData1);
		vertexIDReader = new BufferedReader(fileReader1);	
		
//		inputData2 = new File(adjPath);
//		fileReader2 = new FileReader(inputData2);
//		adjListReader = new BufferedReader(fileReader2);
		
		writer1 = new PrintWriter(new BufferedWriter(
				new FileWriter(vpath + "-wrap")));
//		writer2 = new PrintWriter(new BufferedWriter(
//				new FileWriter(adjPath + "-wrap")));
		
		List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
		shards.add(new JedisShardInfo("nimbus5", 6479, Constants.INFINITE_TIMEOUT));
		shards.add(new JedisShardInfo("nimbus6", 6479, Constants.INFINITE_TIMEOUT));
		shards.add(new JedisShardInfo("nimbus7", 6479, Constants.INFINITE_TIMEOUT));
		shardedJedis = new ShardedJedis(shards);
		}
		catch(Exception e) {
			if(writer1 != null)
				writer1.close();
//			if(writer2 != null)
//				writer2.close();
			if(vertexIDReader != null)
				vertexIDReader.close();
			if(adjListReader != null)
				adjListReader.close();
			if(shardedJedis != null)
				shardedJedis.disconnect();
			e.printStackTrace();
		}
	}
	
	public void wrapIDs() throws Exception {
		String vertexID;
		String[] adjVertices;
		long lineCount = 0;
		try {
			
			while((vertexID = vertexIDReader.readLine()) != null) {
				vertexID = vertexID.trim();
				String vid = cache.get(vertexID);
				if(vid == null) 
					vid = getWrappedVertexID(vertexID, false);
				// write to file
				writer1.println(vid);
				lineCount++;
				if(lineCount % 1000000 == 0)
					System.out.println("Processed " + lineCount + " lines");
			}
			System.out.println("\nDone with phase-1");
			lineCount = 0;
/*			
			while((vertexID = adjListReader.readLine()) != null) {
				adjVertices = vertexID.split("\\s");
				
				//TODO: only vertices should be wrapped first, since they
				// should be sequential. What/can if adjlist vertex points to 
				// something not in the current list of vertices??
				// should search for Dist/Streaming partitioner??
				
				
				
				StringBuilder adjList = new StringBuilder();
				String vid;
				for(String adjVID : adjVertices) {
					adjVID = adjVID.trim();
					vid = cache.get(adjVID);
					if(vid == null)
						vid = getWrappedVertexID(adjVID, true);
					adjList.append(vid).append(" ");
				}
				// write to file
				adjList.deleteCharAt(adjList.length()-1);
				writer2.println(adjList);
				
				lineCount++;
				if(lineCount % 1000000 == 0)
					System.out.println("Processed " + lineCount + " lines");
			}
			System.out.println("Done with phase-2");
*/			
		}
		finally {
			cache.clear();
			if(writer1 != null)
				writer1.close();
//			if(writer2 != null)
//				writer2.close();
			if(vertexIDReader != null)
				vertexIDReader.close();
			if(adjListReader != null)
				adjListReader.close();
			shardedJedis.disconnect();
		}
	}
	
	private String getWrappedVertexID(String vertexID, boolean isAdjVertex) 
		throws Exception{
		String vid = shardedJedis.get(vertexID);
		if(vid == null) {
			if(isAdjVertex) {
				System.out.println("For vertexID: " + vertexID);
				throw new Exception("This is a new vertex");
			}
			
			vid = Long.toString(seqVertexID);
			cache.put(vertexID, vid);
			shardedJedis.set(vertexID, vid);
			// holds the reverse as well
			shardedJedis.set(vid + 'r', vertexID);
			seqVertexID++;
		}
		return vid;
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("Provide vertex and adjacent vertex files");
			System.exit(-1);
		}
		new IDWrapper(args[0], args[1]).wrapIDs();
		System.out.println("Done");
	}

}
