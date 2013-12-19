package dsparq.load;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import dsparq.misc.Constants;
import dsparq.misc.HostInfo;

public class AdjListGenerator {

	private Mongo mongo;
	private DBCollection tripleCollection;
	private PrintWriter adjVertexWriter;
	private PrintWriter vertexWriter;
	private long startID;
	private long endID;
	
	public AdjListGenerator() {
		try {
			// TODO: read the next 2 values from properties file later on
			int mongosCount = 3;
			List<HostInfo> mongosInfo = new ArrayList<HostInfo>(mongosCount);
			mongosInfo.add(new HostInfo("nimbus2", 27017));
			mongosInfo.add(new HostInfo("nimbus8", 27017));
			mongosInfo.add(new HostInfo("nimbus12", 27017));
			String hostName = InetAddress.getLocalHost().getHostName();
			// hostName starts with nimbus. Extracting the number after "nimbus"
			Pattern pattern = Pattern.compile("\\d+");
			Matcher matcher = pattern.matcher(hostName);
			matcher.find();
			int hostID = Integer.parseInt(matcher.group());
			mongo = new Mongo(mongosInfo.get(hostID%mongosCount).getHost(), 
						mongosInfo.get(hostID%mongosCount).getPort());
			DB db = mongo.getDB(Constants.MONGO_RDF_DB);
			tripleCollection = db.getCollection(
								Constants.MONGO_TRIPLE_COLLECTION);
			adjVertexWriter = new PrintWriter(new BufferedWriter(
					new FileWriter("adj-vertex" + hostID)));
			vertexWriter = new PrintWriter(new BufferedWriter(
					new FileWriter("vertex" + hostID)));
			
			// there are 52,775,204 IDs in total and 10 shards.
			// so each shard can take around 5M IDs
			final int ID_COUNT = 5000000;
			// 3 is subtracted because first shard is at nimbus3
			startID = (hostID-3) * ID_COUNT + 1;
			endID = startID + ID_COUNT - 1;
			if(hostID == 12) {
				// put the last ID into it
				endID = 52775204;
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public void generateAdjListFile() {
		// read from startID till endID and write the triples in integer
		// form to a file
		Set<Long> vertices = new HashSet<Long>();
		long totalEdges = 0;
		for(long i = startID; i <= endID; i++) {
			DBCursor cursor = tripleCollection.find(
						new BasicDBObject(Constants.FIELD_TRIPLE_SUBJECT, i));
			vertexWriter.println(i);
			if(cursor.count() == 0) 
				adjVertexWriter.println();
			else {
				while(cursor.hasNext()) {
					DBObject doc = cursor.next();
					long predicate = (Long) doc.get(Constants.FIELD_TRIPLE_PREDICATE);
					if(predicate == 2)
						continue;
					long object = (Long) doc.get(Constants.FIELD_TRIPLE_OBJECT);
					if(!vertices.contains(object)) {
						adjVertexWriter.print(object + " ");
						vertices.add(object);
					}
				}
				adjVertexWriter.println();
				totalEdges += vertices.size();				
				vertices.clear();
			}
			System.out.println("Done with ID: " + i);
		}
		System.out.println("Total edges: " + totalEdges + "  half-edges: " + totalEdges/2);
		mongo.close();
		adjVertexWriter.close();
		vertexWriter.close();
	}
	
	
	public static void main(String[] args) {
		new AdjListGenerator().generateAdjListFile();
	}

}
