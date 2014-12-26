package dsparq.load;

import java.net.InetAddress;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import dsparq.misc.Constants;
import dsparq.misc.PropertyFileHandler;
import dsparq.util.Util;

/**
 * Generates numeric ID and adds it to DB. Numerical IDs are
 * required for Metis (vertex IDs) and serve no other purpose.
 * 
 * @author Raghava
 */
public class IDGenerator {
	
	public void generateIDs() throws Exception {
		PropertyFileHandler propertyFileHandler = 
				PropertyFileHandler.getInstance();
		Mongo mongo;
		mongo = new MongoClient("localhost", 
				propertyFileHandler.getShardPort());
		DB localDB = mongo.getDB(Constants.MONGO_RDF_DB);
		
		/* In general, using a script is considered a bad choice since it 
		 * blocks the server. But this was done to avoid round trips i.e.,
		 * fetch data to the client side, make changes and push it back to
		 * the server. Moreover, this is a one time task. For each document, 
		 * if the typeID is 1 (not an rdf type), a sequential number is 
		 * assigned else some dummy number is assigned which doesn't break 
		 * the sequence.
		*/ 
		
		String insertNumIDScript = 
				"function(ip) { " +
					"var statsCollection = db.getCollection('" + 
						Constants.MONGO_STATS_COLLECTION + "'); " +
					"var statsDoc = statsCollection.findOne(); " + //there is only 1 document
					"var startNum = statsDoc['" + 
						Constants.TOTAL_DOCS + "']; " +
					"var idValsCollection = db.getCollection('" + 
						Constants.MONGO_IDVAL_COLLECTION + "'); " +
					"var cursor = idValsCollection.find({}, {" + 
						Constants.FIELD_HASH_VALUE + ":1, " + 
						Constants.FIELD_TYPEID + ":1}); " +
					"var ipNum = parseInt(ip); " +
					"var i = 0; " +
					"while(cursor.hasNext()) { " +
						"var doc = cursor.next(); " +
						"i = i + 1; " +
						"var tid = doc['" + Constants.FIELD_TYPEID + "']; " +
						"if(tid == 1) { " +
							"idValsCollection.update({" + 
								Constants.FIELD_HASH_VALUE + ": doc['" + 
								Constants.FIELD_HASH_VALUE + "']}, " +
										"{$set : {" + Constants.FIELD_NUMID + 
										": startNum}}); " +
							"startNum = startNum + 1; " +
						"} " +
						"else if(tid == -1) { " +
							"idValsCollection.update({" + 
								Constants.FIELD_HASH_VALUE + ": doc['" + 
								Constants.FIELD_HASH_VALUE + "']}, " +
										"{$set : {" + Constants.FIELD_NUMID + 
										": ipNum}}); " +
							"ipNum = ipNum + 1; " +
						"} " +
					"} " +
					"return {totalTerms : startNum, totalDocs : i}; " +	
				"} ";
		String ip = InetAddress.getLocalHost().getHostAddress();
		String[] ipBlocks = ip.split("\\.");
		StringBuilder flattenedIP = new StringBuilder().append(ipBlocks[0]).
				append(ipBlocks[1]).append(ipBlocks[2]).append(ipBlocks[3]);
//		System.out.println("Using IP: " + flattenedIP);
//		System.out.println("Script: " + insertNumIDScript);
		CommandResult result = localDB.doEval(insertNumIDScript, 
				flattenedIP.toString());
		System.out.println("Result: " + result.toString());
		BasicDBObject resultDoc = (BasicDBObject) result.get("retval");
		int totalTerms = resultDoc.getInt("totalTerms");
		int totalDocs = resultDoc.getInt("totalDocs");
		System.out.println("Total docs: " + totalDocs + 
				"  Assigned numerical IDs: " + totalTerms);
		if(totalTerms > totalDocs)
			throw new Exception("Something went wrong... terms > docs");
		mongo.close();
	}
	
	
	public static void main(String[] args) throws Exception {
		long startTime = System.nanoTime();
		new IDGenerator().generateIDs();
		System.out.println("Time taken (secs): " + 
				Util.getElapsedTime(startTime));
	}
}
