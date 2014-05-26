package dsparq.load;

import java.net.InetAddress;
import java.util.GregorianCalendar;

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
//		DBCollection idValCollection = db.getCollection(
//				Constants.MONGO_IDVAL_COLLECTION);
//		DBCollection statsCollection = db.getCollection(
//				Constants.MONGO_STATS_COLLECTION);
//		DBObject statsDoc = statsCollection.findOne();
//		long count = (Long) statsDoc.get(Constants.TOTAL_DOCS);
		
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
					"while(cursor.hasNext()) { " +
						"var doc = cursor.next(); " +
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
					"return {totalTerms : startNum}; " +	
				"} ";
		String ip = InetAddress.getLocalHost().getHostAddress();
		String[] ipBlocks = ip.split("\\.");
		StringBuilder flattenedIP = new StringBuilder().append(ipBlocks[0]).
				append(ipBlocks[1]).append(ipBlocks[2]).append(ipBlocks[3]);
		System.out.println("Using IP: " + flattenedIP);
		System.out.println("Script: " + insertNumIDScript);
		CommandResult result = localDB.doEval(insertNumIDScript, 
				flattenedIP.toString());
		System.out.println("Result: " + result.toString());
		BasicDBObject resultDoc = (BasicDBObject) result.get("retval");
		System.out.println("Total terms without types: " + 
				(resultDoc.getInt("totalTerms")-1));
		mongo.close();
	}
	
	
	public static void main(String[] args) throws Exception {
		GregorianCalendar start = new GregorianCalendar();
		new IDGenerator().generateIDs();
		System.out.println("Time taken (secs): " + Util.getElapsedTime(start));
	}
}
