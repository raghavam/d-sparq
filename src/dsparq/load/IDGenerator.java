package dsparq.load;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

import org.openjena.riot.RiotReader;
import org.openjena.riot.lang.LangNTriples;

import com.hp.hpl.jena.graph.Triple;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import dsparq.misc.Constants;
import dsparq.misc.HostInfo;
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
					"var startNum = Number(statsDoc['" + 
						Constants.TOTAL_DOCS + "']); " +
					"var idValsCollection = db.getCollection('" + 
						Constants.MONGO_IDVAL_COLLECTION + "'); " +
					"var cursor = idValsCollection.find({}, {" + 
						Constants.FIELD_HASH_VALUE + ":1, " + 
						Constants.FIELD_TYPEID + ":1}); " +
					"while(cursor.hasNext()) { " +
						"var doc = cursor.next(); " +
						"var tid = Number(doc['" + Constants.FIELD_TYPEID + "']); " +
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
										": ip}}); " +
							"ip = ip + 1; " +
						"} " +
					"} " +
					"return {totalTerms : startNum} " +	
				"} ";
		String ip = InetAddress.getLocalHost().getHostAddress();
		String[] ipBlocks = ip.split("\\.");
		StringBuilder flattenedIP = new StringBuilder().append(ipBlocks[0]).
				append(ipBlocks[1]).append(ipBlocks[2]).append(ipBlocks[3]);
//		System.out.println("Using IP: " + flattenedIP);
		CommandResult result = localDB.doEval(insertNumIDScript, 
				flattenedIP.toString());
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
