package dsparq.load;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import dsparq.misc.Constants;
import dsparq.misc.HostInfo;
import dsparq.misc.PropertyFileHandler;
import dsparq.util.Util;

/**
 * Takes a set of files where each line is of the form
 * HashDigest|TypeID|StringValue where
 * HashDigest is the hash of subject/predicate/object. TypeID is
 * -1 for object that follows rdf:type predicate. It is 1 for 
 * everything else. -1 is required for Metis (after removing "type" triples).
 * 
 * @author Raghava
 */
public class HashDigestLoader {

	public void loadTripleIDsIntoDB(File[] files) {
		Mongo mongo = null;
		PropertyFileHandler propertyFileHandler = 
				PropertyFileHandler.getInstance();
		HostInfo hostInfo = propertyFileHandler.getMongoRouterHostInfo();
		try {
			mongo = new MongoClient(hostInfo.getHost(), hostInfo.getPort());
			DB db = mongo.getDB(Constants.MONGO_RDF_DB);
			DBCollection idValCollection = db.getCollection(
					Constants.MONGO_IDVAL_COLLECTION);
			BulkWriteOperation bulkInsert = 
					idValCollection.initializeUnorderedBulkOperation();
			String line;
			for(File file : files) {
				FileReader fileReader = new FileReader(file);
				BufferedReader bufferedReader = new BufferedReader(fileReader);
				while((line = bufferedReader.readLine()) != null) {
					String[] splits = line.split(Constants.REGEX_DELIMITER);
					BasicDBObject doc = new BasicDBObject();
					doc.put(Constants.FIELD_HASH_VALUE, splits[0]);
					doc.put(Constants.FIELD_TYPEID, Long.parseLong(splits[1]));
					
					//not saving the string values here since they take lot of 
					//space. Convert queries to numerical equivalents and compare.
//					doc.put(Constants.FIELD_STR_VALUE, splits[2]);
					
					bulkInsert.insert(doc);
				}
				bulkInsert.execute();
				bufferedReader.close();
				fileReader.close();
				System.out.println("Done with " + file.getName());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(mongo != null)
				mongo.close();
		}
	}
	
	/**
	 * counts the types i.e., rdf:type triples. This function is used
	 * just for verification.
	 * @param files
	 */
	public void countTypes(File[] files) {
		try {
			String line;
			int count = 0;
			for(File file : files) {
				FileReader fileReader = new FileReader(file);
				BufferedReader bufferedReader = new BufferedReader(fileReader);
				while((line = bufferedReader.readLine()) != null) {
					String[] splits = line.split(Constants.REGEX_DELIMITER);
					long typeID = Long.parseLong(splits[1]);
					if(typeID == -1) {
						System.out.println(line);
						count++;
					}
				}
				bufferedReader.close();
				fileReader.close();
				System.out.println("Done with " + file.getName());
				System.out.println("Count of types: " + count);
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		if(args.length != 1) {
			System.out.println("Give the path to directory containing files");
			System.exit(-1);
		}
		File dir = new File(args[0]);
		if(!dir.isDirectory()) {
			System.out.println("This is not a directory");
			System.exit(-1);
		}
		long startTime = System.nanoTime();
		new HashDigestLoader().loadTripleIDsIntoDB(dir.listFiles());
//		new HashDigestLoader().countTypes(dir.listFiles());
		System.out.println("Time taken (secs): " + Util.getElapsedTime(startTime));
	}

}
