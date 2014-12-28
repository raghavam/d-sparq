package dsparq.load;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import dsparq.misc.Constants;
import dsparq.misc.PropertyFileHandler;
import dsparq.util.Util;

/**
 * This class takes a set of files containing triples in
 * sub|pred|obj format and loads them to the local DB in 
 * star schema format.
 * 
 * @author Raghava
 *
 */
public class SplitTripleLoader {

	public void loadTriples(String dirPath) throws Exception {
		File inputDir = new File(dirPath);
		File[] allFiles = inputDir.listFiles();
		PropertyFileHandler propertyFileHandler = 
				PropertyFileHandler.getInstance();
		Mongo mongo = new MongoClient("localhost", 
				propertyFileHandler.getShardPort());
		DB rdfDB = mongo.getDB(Constants.MONGO_RDF_DB);
		DBCollection starSchemaCollection = rdfDB.getCollection(
				Constants.MONGO_STAR_SCHEMA);
		//deleting the existing schema
		starSchemaCollection.drop();
		long previousSubjectID = -1;
		long subjectID = -1;
		boolean firstLine = true;
		List<BasicDBObject> predObjList = new ArrayList<BasicDBObject>();
		long startTime = System.nanoTime();
		for(File tripleFile : allFiles) {
			//bulk operation has to be reinitialized after execute()
			BulkWriteOperation bulkInsert = 
					starSchemaCollection.initializeUnorderedBulkOperation();
			BufferedInputStream inputStream = new BufferedInputStream(
					new FileInputStream(tripleFile));
			Scanner scanner = new Scanner(inputStream, "UTF-8");
			firstLine = true;
			while(scanner.hasNext()) {
				String triple = scanner.nextLine();
				String[] tokens = triple.trim().split(Constants.REGEX_DELIMITER);
				subjectID = Long.parseLong(
						tokens[Constants.POSITION_SUBJECT]);
				if(firstLine) {
					firstLine = false;
					previousSubjectID = subjectID;
				}
				else {
					if(subjectID != previousSubjectID) {
						DBObject doc = new BasicDBObject();
						doc.put(Constants.FIELD_TRIPLE_SUBJECT, previousSubjectID);
						doc.put(Constants.FIELD_TRIPLE_PRED_OBJ, predObjList);
						bulkInsert.insert(doc);
						previousSubjectID = subjectID;
						predObjList = new ArrayList<BasicDBObject>();
					}
				}
				BasicDBObject predObj = new BasicDBObject();
				try {
				predObj.put(Constants.FIELD_TRIPLE_PREDICATE, 
						Long.parseLong(tokens[Constants.POSITION_PREDICATE]));
				predObj.put(Constants.FIELD_TRIPLE_OBJECT, 
						Long.parseLong(tokens[Constants.POSITION_OBJECT]));
				}catch(Exception e) {
					System.out.println("Exception: " + e.getMessage());
					System.out.println("Triple: " + triple);
				}
				predObjList.add(predObj);
			}
			DBObject doc = new BasicDBObject();
			doc.put(Constants.FIELD_TRIPLE_SUBJECT, subjectID);
			doc.put(Constants.FIELD_TRIPLE_PRED_OBJ, predObjList);
			bulkInsert.insert(doc);
			bulkInsert.execute();
			inputStream.close();
			scanner.close();
		}
		System.out.println("All triples inserted in (secs): " + 
				Util.getElapsedTime(startTime));
		System.out.println("Creating indexes now...");
		startTime = System.nanoTime();
		DBObject predObjIndex = BasicDBObjectBuilder.start().
				add(Constants.FIELD_TRIPLE_PREDICATE, 1).
				add(Constants.FIELD_TRIPLE_OBJECT, 1).get();
		starSchemaCollection.createIndex(predObjIndex);
		System.out.println("Indexes created in (secs): " + 
				Util.getElapsedTime(startTime));
		mongo.close();
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			throw new Exception("Provide path to directory containing triples");
		}
		new SplitTripleLoader().loadTriples(args[0]);
	}
}
