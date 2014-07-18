package dsparq.load;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

import dsparq.misc.Constants;
import dsparq.util.Util;

/**
 * This class generates dummy triples in the form of numerical IDs
 * and inserts them into MongoDB. Not useful in the regular 
 * workflow. Used for stress testing MongoDB.
 * @author Raghava
 *
 */
public class DummyTripleLoader {

	private MongoClient mongoClient;
	private DBCollection collection;
	
	public DummyTripleLoader() {
		try {
			mongoClient = new MongoClient("localhost", 10000);
			mongoClient.setWriteConcern(WriteConcern.NORMAL);
			DB rdfdb = mongoClient.getDB("rdfdb");
			collection = rdfdb.getCollection("starschema");
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void insertDummyTriples() {
		long startTime = System.nanoTime();
		int NUM_STAR_SCHEMA_TRIPLES = 4000000;
		Random random = new Random();
		List<BasicDBObject> predObjList = new ArrayList<BasicDBObject>();
		List<DBObject> docList = new ArrayList<DBObject>();
		System.out.println("Start inserting dummy triples...");
		for(int i = 1; i <= NUM_STAR_SCHEMA_TRIPLES; i++) {
			int subject = random.nextInt();
			for(int j = 1; j <= 10; j++) {
				int predicate = random.nextInt(100);
				int object = random.nextInt(1000);
				BasicDBObject predObjDoc = new BasicDBObject();
				predObjDoc.put(Constants.FIELD_TRIPLE_PREDICATE, 
								new Integer(predicate));
				predObjDoc.put(Constants.FIELD_TRIPLE_OBJECT, 
								new Integer(object));
				predObjList.add(predObjDoc);
			}
			DBObject doc = new BasicDBObject();
			doc.put(Constants.FIELD_TRIPLE_SUBJECT, new Integer(subject));
			doc.put(Constants.FIELD_TRIPLE_PRED_OBJ, predObjList);
			docList.add(doc);
			if(docList.size() >= 5000) {
				collection.insert(docList);
				docList.clear();
			}
			predObjList.clear();
			if(i % 1000000 == 0)
				System.out.println("Inserted " + i + " triples");
		}
		if(!docList.isEmpty())
			collection.insert(docList);
		mongoClient.close();
		Util.getElapsedTime(startTime);
	}
	
	public static void main(String[] args) {
		new DummyTripleLoader().insertDummyTriples();
	}
}
