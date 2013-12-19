package dsparq.load;

import java.util.GregorianCalendar;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import dsparq.misc.Constants;
import dsparq.util.Util;

public class IndexCreator {

	public void createPredicateObjectIndex() throws Exception {
		//creates compound index on Predicate-Object in 'starschema' collection
		Mongo mongo = null;
		try {
			mongo = new MongoClient("nimbus5", 10000);
			DB mongoDB = mongo.getDB(Constants.MONGO_RDF_DB);
			DBCollection starSchemaCollection = 
					mongoDB.getCollection(Constants.MONGO_STAR_SCHEMA);
			DBObject index = new BasicDBObject();
			index.put("predobj.predicate", 1);
			index.put("predobj.object", 1);
			starSchemaCollection.ensureIndex(index);
		}
		finally {
			if(mongo != null)
				mongo.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("Creating index...");
		GregorianCalendar start = new GregorianCalendar();
		new IndexCreator().createPredicateObjectIndex();
		System.out.println("Index creation time, secs: " + 
					Util.getElapsedTime(start));
		System.out.println("Done");
	}
}
