package dsparq.load;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import dsparq.misc.Constants;
import dsparq.util.Util;

/**
 * Generates the count of the number of documents in which each
 * predicate participates in. This is done only for predicate because
 * for many queries in the test set (SP2), predicate is a constant whereas
 * subject/object are variables. Another reason is to save space (RAM).
 * 
 * @author Raghava
 *
 */
public class PredicateSelectivity {

	private Mongo mongo;
	
	public PredicateSelectivity() {
		try {
			mongo = new MongoClient("nimbus2", 10000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void generatePredicateSelectivity() {
		try {
			DB db = mongo.getDB(Constants.MONGO_RDF_DB);
			DBCollection starSchemaCollection = db.getCollection(
					Constants.MONGO_STAR_SCHEMA);
			DBCollection predicateSelectivityCollection = db.getCollection(
					Constants.MONGO_PREDICATE_SELECTIVITY);
			List<Long> predList = (List<Long>)starSchemaCollection.distinct(
					"predobj.predicate");
			List<DBObject> selectivityList = 
					new ArrayList<DBObject>(predList.size());
			for(Long predID : predList) {
				DBObject predDoc = new BasicDBObject("predobj.predicate", predID);
				int selectivity = starSchemaCollection.find(predDoc).count();
				DBObject selectivityDoc = new BasicDBObject();
				selectivityDoc.put("_id", predID);
				selectivityDoc.put(Constants.FIELD_PRED_SELECTIVITY, selectivity);
				selectivityList.add(selectivityDoc);
			}
			predicateSelectivityCollection.insert(selectivityList);
		}
		finally {
			if(mongo != null)
				mongo.close();
		}
	}
	
	public static void main(String[] args) {
		System.out.println("Generating predicate selectivity...");
		GregorianCalendar start = new GregorianCalendar();
		new PredicateSelectivity().generatePredicateSelectivity();
		Util.getElapsedTime(start);
		System.out.println("Done");
	}
}
