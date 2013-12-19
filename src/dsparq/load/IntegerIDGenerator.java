package dsparq.load;

import java.util.GregorianCalendar;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import dsparq.misc.Constants;
import dsparq.util.Util;

public class IntegerIDGenerator {

	public void generateIDs() throws Exception {
		Mongo mongo = new Mongo("localhost", 27017);
		DB db = mongo.getDB(Constants.MONGO_RDF_DB);
		DBCollection idValCollection = db.getCollection(
							Constants.MONGO_IDVAL_COLLECTION);
		DBCursor cursor = idValCollection.find();
		long id = 1;
		long typeID = -1;
		int totalDocs = cursor.count();
		System.out.println("Total docs: " + totalDocs);
		long progressCount = 0;
		double multiplier = 1;
		GregorianCalendar start = new GregorianCalendar();
		while(cursor.hasNext()) {
			DBObject doc = cursor.next();
			String hashValue = (String) doc.get(Constants.FIELD_HASH_VALUE);
			Object isTypeObj = doc.get("isType");	
			boolean isType = false;
			if(isTypeObj instanceof Boolean)
				isType = ((Boolean)isTypeObj).booleanValue();
			else if(isTypeObj instanceof String)
				isType = Boolean.parseBoolean((String)isTypeObj);
			else
				throw new Exception("Unexpected isType value");
		
			if(!isType) {			
				idValCollection.update(
						new BasicDBObject(Constants.FIELD_HASH_VALUE, hashValue), 
						new BasicDBObject("$set", 
								new BasicDBObject(Constants.FIELD_ID, id)));
				id++;			
			}
			else {				
				idValCollection.update(
						new BasicDBObject(Constants.FIELD_HASH_VALUE, hashValue), 
						new BasicDBObject("$set", 
								new BasicDBObject(Constants.FIELD_ID, typeID)));
				typeID--;
				
			}
			progressCount++;
			if(progressCount >= (totalDocs*0.1*multiplier)) {
				System.out.println("Done with " + progressCount);
				multiplier++;
			}									
		}	
		mongo.close();
		Util.getElapsedTime(start);
	}
	
	public void generateIDsForProps() throws Exception {
		Mongo mongo = new Mongo("localhost", 27017);
		DB db = mongo.getDB(Constants.MONGO_RDF_DB);
		DBCollection eidValCollection = db.getCollection(
							Constants.MONGO_EIDVAL_COLLECTION);
		DBCursor cursor = eidValCollection.find();
		long id = Constants.START_PREDICATE_ID;
		int totalDocs = cursor.count();
		System.out.println("Total docs: " + totalDocs);
		long progressCount = 0;
		double multiplier = 1;
		GregorianCalendar start = new GregorianCalendar();
		while(cursor.hasNext()) {
			DBObject doc = cursor.next();
			String hashValue = (String) doc.get(Constants.FIELD_HASH_VALUE);
			String strValue = (String) doc.get(Constants.FIELD_STR_VALUE);
					
			if(strValue.equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") ||
					strValue.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") ||
					strValue.equals("rdf:type")) {
				eidValCollection.update(
						new BasicDBObject(Constants.FIELD_HASH_VALUE, hashValue), 
						new BasicDBObject("$set", 
								new BasicDBObject(Constants.FIELD_ID, 
										Constants.PREDICATE_TYPE_LONG)));
			}
			else {				
				eidValCollection.update(
						new BasicDBObject(Constants.FIELD_HASH_VALUE, hashValue), 
						new BasicDBObject("$set", 
								new BasicDBObject(Constants.FIELD_ID, id)));
				id++;			
			}
			progressCount++;
			if(progressCount >= (totalDocs*0.1*multiplier)) {
				System.out.println("Done with " + progressCount);
				multiplier++;
			}									
		}	
		mongo.close();
		Util.getElapsedTime(start);
	} 
	
	public static void main(String[] args) throws Exception {
		new IntegerIDGenerator().generateIDsForProps();
	}

}
