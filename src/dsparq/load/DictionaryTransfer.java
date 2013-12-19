package dsparq.load;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import dsparq.misc.Constants;
import dsparq.util.Util;

/**
 * This class is used to transfer the dictionary from Redis
 * to MongoDB.
 * 
 * @author Raghava
 *
 */
public class DictionaryTransfer {
	private Mongo mongo;
	private DBCollection idValCollection;
	private final int CHUNK_SIZE = 10000;
	
	public DictionaryTransfer() {
		try {
			mongo = new MongoClient("nimbus5", 10000);
			DB rdfDB = mongo.getDB(Constants.MONGO_RDF_DB);
			idValCollection = rdfDB.getCollection(
									Constants.MONGO_IDVAL_COLLECTION);
		}
		catch(Exception e) {
			if(mongo != null)
				mongo.close();
			e.printStackTrace();
		}
	}
	
	public void transferDictionary() throws Exception {
		Jedis idStore = null;
		try {
//			PropertyFileHandler propertyFileHandler = 
//					PropertyFileHandler.getInstance();
//			String redisHosts = propertyFileHandler.getRedisHosts();
			idStore = new Jedis("localhost", 6379, Constants.INFINITE_TIMEOUT);
			idStore.del("subCount", "typeCount", "propCount");
			long totalKeys = idStore.zcard("keys");
			long currentKeys = 0;
			Pipeline p = idStore.pipelined();
			while(currentKeys < totalKeys) {
				Set<String> keys = 
						idStore.zrange("keys", currentKeys, 
								currentKeys + CHUNK_SIZE);
				transferHelper(keys, p);
				currentKeys += CHUNK_SIZE + 1;
				if(currentKeys % 1000000 == 0)
					System.out.println("Reached " + currentKeys);
				keys.clear();
			}
		}
		finally {
			if(idStore != null)
				idStore.disconnect();
			mongo.close();
		}
	}
	
	private void transferHelper(Set<String> keys, Pipeline p) {
		List<Response<String>> responseList = 
			new ArrayList<Response<String>>(keys.size());
		for(String key : keys) 
			responseList.add(p.get(key));
		p.sync();
		insertIntoMongo(p, responseList, keys);
		responseList.clear();
	}
	
	private void insertIntoMongo(Pipeline p, 
			List<Response<String>> responseList, 
			Set<String> keys) {
		try {
			// insert responseList in MongoDB
			List<DBObject> docList = new ArrayList<DBObject>();
			int rindex = 0;
			for(String key : keys) {
				BasicDBObject doc = new BasicDBObject();
				doc.put(Constants.FIELD_ID, Long.parseLong(
						responseList.get(rindex).get()));
//				doc.put(Constants.FIELD_STR_VALUE, idValMap.get("str"));
				doc.put("_id", key);
				docList.add(doc);
				rindex++;
			}
			idValCollection.insert(docList);
			docList.clear();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("Transferring dictionary from Redis to MongoDB");
		GregorianCalendar start = new GregorianCalendar();
		new DictionaryTransfer().transferDictionary();
		System.out.println("Done");
		double secs = Util.getElapsedTime(start);
		System.out.println("Total Secs: " + secs);
	}
}
