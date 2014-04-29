package dsparq.load;

import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import dsparq.misc.Constants;
import dsparq.misc.HostInfo;
import dsparq.misc.PropertyFileHandler;


/**
 * Purpose of this class is to go through the IDVal collection
 * in each shard and figure out the total number of IDs present.
 * This information is later used to assign numerical IDs to each
 * term (subject/predicate/object).
 * 
 * @author Raghava
 */
public class NumericIDPreprocessor {

	public void countTotalIDs() {
		PropertyFileHandler propertyFileHandler = 
				PropertyFileHandler.getInstance();
		List<HostInfo> shardsInfo = propertyFileHandler.getAllShardsInfo();
		try {
			for(HostInfo hostInfo : shardsInfo) {
				Mongo mongoShard = new MongoClient(hostInfo.getHost(), 
						hostInfo.getPort());
				DB db = mongoShard.getDB(Constants.MONGO_RDF_DB);
				DBCollection idValCollection = db.getCollection(
						Constants.MONGO_IDVAL_COLLECTION);
				DBCollection statsCollection = db.getCollection(
						Constants.MONGO_STATS_COLLECTION);
				long totalDocs = idValCollection.count();
				BasicDBObject doc = new BasicDBObject();
				doc.put(Constants.TOTAL_DOCS, totalDocs);
				statsCollection.insert(doc);
				mongoShard.close();
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		new NumericIDPreprocessor().countTotalIDs();
	}

}
