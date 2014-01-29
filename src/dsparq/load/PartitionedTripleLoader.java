package dsparq.load;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import dsparq.misc.Constants;
import dsparq.misc.HostInfo;
import dsparq.util.Util;

/**
 * Takes in the file containing all the vertex IDs
 * which belong to a particular partition and loads
 * all the triples which have the same subject as the 
 * vertices into the local partition
 * 
 * @author raghava
 *
 */
public class PartitionedTripleLoader {

	private Mongo mongo;
	private DBCollection tripleCollection;
	private DBCollection ptripleCollection;
	private DBCollection idValCollection;
	private DBCollection eidValCollection;
	private DBCollection starSchemaCollection;
	private int hostID;
	
	public PartitionedTripleLoader() {
		try {
			// TODO: read the next 2 values from properties file later on
			int mongosCount = 3;
			List<HostInfo> mongosInfo = new ArrayList<HostInfo>(mongosCount);
			mongosInfo.add(new HostInfo("nimbus2", 27017));
			mongosInfo.add(new HostInfo("nimbus8", 27017));
			mongosInfo.add(new HostInfo("nimbus12", 27017));
			String hostName = InetAddress.getLocalHost().getHostName();
			// hostName starts with nimbus. Extracting the number after "nimbus"
			Pattern pattern = Pattern.compile("\\d+");
			Matcher matcher = pattern.matcher(hostName);
			matcher.find();
			hostID = Integer.parseInt(matcher.group());
			mongo = new MongoClient(mongosInfo.get(hostID%mongosCount).getHost(), 
						mongosInfo.get(hostID%mongosCount).getPort());
			Mongo localMongo = new MongoClient("localhost", 10000);
			DB localDB = localMongo.getDB(Constants.MONGO_RDF_DB);
			DB db = mongo.getDB(Constants.MONGO_RDF_DB);
			tripleCollection = db.getCollection(
					Constants.MONGO_TRIPLE_COLLECTION);
			ptripleCollection = localDB.getCollection(
					Constants.MONGO_PARTITIONED_VERTEX_COLLECTION);
			starSchemaCollection = localDB.getCollection(Constants.MONGO_STAR_SCHEMA);
			idValCollection = db.getCollection(Constants.MONGO_IDVAL_COLLECTION);
			eidValCollection = db.getCollection(Constants.MONGO_EIDVAL_COLLECTION);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void loadTriples() throws Exception {
		// since partition starts from nimbus3;
		int partitionID = hostID - 3;
		String fileName = "triples" + partitionID + "-r-0000" + partitionID;
		File inputDataFile = new File(fileName);
		FileReader fileReader = new FileReader(inputDataFile);
		BufferedReader reader = new BufferedReader(fileReader);
		String vertexID;
		List<DBObject> docsToInsert = new ArrayList<DBObject>();
		while((vertexID = reader.readLine()) != null) {
			// query to get all triples whose subject is same as vertexID
			
			// there is a 0 attached to every vertexID
			String[] vertexSplit = vertexID.trim().split("\\s");
			long subjectID = Long.parseLong(vertexSplit[0]);
			DBCursor cursor = tripleCollection.find(new BasicDBObject(
					Constants.FIELD_TRIPLE_SUBJECT, subjectID));
			while(cursor.hasNext()) {
				DBObject resultDoc = cursor.next();
				DBObject doc = new BasicDBObject();
				doc.put(Constants.FIELD_TRIPLE_SUBJECT, subjectID);
				doc.put(Constants.FIELD_TRIPLE_PREDICATE, 
						(Long) resultDoc.get(Constants.FIELD_TRIPLE_PREDICATE));
				doc.put(Constants.FIELD_TRIPLE_OBJECT, 
						(Long) resultDoc.get(Constants.FIELD_TRIPLE_OBJECT));
				docsToInsert.add(doc);
				if(docsToInsert.size() >= 10000) {
					ptripleCollection.insert(docsToInsert);
					docsToInsert.clear();
				}
			}
		}
		if(!docsToInsert.isEmpty())
			ptripleCollection.insert(docsToInsert);
		mongo.close();
	}
	
	public void loadTriplesInStarSchema() throws Exception {
		// since partition starts from nimbus3;
		int partitionID = hostID - 3;
		String fileName = "triples" + partitionID + "-r-0000" + partitionID;
		File inputDataFile = new File(fileName);
		FileReader fileReader = new FileReader(inputDataFile);
		BufferedReader reader = new BufferedReader(fileReader);
		String vertexID;
		List<DBObject> docsToInsert = new ArrayList<DBObject>();
		boolean foundMatch = false;
		BasicDBObject fields = new BasicDBObject();
		fields.put(Constants.FIELD_TRIPLE_PREDICATE, 1);
		fields.put(Constants.FIELD_TRIPLE_OBJECT, 1);
		
		// drop schema if its already present
		starSchemaCollection.drop();
		
		while((vertexID = reader.readLine()) != null) {
			// query to get all triples whose subject is same as vertexID
			
			// there is a 0 attached to every vertexID
			String[] vertexSplit = vertexID.trim().split("\\s");
			long subjectID = Long.parseLong(vertexSplit[0]);
			DBCursor cursor = tripleCollection.find(
					new BasicDBObject(
					Constants.FIELD_TRIPLE_SUBJECT, subjectID), 
					fields);
			List<BasicDBObject> predObjList = new ArrayList<BasicDBObject>();
			while(cursor.hasNext()) {
				foundMatch = true;
				DBObject resultDoc = cursor.next();
				BasicDBObject predObj = new BasicDBObject();
				predObj.put(Constants.FIELD_TRIPLE_PREDICATE, 
						(Long)resultDoc.get(Constants.FIELD_TRIPLE_PREDICATE));
				predObj.put(Constants.FIELD_TRIPLE_OBJECT, 
						(Long)resultDoc.get(Constants.FIELD_TRIPLE_OBJECT));
				predObjList.add(predObj);
			}
			if(foundMatch) {
				DBObject doc = new BasicDBObject();
				doc.put(Constants.FIELD_TRIPLE_SUBJECT, subjectID);
				doc.put(Constants.FIELD_TRIPLE_PRED_OBJ, predObjList);
				docsToInsert.add(doc);
				if(docsToInsert.size() >= 10000) {
					starSchemaCollection.insert(docsToInsert);
					docsToInsert.clear();
				}
				foundMatch = false;
			}
		}
		if(!docsToInsert.isEmpty())
			starSchemaCollection.insert(docsToInsert);
		mongo.close();
	}
	
	public void writeTriples() throws Exception {
		// since partition starts from nimbus3;
		int partitionID = hostID - 3;
		String fileName = "triples" + partitionID + "-r-0000" + partitionID;
		File inputDataFile = new File(fileName);
		FileReader fileReader = new FileReader(inputDataFile);
		BufferedReader reader = new BufferedReader(fileReader);
		PrintWriter writer = new PrintWriter(new BufferedWriter(
						new FileWriter(new File(fileName + ".nt"))));
		String vertexID;
		long lineCount = 0;
		final String DUMMY_PREFIX = "http://num.example.org#";
		while((vertexID = reader.readLine()) != null) {
			// query to get all triples whose subject is same as vertexID
			
			// there is a 0 attached to every vertexID
			String[] vertexSplit = vertexID.trim().split("\\s");
			long subjectID = Long.parseLong(vertexSplit[0]);
			DBCursor cursor = tripleCollection.find(new BasicDBObject(
					Constants.FIELD_TRIPLE_SUBJECT, subjectID));
			
			// it is too slow to collect all the string equivalents
			// So, using a dummy string and retain the IDs
			String subStr = DUMMY_PREFIX + subjectID;
			while(cursor.hasNext()) {
				DBObject resultDoc = cursor.next();
				String predStr = DUMMY_PREFIX + 
						(Long)resultDoc.get(Constants.FIELD_TRIPLE_PREDICATE);
				String objStr = DUMMY_PREFIX +
						(Long)resultDoc.get(Constants.FIELD_TRIPLE_OBJECT);				
				StringBuilder triple = new StringBuilder();
				triple.append("<").append(subStr).append("> <").
					append(predStr).append("> ");
				
				//check whether the Object is a literal or not, 
				if(objStr.startsWith("http")) {
					// this is not a literal - enclose it in < >
					triple.append("<").append(objStr).append("> .");
				}
				else {
					// this is a literal
					triple.append(objStr).append(" .");
				}
				writer.println(triple.toString());
			}
			lineCount++;
			if((lineCount % 10000) == 0)
				System.out.println("Reached " + lineCount);
		}
		writer.close();
		mongo.close();
	}
	
	public void createIndexes() {
		ptripleCollection.ensureIndex(new BasicDBObject(
							Constants.FIELD_TRIPLE_SUBJECT, 1));
//		ptripleCollection.ensureIndex(new BasicDBObject(
//				Constants.FIELD_TRIPLE_PREDICATE, 1));
//		ptripleCollection.ensureIndex(new BasicDBObject(
//				Constants.FIELD_TRIPLE_OBJECT, 1));
		// compound index on predicate & object
		DBObject predObjIndex = BasicDBObjectBuilder.start().
							add(Constants.FIELD_TRIPLE_PREDICATE, 1).
							add(Constants.FIELD_TRIPLE_OBJECT, 1).get();
		ptripleCollection.ensureIndex(predObjIndex);
		mongo.close();
	}
	
	public static void main(String[] args) throws Exception {
		GregorianCalendar start = new GregorianCalendar();
		new PartitionedTripleLoader().loadTriplesInStarSchema();
		System.out.println("Done");
		Util.getElapsedTime(start);
	}
}
