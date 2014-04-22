package dsparq.load;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.openjena.riot.RiotReader;
import org.openjena.riot.lang.LangNTriples;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_Literal;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.util.FmtUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import dsparq.misc.Constants;
import dsparq.misc.HostInfo;
import dsparq.misc.PropertyFileHandler;
import dsparq.util.Util;


/**
 * The purpose of this class is to read the IDs of
 * subjects and objects written in a file and write
 * them to MongoDB. It can be scaled out easily later 
 * on, if MongoDB is used.
 * 
 * @author Raghava Mutharaju
 *
 */
public class IDLoader {
	
	private Mongo mongo;
	
	/**
	 * method to read triples from the nt file and convert
	 * subject/predicate/object into long IDs. The IDs are 
	 * saved to MongoDB. Each document has two fields: value
	 * and id. value holds the subject/predicate/object.
	 * 
	 * Triples in ID form are also saved to DB, so that later
	 * on during triple placement, these can be made use of.
	 * 
	 * @param dirPath - directory path containing nt triple files
	 * @throws Exception
	 */
	public void saveIDsToDB(String dirPath) throws Exception {
		
		initializeShards();
		DB db = mongo.getDB(Constants.MONGO_RDF_DB);
		DBCollection idValCollection = db.getCollection(
											Constants.MONGO_IDVAL_COLLECTION);
		DBCollection eidValCollection = db.getCollection(
											Constants.MONGO_EIDVAL_COLLECTION);
		DBCollection tripleCollection = db.getCollection(Constants.MONGO_TRIPLE_COLLECTION);
		
		File ntFolder = new File(dirPath);
		File[] allfiles = ntFolder.listFiles();
		List<File> ntfiles = new ArrayList<File>();
		for(File f : allfiles)
			if(f.getName().endsWith(".nt"))
				ntfiles.add(f);
		
		FileInputStream in = null;
		PrintWriter writer
		   = new PrintWriter(new BufferedWriter(new FileWriter(Constants.KV_FORMAT_FILE)));
		Triple triple;
		long idCount = Constants.START_ID;
		long predCount = Constants.START_PREDICATE_ID;
		long prevID;
		long insertedID;
		long typeCount = -1;
		boolean isType = false;
		DBObject tripleDocument;
		long tripleCount = 0;
		boolean indexOnce = true;
		
		for(File ntfile : ntfiles) {
			in = new FileInputStream(ntfile);
			LangNTriples ntriples = RiotReader.createParserNTriples(in, null);
			
			System.out.println("\nmapping triples to IDs...");
			while(ntriples.hasNext()) {
				triple = ntriples.next();
				tripleDocument = new BasicDBObject();
				StringBuilder kvLine = new StringBuilder();
				
				// find if the id is already in the db
				prevID = idCount;
				String subject = triple.getSubject().toString();
				insertedID = checkAndInsertID(subject, prevID, idValCollection);
				if(insertedID == prevID)
					idCount++;
				tripleDocument.put(Constants.FIELD_TRIPLE_SUBJECT, insertedID);
				kvLine.append(insertedID).append(Constants.OUTPUT_DELIMITER);
				
				// check if predicate is "rdf:type"
				String predicate = triple.getPredicate().toString();
				if(predicate.equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") ||
				   predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") ||
				   predicate.equals("rdf:type")) {
					prevID = Long.parseLong(Constants.PREDICATE_TYPE);
					isType = true;
				}
				else 
					prevID = predCount;
				insertedID = checkAndInsertID(predicate, prevID, eidValCollection);
				 if((insertedID == prevID) && 
						 (prevID != Long.parseLong(Constants.PREDICATE_TYPE)))
					predCount++;
				tripleDocument.put(Constants.FIELD_TRIPLE_PREDICATE, insertedID);
				kvLine.append(insertedID).append(Constants.OUTPUT_DELIMITER);
				
				// this was required because METIS requires IDs to be in a 
				// sequence and by removing type triples, there were holes in
				// the sequence of IDs.
				if(isType) {
					prevID = typeCount;
					typeCount--;
					isType = false;
				}
				else
					prevID = idCount;
				Node object = triple.getObject();
				boolean isLiteral = object.isLiteral();
				String objectStr;
	
				objectStr = object.toString();
				insertedID = checkAndInsertID(objectStr, prevID, idValCollection);
				if((insertedID == prevID) && (prevID >= 0))
					idCount++;		
				tripleDocument.put(Constants.FIELD_TRIPLE_OBJECT, insertedID);
				kvLine.append(insertedID).
					append(Constants.OUTPUT_DELIMITER).append(isLiteral);
				
				tripleCollection.insert(tripleDocument);
				// write to file in kv format for Hadoop jobs
				writer.println(kvLine.toString());
				
				tripleCount++;
				if(tripleCount%20000 == 0) {
					System.out.println("Done with " + tripleCount + " triples");
					if(indexOnce) {
						indexOnce = false;
						System.out.println("Creating index for value");		
						BasicDBObject valIndex = new BasicDBObject();		
						valIndex.put(Constants.FIELD_HASH_VALUE, 1);
						idValCollection.ensureIndex(valIndex);
						eidValCollection.ensureIndex(valIndex);
						
						System.out.println("Creating index for id");
						BasicDBObject idIndex = new BasicDBObject();		
						idIndex.put(Constants.FIELD_ID, 1);
						idValCollection.ensureIndex(idIndex);
						eidValCollection.ensureIndex(idIndex);
					}
				}
			}
			if(in != null)
				in.close();
		}
		System.out.println("Saved IDs to DB and wrote the KV file");
		
		System.out.println("Creating index for subject");
		BasicDBObject subjectIndex = new BasicDBObject();		
		subjectIndex.put(Constants.FIELD_TRIPLE_SUBJECT, 1);
		tripleCollection.ensureIndex(subjectIndex);
		
		System.out.println("Creating index for predicate");
		BasicDBObject predIndex = new BasicDBObject();		
		predIndex.put(Constants.FIELD_TRIPLE_PREDICATE, 1);
		tripleCollection.ensureIndex(predIndex);
		
		System.out.println("Creating index for object");
		BasicDBObject objectIndex = new BasicDBObject();		
		objectIndex.put(Constants.FIELD_TRIPLE_OBJECT, 1);
		tripleCollection.ensureIndex(objectIndex);
		
		System.out.println("Done");
		if(in != null)
			in.close();
		mongo.close();
		writer.close();
	}
	
	private void initializeShards() throws Exception {
//		PropertyFileHandler propertyFileHandler = 
//						PropertyFileHandler.getInstance();
		
//		HostInfo mongoSHostPort = propertyFileHandler.getMongoSHostInfo();
		HostInfo mongoSHostPort = new HostInfo("ip-10-50-73-199.eu-west-1.compute.internal", 27017);
		mongo = new Mongo(mongoSHostPort.getHost(), mongoSHostPort.getPort());
		DB adminDB = mongo.getDB(Constants.MONGO_ADMIN_DB);
//		List<HostInfo> shardsHostInfo = propertyFileHandler.getAllShardsInfo();
		List<HostInfo> shardsHostInfo = new ArrayList<HostInfo>();
		shardsHostInfo.add(new HostInfo("ip-10-54-83-139.eu-west-1.compute.internal", 10000));
		shardsHostInfo.add(new HostInfo("ip-10-62-93-127.eu-west-1.compute.internal", 10000));
		shardsHostInfo.add(new HostInfo("ip-10-54-82-191.eu-west-1.compute.internal", 10000));
		shardsHostInfo.add(new HostInfo("ip-10-62-119-163.eu-west-1.compute.internal", 10000));
		
		// add shards
		for(HostInfo hinfo : shardsHostInfo) {
			adminDB.command(new BasicDBObject(Constants.MONGO_CONFIG_ADD_SHARD, 
					hinfo.getHost() + ":" + hinfo.getPort()));
		}
		// enable sharding of the db
		adminDB.command(new BasicDBObject(Constants.MONGO_ENABLE_SHARDING, 
				Constants.MONGO_RDF_DB));
		
		// enable sharding of specific collections - idvals, triples collection
		BasicDBObject optionsDoc = new BasicDBObject();
		optionsDoc.put(Constants.MONGO_SHARD_COLLECTION, 
				Constants.MONGO_RDF_DB + "." + Constants.MONGO_IDVAL_COLLECTION);
		optionsDoc.put("key", Constants.FIELD_ID);
		adminDB.command(optionsDoc);
		
		// do it for edge(predicate) ID vals as well
		optionsDoc.clear();
		optionsDoc.put(Constants.MONGO_SHARD_COLLECTION, 
				Constants.MONGO_RDF_DB + "." + Constants.MONGO_EIDVAL_COLLECTION);
		optionsDoc.put("key", Constants.FIELD_ID);
		adminDB.command(optionsDoc);
		
		optionsDoc.clear();
		optionsDoc.put(Constants.MONGO_SHARD_COLLECTION, 
				Constants.MONGO_RDF_DB + "." + Constants.MONGO_TRIPLE_COLLECTION);
		optionsDoc.put("key", Constants.FIELD_TRIPLE_SUBJECT);
		adminDB.command(optionsDoc);
	}
	
	private long checkAndInsertID(String tripleValue, 
			long idCount, DBCollection collection) throws Exception {
		DBObject queryID = new BasicDBObject();
		// use SHA-1 to hash the URIs since MongoDB has problems
		// in indexing big keys.
		String digestValue = Util.generateMessageDigest(tripleValue);
		queryID.put(Constants.FIELD_HASH_VALUE, digestValue);
		DBObject resultID = collection.findOne(queryID);

		// check if the id mapping is present in DB
		if(resultID == null) {
			DBObject document = new BasicDBObject();
			document.put(Constants.FIELD_HASH_VALUE, digestValue);
			document.put(Constants.FIELD_ID, Long.valueOf(idCount));
			document.put(Constants.FIELD_STR_VALUE, tripleValue);
			collection.insert(document);
			return idCount;
		}
		else
			return (Long)resultID.get(Constants.FIELD_ID);
	}

	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			System.out.println("Provide the directory path of the triple files in NT format");
			System.exit(-1);
		}
		new IDLoader().saveIDsToDB(args[0]);
	}

}
