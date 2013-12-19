package dsparq.load;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import org.openjena.riot.RiotReader;
import org.openjena.riot.lang.LangNTriples;

import com.hp.hpl.jena.graph.Triple;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import dsparq.misc.Constants;
import dsparq.misc.HostInfo;
import dsparq.misc.PropertyFileHandler;
import dsparq.util.Util;

public class IDGenerator {

	public void generateIDs(String dirPath) throws Exception {
		PropertyFileHandler propertyFileHandler = 
					PropertyFileHandler.getInstance();

		HostInfo mongosInfo = propertyFileHandler.getMongoSHostInfo();
		Mongo mongo = new Mongo(mongosInfo.getHost(), mongosInfo.getPort());
		DB rdfDB = mongo.getDB(Constants.MONGO_RDF_DB);
		DBCollection idValCollection = rdfDB.getCollection(
											Constants.MONGO_IDVAL_COLLECTION);
		DBCollection eidValCollection = rdfDB.getCollection(
											Constants.MONGO_EIDVAL_COLLECTION);
//		DBCollection tripleCollection = rdfDB.getCollection(Constants.MONGO_TRIPLE_COLLECTION);
		
		File ntFolder = new File(dirPath);
		File[] allfiles = ntFolder.listFiles();
		List<File> ntfiles = new ArrayList<File>();
		for(File f : allfiles)
			if(f.getName().endsWith(".nt"))
				ntfiles.add(f);
		
		List<DBObject> docList1 = new ArrayList<DBObject>();
		List<DBObject> docList2 = new ArrayList<DBObject>();
		FileInputStream in = null;
		System.out.println("Total nt files: " + ntfiles.size());
		long count = 0;
		double multiplier = 1.0;
		for(File ntfile : ntfiles) {
			in = new FileInputStream(ntfile);
			LangNTriples ntriples = RiotReader.createParserNTriples(in, null);
			count++;
			if(count >= (ntfiles.size() * 0.1 * multiplier)) {
				System.out.println("Done with " + count);
				multiplier++;
			}
			
			while(ntriples.hasNext()) {
				Triple triple = ntriples.next();
				String subject = triple.getSubject().toString();
				String hashSub = Util.generateMessageDigest(subject);
				BasicDBObject subDoc = new BasicDBObject();
				subDoc.put(Constants.FIELD_STR_VALUE, subject);
				subDoc.put(Constants.FIELD_HASH_VALUE, hashSub);
				subDoc.put("isType", "false");
				docList1.add(subDoc);
				
				String predicate = triple.getPredicate().toString();
				String hashPred = Util.generateMessageDigest(predicate);
				boolean isType = false;
				if(predicate.equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") ||
						   predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") ||
						   predicate.equals("rdf:type"))
					isType = true;
				BasicDBObject subPred = new BasicDBObject();
				subPred.put(Constants.FIELD_STR_VALUE, predicate);
				subPred.put(Constants.FIELD_HASH_VALUE, hashPred);
				docList2.add(subPred);
				
				String object = triple.getObject().toString();
				String hashObj = Util.generateMessageDigest(object);
				BasicDBObject subObj = new BasicDBObject();
				subObj.put(Constants.FIELD_STR_VALUE, object);
				subObj.put(Constants.FIELD_HASH_VALUE, hashObj);
				subObj.put("isType", isType);
				docList1.add(subObj);
				
				if(docList1.size() >= 10000) {
					insertDocs(idValCollection, docList1);
					docList1.clear();
				}
				if(docList2.size() >= 10000) {
					insertDocs(eidValCollection, docList2);
					docList2.clear();
				}
			}
		}
		if(!docList1.isEmpty())
			insertDocs(idValCollection, docList1);
		if(!docList2.isEmpty())
			insertDocs(eidValCollection, docList2);
		if(in != null)
			in.close();
	}
	
	private void insertDocs(DBCollection collection, List<DBObject> docs) {
		for(int i=0; i<docs.size(); i++) {
			try {
				collection.insert(docs.get(i), WriteConcern.SAFE);
			}
			catch(MongoException.DuplicateKey e) {
				// duplicate hash entry
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			throw new Exception("Provide dir path to nt files");
		}
		new IDGenerator().generateIDs(args[0]);
	}
}
