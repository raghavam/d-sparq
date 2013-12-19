package dsparq.load;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import dsparq.misc.Constants;
import dsparq.util.Util;

public class SplitTripleLoader2 {

	public void insertHashDigest(String path) throws Exception {
		File inputPath = new File(path);
		File[] allfiles = inputPath.listFiles();
		long count = 1;
		Model model = ModelFactory.createDefaultModel();
		Mongo mongo = new Mongo("nimbus2", 27017);
		DB db = mongo.getDB(Constants.MONGO_RDF_DB);
		DBCollection idValCollection = db.getCollection(
							Constants.MONGO_IDVAL_COLLECTION);
		DBCollection eidValCollection = db.getCollection(
							Constants.MONGO_EIDVAL_COLLECTION);
		// unique index on hash
		BasicDBObject indexDoc = new BasicDBObject();
		indexDoc.put(Constants.FIELD_HASH_VALUE, 1);
		BasicDBObject uniqueOption = new BasicDBObject();
		uniqueOption.put("unique", true);
		idValCollection.ensureIndex(indexDoc, uniqueOption);
		eidValCollection.ensureIndex(indexDoc, uniqueOption);
		List<DBObject> docList1 = new ArrayList<DBObject>();
		List<DBObject> docList2 = new ArrayList<DBObject>();
		GregorianCalendar start = new GregorianCalendar();
		for(File file : allfiles) {
			FileReader fileReader = new FileReader(file);
			BufferedReader reader = new BufferedReader(fileReader);
			System.out.println("Reading file-" + count + " of " + allfiles.length);
			count++;
			// load this file into jena as a n-triple file
			model.read(reader, Constants.BASE_URI, "N-TRIPLE");
			StmtIterator stmtIterator = model.listStatements();
			while(stmtIterator.hasNext()) {
				Triple triple = stmtIterator.next().asTriple();
				String subject = triple.getSubject().toString();
				String hashSub = Util.generateMessageDigest(subject);
				BasicDBObject subDoc = new BasicDBObject();
				subDoc.put(Constants.FIELD_STR_VALUE, subject);
				subDoc.put(Constants.FIELD_HASH_VALUE, hashSub);
				subDoc.put("isType", false);
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
					idValCollection.insert(docList1);
					docList1.clear();
				}
				if(docList2.size() >= 10000) {
					eidValCollection.insert(docList2);
					docList2.clear();
				}
			}
			reader.close();
			fileReader.close();
			model.removeAll();
		}
		if(!docList1.isEmpty()) {
			idValCollection.insert(docList1);
			docList1.clear();
		}
		if(!docList2.isEmpty()) {
			eidValCollection.insert(docList2);
			docList2.clear();
		}
		Util.getElapsedTime(start);
		mongo.close();
		model.close();
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			throw new Exception("Provide path to input triples dir");
		}
		new SplitTripleLoader2().insertHashDigest(args[0]);
	}
}
