package dsparq.load;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import dsparq.misc.HostInfo;
import dsparq.misc.PropertyFileHandler;
import dsparq.util.Util;

/**
 * This class is used to insert triples in
 * integer ID form. For each subject/predicate/object
 * of a triple, their equivalent integer ID is inserted
 * into the DB.
 * 
 * @author Raghava
 *
 */
public class TripleIDInserter {

	private Mongo mongo;
	private DBCollection idValCollection;
	private DBCollection eidValCollection;
	private DBCollection tripleCollection;
	private Model model;
	
	public TripleIDInserter() {
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
			int hostID = Integer.parseInt(matcher.group());
			mongo = new Mongo(mongosInfo.get(hostID%mongosCount).getHost(), 
						mongosInfo.get(hostID%mongosCount).getPort());
			DB db = mongo.getDB(Constants.MONGO_RDF_DB);
			idValCollection = db.getCollection(
					Constants.MONGO_IDVAL_COLLECTION);
			eidValCollection = db.getCollection(
								Constants.MONGO_EIDVAL_COLLECTION);
			tripleCollection = db.getCollection(
								Constants.MONGO_TRIPLE_COLLECTION);
			model = ModelFactory.createDefaultModel();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void insertTripleIDs(String path) throws Exception {
		File inputPath = new File(path);
		File[] allfiles = inputPath.listFiles();
		long count = 1;
		List<DBObject> docList = new ArrayList<DBObject>();
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
				BasicDBObject tripleDoc = new BasicDBObject();
				String subject = triple.getSubject().toString();
				String hashSub = Util.generateMessageDigest(subject);
				DBObject resultDoc1 = idValCollection.findOne(
						new BasicDBObject(Constants.FIELD_HASH_VALUE, hashSub), 
						new BasicDBObject(Constants.FIELD_ID, 1));
				tripleDoc.put(Constants.FIELD_TRIPLE_SUBJECT, 
						resultDoc1.get(Constants.FIELD_ID));
				
				String predicate = triple.getPredicate().toString();
				String hashPred = Util.generateMessageDigest(predicate);
				DBObject resultDoc2 = eidValCollection.findOne(
						new BasicDBObject(Constants.FIELD_HASH_VALUE, hashPred), 
						new BasicDBObject(Constants.FIELD_ID, 1));
				tripleDoc.put(Constants.FIELD_TRIPLE_PREDICATE, 
						resultDoc2.get(Constants.FIELD_ID));
				
				String object = triple.getObject().toString();
				String hashObj = Util.generateMessageDigest(object);
				DBObject resultDoc3 = idValCollection.findOne(
						new BasicDBObject(Constants.FIELD_HASH_VALUE, hashObj), 
						new BasicDBObject(Constants.FIELD_ID, 1));
				tripleDoc.put(Constants.FIELD_TRIPLE_OBJECT, 
						resultDoc3.get(Constants.FIELD_ID));
				docList.add(tripleDoc);
				
				if(docList.size() >= 10000) {
					tripleCollection.insert(docList);
					docList.clear();
				}
			}
			reader.close();
			fileReader.close();
			model.removeAll();
		}
		if(!docList.isEmpty()) {
			tripleCollection.insert(docList);
			docList.clear();
		}
		Util.getElapsedTime(start);
		mongo.close();
		model.close();
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			throw new Exception("Provide path to the folder containing N-Triples");
		}
		new TripleIDInserter().insertTripleIDs(args[0]);
	}
}
