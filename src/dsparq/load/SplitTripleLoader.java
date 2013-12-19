package dsparq.load;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import dsparq.misc.Constants;

/**
 * This class takes a set of files containing triples in
 * sub|pred|obj|bool format and loads them to the local DB
 * 
 * @author Raghava
 *
 */
public class SplitTripleLoader {

	public void loadTriples(String dirPath) throws Exception {
		File inputDir = new File(dirPath);
		File[] allFiles = inputDir.listFiles();
		Mongo mongo = new Mongo("localhost", 10000);
		DB rdfDB = mongo.getDB(Constants.MONGO_RDF_DB);
		DBCollection ptripleCollection = rdfDB.getCollection(
								Constants.MONGO_PARTITIONED_VERTEX_COLLECTION);
		long lineCount = 0;
		List<DBObject> docList = new ArrayList<DBObject>();
		for(File tripleFile : allFiles) {
			BufferedInputStream inputStream = new BufferedInputStream(
					new FileInputStream(tripleFile));
			Scanner scanner = new Scanner(inputStream, "UTF-8");
			while(scanner.hasNext()) {
				String triple = scanner.nextLine();
				String[] tokens = triple.trim().split(Constants.REGEX_DELIMITER);
				BasicDBObject tripleDoc = new BasicDBObject();
				tripleDoc.put(Constants.FIELD_TRIPLE_SUBJECT, 
								Long.parseLong(tokens[Constants.POSITION_SUBJECT]));
				tripleDoc.put(Constants.FIELD_TRIPLE_PREDICATE, 
						Long.parseLong(tokens[Constants.POSITION_PREDICATE]));
				tripleDoc.put(Constants.FIELD_TRIPLE_OBJECT, 
						Long.parseLong(tokens[Constants.POSITION_OBJECT]));
				docList.add(tripleDoc);
				lineCount++;
				if(lineCount == 10000) {
					System.out.println("Done with 10000");
					// this is the bulk insert in MongoDB java
					ptripleCollection.insert(docList);
					lineCount = 0;
					docList.clear();
				}
			}
			if(!docList.isEmpty()) {
				ptripleCollection.insert(docList);
				lineCount = 0;
				docList.clear();
			}
			inputStream.close();
			scanner.close();
		}
		System.out.println("All triples inserted");
		mongo.close();
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			throw new Exception("Provide path to directory containing triples");
		}
		new SplitTripleLoader().loadTriples(args[0]);
	}
}
