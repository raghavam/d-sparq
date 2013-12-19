package dsparq.partitioning;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Scanner;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import dsparq.misc.Constants;
import dsparq.misc.HostInfo;
import dsparq.misc.PropertyFileHandler;

/**
 * This class is used to unwrap the pseudo IDs and 
 * assign the real IDs to vertices
 * 
 * @author Raghava
 *
 */
public class IDUnwrapper {
	
	public void unwrapIDs(String path) throws Exception {
		BufferedInputStream inputStream = new BufferedInputStream(
				new FileInputStream(new File(path)));
		Scanner scanner = new Scanner(inputStream, "UTF-8");
		
		PropertyFileHandler propertyFileHandler = 
								PropertyFileHandler.getInstance();
		HostInfo mongosHostInfo = propertyFileHandler.getMongoSHostInfo();
		Mongo mongo = new Mongo(mongosHostInfo.getHost(), 
							mongosHostInfo.getPort());
		DB rdfDB = mongo.getDB(Constants.MONGO_RDF_DB);
		DBCollection wrapIDCollection = rdfDB.getCollection(
									Constants.MONGO_WRAPPED_ID_COLLECTION);
		
		PrintWriter writer
		   = new PrintWriter(new BufferedWriter(new FileWriter("unwrapped-vertex-id")));
		
		System.out.println("Started unwrapping...");
		while(scanner.hasNext()) {
			long vertexID = Long.parseLong(scanner.nextLine().trim());
			DBObject unwrappedDoc = wrapIDCollection.findOne(new BasicDBObject(
												Constants.FIELD_WID, vertexID));
			long unwrappedVertexID = (Long)unwrappedDoc.get(Constants.FIELD_ID);
			writer.println(unwrappedVertexID);
		}
		System.out.println("Done");
		
		writer.close();
		mongo.close();
	}

	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			throw new Exception("Provide vertex ID file");
		}
		new IDUnwrapper().unwrapIDs(args[0]);
	}
}
