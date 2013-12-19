package dsparq.partitioning;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.GregorianCalendar;
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
 * The IDLoader2 may generate holes in IDs, i.e., the
 * IDs might not be in a sequence which is required for
 * METIS. This class is used to generate wrapper IDs i.e.
 * ID on top of an ID. This set of wrapper IDs would be 
 * sequential.
 * 
 * @author Raghava
 *
 */
public class PseudoIDGenerator {
	
	public void generateIDs(String vidPath, String adjVIDPath) throws Exception {
		PropertyFileHandler propertyFileHandler = 
									PropertyFileHandler.getInstance();
		HostInfo mongosHostInfo = propertyFileHandler.getMongoSHostInfo();
		Mongo mongo = new Mongo(mongosHostInfo.getHost(), mongosHostInfo.getPort());
		DB rdfDB = mongo.getDB(Constants.MONGO_RDF_DB);
		DBCollection wrappedIDCollection = rdfDB.getCollection(
								Constants.MONGO_WRAPPED_ID_COLLECTION);
/*		
		BufferedInputStream inputStream = new BufferedInputStream(
									new FileInputStream(new File(vidPath)));
		Scanner scanner = new Scanner(inputStream, "UTF-8");
		
		// generate an alternate VertexID file
		PrintWriter writer
		   = new PrintWriter(new BufferedWriter(new FileWriter("wrapped-vertex-id")));
		
		// read vertexID file first and generate pseudo/wrapper IDs.
		System.out.println("Reading vid file and generating IDs");
		GregorianCalendar start = new GregorianCalendar();
		long wrapperID = 1;
		while(scanner.hasNext()) {
			String line = scanner.nextLine().trim();
			long vid = Long.parseLong(line);
			// check if vid already exists
			DBObject result = wrappedIDCollection.findOne(new BasicDBObject
												(Constants.FIELD_ID, vid));
			if(result == null) {
				// insert into DB
				BasicDBObject insertDoc = new BasicDBObject();
				insertDoc.put(Constants.FIELD_ID, vid);
				insertDoc.put(Constants.FIELD_WID, wrapperID);
				wrappedIDCollection.insert(insertDoc);
				writer.println(wrapperID);
				wrapperID++;
				
				if((wrapperID % 50000) == 0)
					System.out.println("Reached " + wrapperID);
			}
		}
		writer.close();
		inputStream.close();
		scanner.close();
		
		System.out.println("Done with phase-1");
		printElapsedTime(start);
*/		
		BufferedInputStream inputStream2 = new BufferedInputStream(
				new FileInputStream(new File(adjVIDPath)));
		Scanner scanner2 = new Scanner(inputStream2, "UTF-8");
		PrintWriter writer2
		   = new PrintWriter(new BufferedWriter(new FileWriter("wrapped-adjvertex-id2")));
		System.out.println("Checking adjacent vertices file");
		GregorianCalendar start2 = new GregorianCalendar();
		long lineCount = 0;
		while(scanner2.hasNext()) {
			String line = scanner2.nextLine().trim();
			String[] adjVertexIDs = line.split("\\s");
			StringBuilder adjVertexBuilder = new StringBuilder();
			for(String s : adjVertexIDs) {
				DBObject result = wrappedIDCollection.findOne(new BasicDBObject
									(Constants.FIELD_ID, Long.parseLong(s)));
				adjVertexBuilder.append((Long)result.
						get(Constants.FIELD_WID)).append(" ");
			}
			writer2.println(adjVertexBuilder.toString());
			lineCount++;
			if((lineCount % 50000) == 0)
				System.out.println("Reached " + lineCount);
		}
		System.out.println("Done phase-2");
		printElapsedTime(start2);
		
		writer2.close();
		inputStream2.close();
		scanner2.close();
		mongo.close();
	}
	
	private static void printElapsedTime(GregorianCalendar startTime) {
		GregorianCalendar iterEnd = new GregorianCalendar();		
		double totalDiff = (iterEnd.getTimeInMillis() - startTime.getTimeInMillis())/1000;
		long totalMins = (long)totalDiff/60;
		double totalSecs = totalDiff - (totalMins * 60);
		System.out.println(totalMins + " mins and " + totalSecs + " secs");
	}

	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			String msg = "Needs 2 inputs -- the outputs of UndirectedGraphPartitioner \n\t" +
						 "vertexID file and adjVertexID file ";
			throw new Exception(msg);
		}
		new PseudoIDGenerator().generateIDs(args[0], args[1]);
	}
}
