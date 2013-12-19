package dsparq.partitioning;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import dsparq.misc.Constants;
import dsparq.misc.HostInfo;
import dsparq.misc.PropertyFileHandler;

/**
 * This class is used to insert triples into their
 * respective partitions. It also generates an nt 
 * file which is input to RDF-3X. 
 * 
 * Note that this class could be removed when partitions
 * are done on separate machines. A hadoop job could be
 * used instead of this class.
 * 
 * @author Raghava
 *
 */
public class PartitionTripleInserter {

	public void tripleInserter(String path) throws Exception {
		PropertyFileHandler propertyFileHandler = 
			PropertyFileHandler.getInstance();
		int numPartitions = propertyFileHandler.getShardCount();
		BufferedReader reader = new BufferedReader(new FileReader(
				new File(path)));
		List<PrintWriter> writerList = new ArrayList<PrintWriter>(numPartitions); 
		for(int i=0; i<numPartitions; i++)
			writerList.add(i, new PrintWriter(new BufferedWriter(
					new FileWriter(new File("partriple" + i + ".nt")))));

		List<Mongo> mongoShards = new ArrayList<Mongo>(numPartitions);
		List<HostInfo> shardsHostInfo = propertyFileHandler.getAllShardsInfo();
		for(int i=0; i<numPartitions; i++) {
			HostInfo hostInfo = shardsHostInfo.get(i);
			mongoShards.add(i, new Mongo(hostInfo.getHost(), hostInfo.getPort()));
		}
		HostInfo mongoSHostInfo = propertyFileHandler.getMongoSHostInfo();
		Mongo mongoS = new Mongo(mongoSHostInfo.getHost(), mongoSHostInfo.getPort());
		List<DBCollection> partitionedTripleCollections = 
			new ArrayList<DBCollection>(numPartitions);
		for(int i=0; i<numPartitions; i++) {
			DB db = mongoShards.get(i).getDB(Constants.MONGO_RDF_DB);
			partitionedTripleCollections.add(i, db.getCollection(
							Constants.MONGO_PARTITIONED_VERTEX_COLLECTION));
		}

		DB db = mongoS.getDB(Constants.MONGO_RDF_DB);
		DBCollection tripleCollection = db.getCollection(
										Constants.MONGO_TRIPLE_COLLECTION);
		DBCollection idValCollection = db.getCollection(
										Constants.MONGO_IDVAL_COLLECTION);
		DBCollection eidValCollection = db.getCollection(
										Constants.MONGO_EIDVAL_COLLECTION);
		
		String line;
		long count = 0;
		System.out.println("Start...");
		while((line = reader.readLine()) != null) {
			String[] vertexPartitionID = line.trim().split("\\s");
			long vertexID = Long.parseLong(vertexPartitionID[0]);
			int partitionID = Integer.parseInt(vertexPartitionID[1]);

			// get all the triples with vertexID as the subject
			// there won't be duplicate vertices - each vertex is assigned
			// only 1 partition
			DBCursor resultCursor = tripleCollection.find(new BasicDBObject(
					Constants.FIELD_TRIPLE_SUBJECT, vertexID));
			while(resultCursor.hasNext()) {
				DBObject resultDoc = resultCursor.next();
				long subject = (Long)resultDoc.get(
									Constants.FIELD_TRIPLE_SUBJECT);
				long predicate = (Long)resultDoc.get(
									Constants.FIELD_TRIPLE_PREDICATE);
				long object = (Long)resultDoc.get(
									Constants.FIELD_TRIPLE_OBJECT);
				BasicDBObject tripleDoc = new BasicDBObject();
				tripleDoc.put(Constants.FIELD_TRIPLE_SUBJECT, subject);
				tripleDoc.put(Constants.FIELD_TRIPLE_PREDICATE, predicate);
				tripleDoc.put(Constants.FIELD_TRIPLE_OBJECT, object);
				partitionedTripleCollections.get(partitionID).insert(tripleDoc);
								
				// convert IDs to String and put them in nt format for
				// RDF-3X to read it.
				DBObject subResultDoc = idValCollection.findOne(
						new BasicDBObject(Constants.FIELD_ID, subject));
				String subStr = (String)subResultDoc.get(Constants.FIELD_STR_VALUE);
				subStr = "<" + subStr + ">";
				
				DBObject predResultDoc = eidValCollection.findOne(
						new BasicDBObject(Constants.FIELD_ID, predicate));
				String predStr = (String)predResultDoc.get(Constants.FIELD_STR_VALUE);
				predStr = "<" + predStr + ">";
				
				DBObject objResultDoc = idValCollection.findOne(
						new BasicDBObject(Constants.FIELD_ID, object));
				String objStr = (String)objResultDoc.get(Constants.FIELD_STR_VALUE);
				if(objStr.startsWith("http"))
					objStr = "<" + objStr + ">";
				writerList.get(partitionID).println(subStr + " " + 
						predStr + " " + objStr + " .");
			}
			count++;
			if(count % 20000 == 0)
				System.out.println("Done with " + count);
		}
		System.out.println("\nDone");
		System.out.println("Creating indexes on subject, predicate " +
				"and object in all partitions");
		for(DBCollection ptripleCollection : partitionedTripleCollections) {
			ptripleCollection.ensureIndex(new BasicDBObject(
					Constants.FIELD_TRIPLE_SUBJECT, 1));
			ptripleCollection.ensureIndex(new BasicDBObject(
					Constants.FIELD_TRIPLE_PREDICATE, 1));
			ptripleCollection.ensureIndex(new BasicDBObject(
					Constants.FIELD_TRIPLE_OBJECT, 1));
		}
		reader.close();
		for(PrintWriter writer : writerList)
			writer.close();
		for(Mongo shard : mongoShards)
			shard.close();
		mongoS.close();
	}
	
	public static void main(String[] args) throws Exception {
		// input: file contains all vertex IDs of a particular partition
		if(args.length != 1) {
			throw new Exception("Provide vertex-partition file ");
		}
		new PartitionTripleInserter().tripleInserter(args[0]);
	}
}
