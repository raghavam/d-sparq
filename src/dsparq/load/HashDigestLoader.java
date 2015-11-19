package dsparq.load;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import dsparq.misc.Constants;
import dsparq.misc.HostInfo;
import dsparq.misc.PropertyFileHandler;
import dsparq.util.Util;

/**
 * Takes a set of files where each line is of the form
 * HashDigest|TypeID|StringValue where HashDigest is the hash of
 * subject/predicate/object. TypeID is -1 for object that follows rdf:type
 * predicate. It is 1 for everything else. -1 is required for Metis (after
 * removing "type" triples).
 * 
 * @author Raghava
 */
public class HashDigestLoader {

	public void insertTripleHashDigestsIntoDB(File[] files) {
		Mongo mongo = null;
		PropertyFileHandler propertyFileHandler = PropertyFileHandler
				.getInstance();
		HostInfo hostInfo = propertyFileHandler.getMongoRouterHostInfo();
		LinkedBlockingQueue<DBObject> tripleHashDocQueue = new LinkedBlockingQueue<DBObject>(
				Constants.CONTAINER_CAPACITY);
		try {
			mongo = new MongoClient(hostInfo.getHost(), hostInfo.getPort());
			DB db = mongo.getDB(Constants.MONGO_RDF_DB);
			DBCollection idValCollection = db
					.getCollection(Constants.MONGO_IDVAL_COLLECTION);
			int numThreads = Runtime.getRuntime().availableProcessors();
			CountDownLatch synchLatch = new CountDownLatch(numThreads);
			ExecutorService threadExecutor = Executors
					.newFixedThreadPool(numThreads);
			List<HashDigestDocConsumer> tasks = new ArrayList<HashDigestDocConsumer>(
					numThreads);

			AtomicInteger docCount = new AtomicInteger(0);
			for (int i = 0; i < numThreads; i++) {
				tasks.add(new HashDigestDocConsumer(tripleHashDocQueue,
						synchLatch, idValCollection, docCount));
				threadExecutor.submit(tasks.get(i));
			}

			String line;
			long numericID = 1;
			// Keeping the numID of subjects/objects separate from predicates.
			// This is useful/required when graph partitioning using Metis. 
			// If not, there will be holes in the sequence of vertex IDs.
			long predicateNumericID = 1;
			long ignoreID = -1;
			int tripleCount = 0;
			for (File file : files) {
				System.out.println("Inserting contents of " + file.getName());
				FileReader fileReader = new FileReader(file);
				BufferedReader bufferedReader = new BufferedReader(fileReader);
				while ((line = bufferedReader.readLine()) != null) {
					String[] splits = line.split(Constants.REGEX_DELIMITER);
					BasicDBObject doc = new BasicDBObject();
					doc.put(Constants.FIELD_HASH_VALUE, splits[0]);
					long typeID = Long.parseLong(splits[1]);
					doc.put(Constants.FIELD_TYPEID, typeID);
					if (typeID == 1) {
						if(splits[3].equals(Constants.PREDICATE_INDICATOR)) {
							doc.put(Constants.FIELD_NUMID, predicateNumericID);
//							System.out.println(line + "|" + predicateNumericID);
							predicateNumericID++;
						}
						else {
							doc.put(Constants.FIELD_NUMID, numericID);
//							System.out.println(line + "|" + numericID);
							numericID++;
						}
					} else if (typeID == -1) {
						doc.put(Constants.FIELD_NUMID, ignoreID);
//						System.out.println(line + "|" + ignoreID);
						ignoreID--;
					}
					
					// not saving the string values here since they take lot of
					// space. Convert queries to numerical equivalents and
					// compare.
					// doc.put(Constants.FIELD_STR_VALUE, splits[2]);

					tripleHashDocQueue.put(doc);
					tripleCount++;
				}
				// bulkInsert.execute();
				bufferedReader.close();
				fileReader.close();
				System.out.println("Done with " + file.getName());
			}
			// Null doc indicates end of documents
			BasicDBObject nullDoc = new BasicDBObject();
			nullDoc.put(Constants.FIELD_HASH_VALUE, null);
			// one for each thread
			for (int i = 0; i < numThreads; i++) {
				tripleHashDocQueue.put(nullDoc);
			}
			System.out.println("Added nulls to indicate end of insertion");
			synchLatch.await();
			threadExecutor.shutdown();
			System.out.println("\nDone");
			System.out.println("Triples: " + tripleCount + "  Docs: "
					+ docCount + " Both should match");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (mongo != null)
				mongo.close();
		}
	}
	
	/**
	 * counts the types i.e., rdf:type triples. This function is used just for
	 * verification.
	 * 
	 * @param files
	 */
	public void countTypes(File[] files) {
		try {
			String line;
			int count = 0;
			for (File file : files) {
				FileReader fileReader = new FileReader(file);
				BufferedReader bufferedReader = new BufferedReader(fileReader);
				while ((line = bufferedReader.readLine()) != null) {
					String[] splits = line.split(Constants.REGEX_DELIMITER);
					long typeID = Long.parseLong(splits[1]);
					if (typeID == -1) {
						System.out.println(line);
						count++;
					}
				}
				bufferedReader.close();
				fileReader.close();
				System.out.println("Done with " + file.getName());
				System.out.println("Count of types: " + count);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Give the path to directory containing files");
			System.exit(-1);
		}
		File dir = new File(args[0]);
		if (!dir.isDirectory()) {
			System.out.println("This is not a directory");
			System.exit(-1);
		}
		long startTime = System.nanoTime();
		new HashDigestLoader().insertTripleHashDigestsIntoDB(dir.listFiles());
		// new HashDigestLoader().countTypes(dir.listFiles());
		System.out.println("Time taken (secs): "
				+ Util.getElapsedTime(startTime));
	}

}

class HashDigestDocConsumer implements Runnable {

	private LinkedBlockingQueue<DBObject> docQueue;
	private CountDownLatch synchLatch;
	private DBCollection idValCollection;
	private BulkWriteOperation bulkInsert;
	private AtomicInteger docCount;

	HashDigestDocConsumer(LinkedBlockingQueue<DBObject> docQueue,
			CountDownLatch synchLatch, DBCollection idValCollection,
			AtomicInteger docCount) {
		this.docQueue = docQueue;
		this.synchLatch = synchLatch;
		this.idValCollection = idValCollection;
		bulkInsert = idValCollection.initializeUnorderedBulkOperation();
		this.docCount = docCount;
	}

	@Override
	public void run() {
		int count = 0;
		DBObject doc = null;
		while (true) {
			try {
				doc = docQueue.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (doc.get(Constants.FIELD_HASH_VALUE) == null)
				break;
			bulkInsert.insert(doc);
			docCount.incrementAndGet();

			count++;
			if (count == 1000) {
				bulkInsert.execute();
				bulkInsert = idValCollection.initializeUnorderedBulkOperation();
				count = 0;
			}
		}
		if (count > 1) {
			bulkInsert.execute();
			bulkInsert = idValCollection.initializeUnorderedBulkOperation();
			count = 0;
		}
		synchLatch.countDown();
		System.out.println(Thread.currentThread().getName() + 
				"  countDown: " + synchLatch.getCount());
	}
}
