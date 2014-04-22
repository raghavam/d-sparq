package dsparq.load;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

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

/**
 * A hadoop job which essentially does the same thing as 
 * the java program IDLoader.
 * 
 * Known issue: unique ID generation in distributed environment
 * is not straight forward. In this implementation, if the same
 * string is checked by different reducers, then there is a possibility
 * of having a hole in the sequence -- i.e. a number could be missing
 * from the sequence with that number not getting assigned to any string.
 * 
 * @author Raghava
 *
 */
public class IDLoader2 extends Configured implements Tool {

	private static final Logger log = Logger.getLogger(IDLoader2.class); 
	
	private static class Map extends MapReduceBase implements 
			Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key, Text value, 
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			
			String[] tripleTokens = key.toString().trim().split("\\s");
			output.collect(new Text(tripleTokens[Constants.POSITION_SUBJECT]), 
					new Text(tripleTokens[Constants.POSITION_PREDICATE] +
					         Constants.OUTPUT_DELIMITER + 
					         tripleTokens[Constants.POSITION_OBJECT]));
		}
	}
	
	private static class Reduce extends MapReduceBase implements 
			Reducer<Text, Text, Text, Text> {

		private Mongo mongo;
		private DBCollection idValCollection;
		private DBCollection eidValCollection;
		private DBCollection tripleCollection;
		private DBCollection seqIDCollection;
		
		@Override
		public void configure(JobConf conf) {
			try {
				String hostInfo = conf.get("MongosInfo");
				String[] hostPort = hostInfo.split(":");
				mongo = new Mongo(hostPort[0], Integer.parseInt(hostPort[1]));
				DB db = mongo.getDB(Constants.MONGO_RDF_DB);
				idValCollection = db.getCollection(Constants.MONGO_IDVAL_COLLECTION);
				eidValCollection = db.getCollection(Constants.MONGO_EIDVAL_COLLECTION);
				tripleCollection = db.getCollection(Constants.MONGO_TRIPLE_COLLECTION);
				seqIDCollection = db.getCollection(Constants.MONGO_SEQID_COLLECTION);
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			try {
				// check and insert IDs of key & values.
				Long subjectID = checkIfIDPresent(key.toString(), idValCollection);
				if(subjectID == Long.MIN_VALUE) {
					subjectID = generateSequenceID(Constants.FIELD_SEQID, false); 
					subjectID = insertID(key.toString(), subjectID, idValCollection);
				}
//				log.info("Subject: " + key.toString() + "  ID: " + subjectID);
				
				Long predicateID, objectID;
				while(values.hasNext()) {
					String[] predObj = values.next().toString().trim().
											split(Constants.REGEX_DELIMITER);
					boolean isType = false;
					if(predObj[0].equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") ||
							predObj[0].equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") ||
							predObj[0].equals("rdf:type")) {
						isType = true;
						predicateID = Long.parseLong(Constants.PREDICATE_TYPE);
						objectID = checkIfIDPresent(predObj[1], idValCollection);
						if(objectID == Long.MIN_VALUE) {
							objectID = generateSequenceID(Constants.FIELD_TYPE_SEQID, isType);
							objectID = insertID(predObj[1], objectID, idValCollection);
						}
					}
					else {
						predicateID = checkIfIDPresent(predObj[0], eidValCollection);
						objectID = checkIfIDPresent(predObj[1], idValCollection);
						if(predicateID == Long.MIN_VALUE) {
							predicateID = generateSequenceID(Constants.FIELD_PRED_SEQID, isType);
							predicateID = insertID(predObj[0], predicateID, eidValCollection);
						}
						if(objectID == Long.MIN_VALUE) {
							objectID = generateSequenceID(Constants.FIELD_SEQID, isType);
							objectID = insertID(predObj[1], objectID, idValCollection);
						}
					}
					
//					log.info("Predicate: " + predObj[0] + "  ID: " + predicateID);
//					log.info("Object: " + predObj[1] + "  ID: " + objectID + "\n");
					
					DBObject tripleDoc = new BasicDBObject();
					tripleDoc.put(Constants.FIELD_TRIPLE_SUBJECT, subjectID);
					tripleDoc.put(Constants.FIELD_TRIPLE_PREDICATE, predicateID);
					tripleDoc.put(Constants.FIELD_TRIPLE_OBJECT, objectID);
					tripleCollection.insert(tripleDoc);
					
					// output the values 
					// TODO: isType is not same as isLiteral. Replace it later
					output.collect(new Text(subjectID + Constants.OUTPUT_DELIMITER + 
							predicateID + Constants.OUTPUT_DELIMITER +
							objectID + Constants.OUTPUT_DELIMITER +
							isType), new Text());
				}
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void close() throws IOException {
			mongo.close();
		}
		
		private Long insertID(String value, Long id, DBCollection collection) {
			// use SHA-1 to hash the URIs since MongoDB has problems
			// in indexing big keys.
			String digestValue = null;
			Long checkedID = id;
			try {
				digestValue = Util.generateMessageDigest(value);
				DBObject document = new BasicDBObject();
				document.put(Constants.FIELD_HASH_VALUE, digestValue);
				document.put(Constants.FIELD_ID, Long.valueOf(id));
				document.put(Constants.FIELD_STR_VALUE, value);
				// check if this is a duplicate insertion
				collection.insert(document, WriteConcern.SAFE);
			}
			catch(MongoException.DuplicateKey e) {
				// since this is duplicate, read the existing id and return it
				DBObject result = collection.findOne(new BasicDBObject(
						Constants.FIELD_HASH_VALUE, digestValue));
				checkedID = (Long)result.get(Constants.FIELD_ID);
				log.error("dup key. existing id: " + checkedID + "  new id: " + id);
				e.printStackTrace();
			}
			catch(Exception e) {
				System.out.println("Exception for value: " + value + 
						" and id: " + id);
				e.printStackTrace();
			}
			return checkedID;
		}
		
		private Long generateSequenceID(String fieldName, boolean isType) {
			int differential;
			String comparisonOp;
			if(isType) {
				differential = -1;
				comparisonOp = "$lte";
			}
			else {
				differential = 1;
				comparisonOp = "$gte";
			}
			DBObject result = seqIDCollection.findAndModify(
					new BasicDBObject(fieldName, 
							new BasicDBObject(comparisonOp, 0)), 
					new BasicDBObject(fieldName, 1), 
					null, false, 
					new BasicDBObject("$inc", 
							new BasicDBObject(fieldName,differential)), 
					true, false);
			return (Long)result.get(fieldName);
		}
		
		private Long checkIfIDPresent(String tripleValue, 
				DBCollection collection) throws Exception {
			DBObject queryID = new BasicDBObject();
			// use SHA-1 to hash the URIs since MongoDB has problems
			// in indexing big keys.
			String digestValue = Util.generateMessageDigest(tripleValue);
			queryID.put(Constants.FIELD_HASH_VALUE, digestValue);
			DBObject resultID = collection.findOne(queryID);
			return (resultID == null) ? Long.MIN_VALUE : 
										(Long)resultID.get(Constants.FIELD_ID);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {

		String triples = args[0];
		String outputDir = args[1];
		String mongosHostPort = args[2];
		
		Path outputPath = new Path(outputDir);
		Configuration fconf = new Configuration();
		FileSystem fs = FileSystem.get(fconf);

		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		
		JobConf jobConf = new JobConf(this.getClass());
		jobConf.setJobName("IDLoader2");
		
		jobConf.set("MongosInfo", mongosHostPort);
		
		FileOutputFormat.setOutputPath(jobConf, outputPath);
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);	
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(jobConf, new Path(triples));

		jobConf.setMapperClass(Map.class);
		jobConf.setReducerClass(Reduce.class);
//		jobConf.setMapOutputKeyClass(LongWritable.class);
//		jobConf.setMapOutputValueClass(LongWritable.class);
		
//		jobConf.setNumReduceTasks(0);
		
		PropertyFileHandler propertyFileHandler = 
			PropertyFileHandler.getInstance();
		
		int numReduceTasks = (int)(0.95 * Integer.parseInt(jobConf.get(
				"mapred.tasktracker.reduce.tasks.maximum")) * 
				propertyFileHandler.getShardCount());
		jobConf.setNumReduceTasks(numReduceTasks);
		
		RunningJob job1 = JobClient.runJob(jobConf);

		if (job1.isSuccessful()) {

		} else {
			System.out.println("FAILED!!!");
		}
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			String msg = "Incorrect arguments -- requires 2 arguments.\n\t " +
					"1) NT Triples file \n\t" +
					"2) Output path";
			throw new Exception(msg);
		}
		
		IDLoader2 idLoader = new IDLoader2();
		idLoader.initializeShards();
		// read property file here and just pass the necessary values
		PropertyFileHandler propHandler = PropertyFileHandler.getInstance();
		HostInfo mongoSHostInfo = propHandler.getMongoSHostInfo();
		String[] extraArgs = new String[args.length + 1];
		for(int i=0; i<args.length; i++)
			extraArgs[i] = args[i];
		extraArgs[args.length] = mongoSHostInfo.toString();
		ToolRunner.run(new Configuration(), idLoader, extraArgs);
	}
	
	private void initializeShards() throws Exception {
		
		PropertyFileHandler propertyFileHandler = 
						PropertyFileHandler.getInstance();
		
		HostInfo mongoSHostPort = propertyFileHandler.getMongoSHostInfo();
//		HostInfo mongoSHostPort = new HostInfo("ip-10-50-73-199.eu-west-1.compute.internal", 27017);
		Mongo mongo = new Mongo(mongoSHostPort.getHost(), mongoSHostPort.getPort());
/*		
		DB adminDB = mongo.getDB(Constants.MONGO_ADMIN_DB);
		List<HostInfo> shardsHostInfo = propertyFileHandler.getAllShardsInfo();
		
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
*/		
		// create unique indexes (to avoid duplicates) on all the above collections
		DB rdfDB = mongo.getDB(Constants.MONGO_RDF_DB);
		
/*		
		DBCollection idValCollection = rdfDB.getCollection(
											Constants.MONGO_IDVAL_COLLECTION);
		idValCollection.ensureIndex(new BasicDBObject(Constants.FIELD_HASH_VALUE, 1), 
						new BasicDBObject(Constants.MONGO_UNIQUE_INDEX, true));
		DBCollection eidValCollection = rdfDB.getCollection(
											Constants.MONGO_EIDVAL_COLLECTION);
		eidValCollection.ensureIndex(new BasicDBObject(Constants.FIELD_HASH_VALUE, 1), 
				new BasicDBObject(Constants.MONGO_UNIQUE_INDEX, true));
 		DBCollection tripleCollection = rdfDB.getCollection(
											Constants.MONGO_TRIPLE_COLLECTION);
		BasicDBObject indexDoc = new BasicDBObject();
		indexDoc.put(Constants.FIELD_TRIPLE_SUBJECT, 1);
		indexDoc.put(Constants.FIELD_TRIPLE_PREDICATE, 1);
		indexDoc.put(Constants.FIELD_TRIPLE_OBJECT, 1);
		tripleCollection.ensureIndex(indexDoc, 
				new BasicDBObject(Constants.MONGO_UNIQUE_INDEX, true));
*/		
		// add sequence id to DB		
		DBCollection seqCol = rdfDB.getCollection(
								Constants.MONGO_SEQID_COLLECTION);
		seqCol.insert(new BasicDBObject(Constants.FIELD_SEQID, Constants.START_ID-1));
		seqCol.insert(new BasicDBObject(Constants.FIELD_PRED_SEQID, 
				Constants.START_PREDICATE_ID-1));
		seqCol.insert(new BasicDBObject(Constants.FIELD_TYPE_SEQID, Constants.START_TYPE_ID));
		
		mongo.close();
	}

}
