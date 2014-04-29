package dsparq.partitioning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

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
 * input: (vertexID, partitionID)
 * output: sets of triples belonging to each particular partition
 * 
 * This class takes in the output of another Hadoop job which
 * gives out (vertexID, partitionID) and inserts triples
 * whose subject is same as vertexID into DB of that partition. It also
 * writes triples belonging to a partition in an .nt file. This
 * is later used to load these triples in RDF-3X.
 * 
 * @author Raghava
 *
 */
public class PartitionedTripleSplitter extends Configured implements Tool {

	private static final Logger log = Logger.getLogger(
						PartitionedTripleSplitter.class);
			
	private static class Map extends MapReduceBase implements 
			Mapper<Text, Text, LongWritable, LongWritable> {
		
		@Override
		public void map(Text key, Text value, 
				OutputCollector<LongWritable, LongWritable> output,
				Reporter reporter) throws IOException {
			// expected input format: vertexID partitionID
			
			// key itself contains vertexID and partitionID, value is empty
			String[] tokens = key.toString().split("\\s");
			output.collect(new LongWritable(Long.parseLong(tokens[1].toString())), 
					new LongWritable(Long.parseLong(tokens[0].toString())));
		}
	}
	
	private static class Reduce extends MapReduceBase implements 
			Reducer<LongWritable, LongWritable, Text, Text> {
		
		private java.util.Map<Long, DBCollection> shardMap = 
									new HashMap<Long, DBCollection>();
		private List<Mongo> shardMongos = new ArrayList<Mongo>();
		private MultipleOutputs multipleOutputs;
		private Mongo mongoRouter;
		private DBCollection tripleCollection;
		private DBCollection idValCollection;
		private DBCollection eidValCollection;
		private DBCollection wrappedIDCollection;
		
		@Override
		public void configure(JobConf conf) {
			try {
				multipleOutputs = new MultipleOutputs(conf);
				String hostInfo = conf.get("MongosInfo");
				String[] hostPort = hostInfo.split(":");
				mongoRouter = new Mongo(hostPort[0], Integer.parseInt(hostPort[1]));
				DB db = mongoRouter.getDB(Constants.MONGO_RDF_DB);
				tripleCollection = db.getCollection(Constants.MONGO_TRIPLE_COLLECTION);
				idValCollection = db.getCollection(Constants.MONGO_IDVAL_COLLECTION);
				eidValCollection = db.getCollection(Constants.MONGO_EIDVAL_COLLECTION);
				wrappedIDCollection = db.getCollection(
								Constants.MONGO_WRAPPED_ID_COLLECTION);
				
				long shardCount = Long.parseLong(conf.get("count"));
				for(long i=0; i<shardCount; i++) {
					String hostPortStr = conf.get(Long.toString(i));
					hostPort = hostPortStr.split(":");
					Mongo mongo = new Mongo(hostPort[0], Integer.parseInt(hostPort[1]));
					shardMongos.add(mongo);
					DB rdfDB = mongo.getDB(Constants.MONGO_RDF_DB);
					DBCollection shardCollection = rdfDB.getCollection(
								Constants.MONGO_PARTITIONED_VERTEX_COLLECTION);
					shardMap.put(i, shardCollection);
				}
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void reduce(LongWritable key, Iterator<LongWritable> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// all the Vertices belonging to this partition would be given by
			// the iterator
			DBCollection collectionToUse = shardMap.get(key.get());
			while(values.hasNext()) {
				long wrappedVertexID = values.next().get();
				
				// 1) find the real ID 
				// 2) Get all triples whose subject is same as the real ID
				// 3) Write them to DB
				// 4) Get the string equivalent and write it to file
				
				DBObject realID = wrappedIDCollection.findOne(new BasicDBObject(
										Constants.FIELD_WID, wrappedVertexID));
				long vertexID = (Long)realID.get(Constants.FIELD_ID);
				
				DBCursor resultCursor = tripleCollection.find(new BasicDBObject(
						Constants.FIELD_TRIPLE_SUBJECT, vertexID));
				while(resultCursor.hasNext()) {
					DBObject resultDoc = resultCursor.next();
					long subjectID = (Long)resultDoc.get(
							Constants.FIELD_TRIPLE_SUBJECT);
					long predicateID = (Long)resultDoc.get(
							Constants.FIELD_TRIPLE_PREDICATE);
					long objectID = (Long)resultDoc.get(
							Constants.FIELD_TRIPLE_OBJECT);
					BasicDBObject tripleDoc = new BasicDBObject();
					tripleDoc.put(Constants.FIELD_TRIPLE_SUBJECT, subjectID);
					tripleDoc.put(Constants.FIELD_TRIPLE_PREDICATE, predicateID);
					tripleDoc.put(Constants.FIELD_TRIPLE_OBJECT, objectID);
					collectionToUse.insert(tripleDoc);
					
					DBObject subResultDoc = idValCollection.findOne(
							new BasicDBObject(Constants.FIELD_ID, subjectID));
					String subjectStr = (String)subResultDoc.get(Constants.FIELD_STR_VALUE);
					String predicateStr = null;
					if(predicateID == Long.parseLong(Constants.PREDICATE_TYPE)) 
						predicateStr = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
					else {
						DBObject predResultDoc = eidValCollection.findOne(
							new BasicDBObject(Constants.FIELD_ID, predicateID));
						predicateStr = (String)predResultDoc.get(Constants.FIELD_STR_VALUE);
					}
					DBObject objResultDoc = idValCollection.findOne(
							new BasicDBObject(Constants.FIELD_ID, objectID));
					String objectStr = (String)objResultDoc.get(Constants.FIELD_STR_VALUE);
					multipleOutputs.getCollector("triples" + key.toString(), 
							reporter).collect(new Text(subjectStr + " " + 
									predicateStr + " " + objectStr + " ."), 
														new Text(""));
				}			
			}
		}
		
		@Override
		public void close() throws IOException {
			multipleOutputs.close();
			// close all mongo connections
			for(Mongo mongo : shardMongos)
				mongo.close();
			mongoRouter.close();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			String msg = "Incorrect arguments -- requires 2 arguments.\n\t " +
					"1) directory containing vertex-partition ID pairs \n\t" +
					"2) path to output directory which contains triples \n";
			throw new Exception(msg + this.getClass());
		}

		String triples = args[0];
		String outputDir = args[1];

		Path outputPath = new Path(outputDir);
		Configuration fconf = new Configuration();
		FileSystem fs = FileSystem.get(fconf);

		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
			
		PropertyFileHandler propertyFileHandler = 
			PropertyFileHandler.getInstance();
		
		// phase 1
		JobConf jobConf = new JobConf(this.getClass());
		jobConf.setJobName("PartitionedTripleSplitter");
		
		List<HostInfo> shardsInfo = propertyFileHandler.getAllShardsInfo();
		int shardCount = propertyFileHandler.getShardCount();
		jobConf.set("count", Integer.toString(shardCount));
		for(int i=0; i<shardCount; i++) {
			jobConf.set(Integer.toString(i), shardsInfo.get(i).toString());
			log.info(Integer.toString(i) + " is assigned to " + 
					shardsInfo.get(i).toString());
		}
		
		HostInfo mongosHostPort = propertyFileHandler.getMongoRouterHostInfo();
		jobConf.set("MongosInfo", mongosHostPort.toString());
		
		Path pOutput = new Path(outputDir + "");
		
		FileOutputFormat.setOutputPath(jobConf, pOutput);
		
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);	
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(jobConf, new Path(triples));

		jobConf.setMapperClass(Map.class);
		jobConf.setReducerClass(Reduce.class);
		
		jobConf.setMapOutputKeyClass(LongWritable.class);
		jobConf.setMapOutputValueClass(LongWritable.class);
		
		int numReduceTasks = (int)(0.95 * Integer.parseInt(jobConf.get(
				"mapred.tasktracker.reduce.tasks.maximum")) * 
				shardCount);
		jobConf.setNumReduceTasks(numReduceTasks);
		
		for(int i=0; i<shardCount; i++)
			MultipleOutputs.addNamedOutput(jobConf, "triples" + i, 
					TextOutputFormat.class, Text.class, Text.class);
		
		RunningJob job = JobClient.runJob(jobConf);

		if (!job.isSuccessful()) {
			System.out.println("Hadoop Job Failed");
		}					
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new PartitionedTripleSplitter(), args);
	}
}

/*
 * map.emit(partitionID, vertexID)
 * in reduce all vertices for a partition are collected and
 * inserted into DB and written to nt file, by accessing DB to
 * convert from ID to Str. 
 */
