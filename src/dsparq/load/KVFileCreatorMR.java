package dsparq.load;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.util.Hashing;
import dsparq.misc.Constants;
import dsparq.misc.PropertyFileHandler;
import dsparq.util.LRUCache;
import dsparq.util.Util;

/**
 * Takes triples as input and puts out triples in 
 * Subject|Predicate|Object format where Subject/Predicate/Object
 * are in their equivalent IDs. This format is suitable for MR programs.
 * 
 * @author Raghava
 */
public class KVFileCreatorMR extends Configured implements Tool {

	private static class Map extends MapReduceBase implements 
	Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key, Text value, 
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			
			// expected input format: subject predicate object
			
			StringReader reader = new StringReader(key.toString());
			NxParser nxParser = new NxParser(reader);
			Node[] nodes = nxParser.next();		
			
			String object = null;
			if(nodes[2] instanceof Literal) {
				//make this in the same form as Jena sees it.
				//Eg: "Journal 1 (1940)"^^http://www.w3.org/2001/XMLSchema#string
				Literal literal = (Literal) nodes[2];
				StringBuilder sb = new StringBuilder();
				sb.append("\"").append(literal.getData()).append("\"");
				sb.append("^^").append(literal.getDatatype().toString());
				object = sb.toString();
			}
			else
				object = nodes[Constants.POSITION_OBJECT].toString();
			output.collect(new Text(nodes[Constants.POSITION_SUBJECT].toString()), 
					new Text(nodes[Constants.POSITION_PREDICATE].toString() +
					         Constants.TRIPLE_TERM_DELIMITER + object));
			
			reader.close();
		}
	}
	
	private static class Reduce extends MapReduceBase implements 
	Reducer<Text, Text, Text, NullWritable> {
		
		private Mongo mongo; 
		private DBCollection idValCollection;
		private LRUCache<String, String> dictionary;
		
		@Override
		public void configure(JobConf conf) {
			dictionary = new LRUCache<String, String>(100000);
			String mongoRouter = conf.get("mongo.router");
			String[] hostPort = mongoRouter.split(":");
			try {
				mongo = new MongoClient(hostPort[0], 
						Integer.parseInt(hostPort[1]));
				DB db = mongo.getDB(Constants.MONGO_RDF_DB);
				idValCollection = db.getCollection(
						Constants.MONGO_IDVAL_COLLECTION);
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			try {
				String subDigest = Util.generateMessageDigest(key.toString());
				String subID;
				if(dictionary.containsKey(subDigest))
					subID = dictionary.get(subDigest);
				else {
					subID = getFromDB(subDigest, key.toString());
					dictionary.put(subDigest, subID);
				}
				if(subID == null)
					throw new Exception("subID is null: " + key.toString());
				while(values.hasNext()) {
					String[] predObj = 
						values.next().toString().split(Constants.REGEX_DELIMITER);
					String predDigest = Util.generateMessageDigest(predObj[0]);
					String predID;
					if(dictionary.containsKey(predDigest))
						predID = dictionary.get(predDigest);
					else {
						predID = getFromDB(predDigest, predObj[0]);
						dictionary.put(predDigest, predID);
					}
					if(predID == null)
						throw new Exception("predID is null: " + predObj[0]);
					String objDigest = Util.generateMessageDigest(predObj[1]);
					String objID;
					if(dictionary.containsKey(objDigest))
						objID = dictionary.get(objDigest);
					else {
						objID = getFromDB(objDigest, predObj[1]);
						dictionary.put(objDigest, objID);
					}
					if(objID == null)
						throw new Exception("objID is null: " + predObj[1]);
					output.collect(new Text(subID + Constants.TRIPLE_TERM_DELIMITER + 
							predID + Constants.TRIPLE_TERM_DELIMITER + objID), null);
				}
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void close() throws IOException {
			mongo.close();
			dictionary.clear();
		}
		
		private String getFromDB(String digestValue, String term) throws Exception {
			BasicDBObject queryDoc = new BasicDBObject();
			queryDoc.put(Constants.FIELD_HASH_VALUE, digestValue);
			BasicDBObject projectionDoc = new BasicDBObject();
			projectionDoc.put(Constants.FIELD_NUMID, 1);
			DBObject resultDoc = idValCollection.findOne(queryDoc, 
					projectionDoc);
			if(resultDoc == null)
				throw new Exception("ID not present: " + digestValue + "  " + term);
			Object numID = resultDoc.get(Constants.FIELD_NUMID);
			if(numID == null)
				throw new Exception("numID is null for " + digestValue);
			return numID.toString();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
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
		JobConf jobConf = new JobConf(this.getClass());
		jobConf.setJobName("KVFileCreatorMR");
		
		if(propertyFileHandler == null)
			System.out.println("prop file handler is null");
		jobConf.set("mongo.router", propertyFileHandler.getMongoRouter());
		FileInputFormat.setInputPaths(jobConf, new Path(triples));
		FileOutputFormat.setOutputPath(jobConf, outputPath);
		jobConf.setMapperClass(Map.class);
		jobConf.setReducerClass(Reduce.class);
		jobConf.setMapOutputKeyClass(Text.class);
	    jobConf.setMapOutputValueClass(Text.class);
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);			
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(NullWritable.class);
		
		int numNodes = propertyFileHandler.getShardCount();
		int numReducers = (int)Math.ceil(0.95 * numNodes * 2);
		jobConf.setNumReduceTasks(numReducers);
		RunningJob job = JobClient.runJob(jobConf);
		if(!job.isSuccessful()) {
			System.out.println("FAILED!!!");
			return -1;
		}
		else
			return 0;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			String msg = "Incorrect arguments -- requires 2 arguments.\n\t " +
					"1) NT Triples file \n\t" +
					"2) Output path";
			throw new Exception(msg);
		}
		ToolRunner.run(new KVFileCreatorMR(), args);
	}
}
