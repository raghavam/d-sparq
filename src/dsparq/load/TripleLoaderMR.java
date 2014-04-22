package dsparq.load;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.util.Hashing;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import dsparq.misc.Constants;
import dsparq.misc.PropertyFileHandler;
import dsparq.util.Util;

/**
 * MR task to load triples (in numerical form) to a 
 * single instance of MongoDB - used for stress testing.
 * 
 * @author Raghava
 *
 */
public class TripleLoaderMR extends Configured implements Tool {

	private static final Logger log = Logger.getLogger(TripleLoaderMR.class);
	
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
			StringBuilder predObj = new StringBuilder();
			predObj.append(nodes[1].toString()).
				append(Constants.TRIPLE_TERM_DELIMITER);
			String obj = nodes[2].toString();
			if(nodes[2] instanceof Literal) {
				//make this in the same form as Jena sees it.
				//Eg: "Journal 1 (1940)"^^http://www.w3.org/2001/XMLSchema#string
				Literal literal = (Literal) nodes[2];
				StringBuilder sb = new StringBuilder();
				sb.append("\"").append(literal.getData()).append("\"");
				sb.append("^^").append(literal.getDatatype().toString());
				obj = sb.toString();
			}
			predObj.append(obj);			
			output.collect(new Text(nodes[0].toString()), 
					new Text(predObj.toString()));
			reader.close();
		}
	}
		
	private static class Reduce extends MapReduceBase implements 
		Reducer<Text, Text, Text, Text> {

		private ShardedJedis shardedJedis;
		private Mongo localMongo;
		private DBCollection starSchemaCollection;
		
		@Override
		public void configure(JobConf conf) {
			String redisHosts = conf.get("RedisHosts");
			String[] hostPorts = redisHosts.split(",");
			List<JedisShardInfo> shards = 
				new ArrayList<JedisShardInfo>(hostPorts.length);
			for(String hostPort : hostPorts) {
				String[] hp = hostPort.split(":");
				shards.add(new JedisShardInfo(hp[0], Integer.parseInt(hp[1]), 
						Constants.INFINITE_TIMEOUT));
			}
			shardedJedis = new ShardedJedis(shards, Hashing.MURMUR_HASH);
			try {
				localMongo = new MongoClient("nimbus5", 10000);
				DB localDB = localMongo.getDB(Constants.MONGO_RDF_DB);
				starSchemaCollection = localDB.getCollection(Constants.MONGO_STAR_SCHEMA);
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			/*	MR failed tasks are re-run. When they are rerun 
				perhaps the same document is inserted twice into MongoDB.
				To avoid this, make subjectID as _id of the document and let it
				fail on insertion -- handle exception and move on?
			*/
			try {
				String subDigestValue = Util.generateMessageDigest(key.toString());
				ShardedJedisPipeline pipeline = shardedJedis.pipelined();
				Response<String> subID = pipeline.get(subDigestValue);
				List<BasicDBObject> predObjList = new ArrayList<BasicDBObject>();
				List<Response<String>> propIDResponseList = 
						new ArrayList<Response<String>>();
				List<Response<String>> objIDResponseList = 
						new ArrayList<Response<String>>();
				while(values.hasNext()) {
					String[] predObj = values.next().toString().split(
										Constants.REGEX_DELIMITER);
					String propDigestValue = Util.generateMessageDigest(predObj[0]);
					propIDResponseList.add(pipeline.get(propDigestValue));
					objIDResponseList.add(pipeline.get(
							Util.generateMessageDigest(predObj[1])));
				}
				pipeline.sync();
				for(int i=0; i<propIDResponseList.size(); i++) {
					String predID = propIDResponseList.get(i).get();
					String objID = objIDResponseList.get(i).get();
					BasicDBObject predObjDoc = new BasicDBObject();
					predObjDoc.put(Constants.FIELD_TRIPLE_PREDICATE, 
									Long.parseLong(predID));
					predObjDoc.put(Constants.FIELD_TRIPLE_OBJECT, 
									Long.parseLong(objID));
					predObjList.add(predObjDoc);
				}				
				DBObject doc = new BasicDBObject();
				doc.put(Constants.FIELD_TRIPLE_SUBJECT, Long.parseLong(subID.get()));
				doc.put(Constants.FIELD_TRIPLE_PRED_OBJ, predObjList);
				starSchemaCollection.insert(doc);				
			}
			catch(Exception e) {
				log.error(e.getMessage());
				e.printStackTrace();
			}
		}
		
		@Override
		public void close() throws IOException {
			shardedJedis.disconnect();
			localMongo.close();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		String triples = args[0];
		PropertyFileHandler propertyFileHandler = 
			PropertyFileHandler.getInstance();
		JobConf jobConf = new JobConf(this.getClass());
		jobConf.setJobName("TripleLoaderMR");
		
		jobConf.set("RedisHosts", propertyFileHandler.getRedisHosts());
		FileInputFormat.setInputPaths(jobConf, new Path(triples));
		jobConf.setMapperClass(Map.class);
		jobConf.setReducerClass(Reduce.class);
		jobConf.setMapOutputKeyClass(Text.class);
	    jobConf.setMapOutputValueClass(Text.class);
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(NullOutputFormat.class);
		int numReduceTasks = (int)(0.95 * Integer.parseInt(jobConf.get(
				"mapred.tasktracker.reduce.tasks.maximum")) * 14);
		jobConf.setNumReduceTasks(numReduceTasks);
		RunningJob job = JobClient.runJob(jobConf);
		if(!job.isSuccessful()) {
			System.out.println("FAILED!!!");
			return -1;
		}
		else
			return 0;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			String msg = "Incorrect arguments -- requires 1 argument.\n\t " +
					"1) NT Triples file \n\t";
			throw new Exception(msg);
		}
		ToolRunner.run(new Configuration(), new TripleLoaderMR(), args);
	}
}
