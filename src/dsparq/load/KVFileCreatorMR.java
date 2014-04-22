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
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import redis.clients.jedis.Jedis;
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
			output.collect(new Text(nodes[Constants.POSITION_SUBJECT].toString()), 
					new Text(nodes[Constants.POSITION_PREDICATE].toString() +
					         Constants.OUTPUT_DELIMITER + 
					         nodes[Constants.POSITION_OBJECT].toString()));
			reader.close();
		}
	}
	
	private static class Reduce extends MapReduceBase implements 
	Reducer<Text, Text, Text, Text> {
		
		private ShardedJedis shardedJedis; 
		private LRUCache<String, String> dictionary;
		
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
			dictionary = new LRUCache<String, String>(100000);
		}
		
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			try {
				String subDigest = Util.generateMessageDigest(key.toString());
				String subID;
				if(dictionary.containsKey(subDigest))
					subID = dictionary.get(subDigest);
				else {
					subID = shardedJedis.get(subDigest);
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
						predID = shardedJedis.get(predDigest);
						dictionary.put(predDigest, predID);
					}
					if(predID == null)
						throw new Exception("predID is null: " + predObj[0]);
					String objDigest = Util.generateMessageDigest(predObj[1]);
					String objID;
					if(dictionary.containsKey(objDigest))
						objID = dictionary.get(objDigest);
					else {
						objID = shardedJedis.get(objDigest);
						dictionary.put(objDigest, objID);
					}
					if(objID == null)
						throw new Exception("objID is null: " + predObj[1]);
					output.collect(new Text(subID + Constants.OUTPUT_DELIMITER + 
							predID + Constants.OUTPUT_DELIMITER + objID), new Text(""));
				}
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void close() throws IOException {
			shardedJedis.disconnect();
			dictionary.clear();
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
		System.out.println("Hosts: " + propertyFileHandler.getRedisHosts());
		jobConf.set("RedisHosts", propertyFileHandler.getRedisHosts());
		FileInputFormat.setInputPaths(jobConf, new Path(triples));
		FileOutputFormat.setOutputPath(jobConf, outputPath);
		jobConf.setMapperClass(Map.class);
		jobConf.setReducerClass(Reduce.class);
		jobConf.setMapOutputKeyClass(Text.class);
	    jobConf.setMapOutputValueClass(Text.class);
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);			
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		
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
		if (args.length != 2) {
			String msg = "Incorrect arguments -- requires 2 arguments.\n\t " +
					"1) NT Triples file \n\t" +
					"2) Output path";
			throw new Exception(msg);
		}
		ToolRunner.run(new Configuration(), new KVFileCreatorMR(), args);
	}
}
