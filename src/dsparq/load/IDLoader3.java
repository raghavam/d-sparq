package dsparq.load;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import org.semanticweb.yars.nx.Literal;
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
 * Does dictionary for each term in the triple using
 * MapReduce + Redis. Also creates a SHA-1 digest message
 * for each string. 
 * 
 * @author Raghava
 *
 */
public class IDLoader3 extends Configured implements Tool {

//	private static final Logger log = Logger.getLogger(IDLoader3.class);
	
	private static class Map extends MapReduceBase implements 
	Mapper<Text, Text, Text, LongWritable> {

		@Override
		public void map(Text key, Text value, 
				OutputCollector<Text, LongWritable> output,
				Reporter reporter) throws IOException {
			
			// expected input format: subject predicate object
			
			StringReader reader = new StringReader(key.toString());
			NxParser nxParser = new NxParser(reader);
			Node[] nodes = nxParser.next();			
			output.collect(new Text(nodes[0].toString()), 
					new LongWritable(1));
			output.collect(new Text(nodes[1].toString()), 
					new LongWritable(1));
			if(nodes[1].toString().equals(
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
				output.collect(new Text(nodes[2].toString()), 
					new LongWritable(-1));
			}
			else {
				if(nodes[2] instanceof Literal) {
					//make this in the same form as Jena sees it.
					//Eg: "Journal 1 (1940)"^^http://www.w3.org/2001/XMLSchema#string
					Literal literal = (Literal) nodes[2];
					StringBuilder sb = new StringBuilder();
					sb.append("\"").append(literal.getData()).append("\"");
					sb.append("^^").append(literal.getDatatype().toString());
					output.collect(new Text(sb.toString()), 
							new LongWritable(1));
				}
				else
					output.collect(new Text(nodes[2].toString()), 
							new LongWritable(1));
			}
			reader.close();
		}
	}
	
	private static class Reduce extends MapReduceBase implements 
	Reducer<Text, LongWritable, Text, Text> {
		
		private ShardedJedis shardedJedis;
		private Jedis idStore;
		private LRUCache<String, Long> dictionary;
		private String insertionScript;
		
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
				// can pipelining be done?
			}
			shardedJedis = new ShardedJedis(shards, Hashing.MURMUR_HASH);
			idStore = new Jedis("nimbus2", 6379);
			idStore.connect();
			dictionary = new LRUCache<String, Long>(300000);
			insertionScript = 
				"local termID = tonumber(KEYS[1]) " +
				"local numericID " +
				"if(termID == 1) then " +
					"numericID = redis.call('INCR', 'subCount') " +
				"elseif(termID == -1) then " +
					"numericID = redis.call('DECR', 'typeCount') " +
				"end " +
				"return numericID ";
		}
	
		@Override
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			try {
				
				String digestValue = Util.generateMessageDigest(key.toString());
				long termID = values.next().get();
				if(termID == -1) {
					while(values.hasNext()) {
						termID = values.next().get();
						if(termID == 1)
							break;
					}
				}
				// check locally whether digestValue has an ID assigned; if not
				// contact Redis - (increment & insert ID) and get the new ID
				List<String> keys = new ArrayList<String>();
				keys.add(digestValue);
				if(termID == 1) {
					if(!dictionary.containsKey(digestValue)) {
						Long numericID = fetchNumericID(digestValue, 
								termID, key.toString());
						dictionary.put(digestValue, numericID);
					}
				}
				else if(termID == -1) {
					if(!dictionary.containsKey(digestValue)) {
						Long numericID = fetchNumericID(digestValue, 
								termID, key.toString());
						dictionary.put(digestValue, numericID);
					}
				}
				else
					throw new Exception("Unknown TermID: " + termID);
			}
			catch(IOException e) { throw e; }
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		private Long fetchNumericID(String digestValue, long termID, String key) {
			String strID = shardedJedis.get(digestValue);
			Long numericID;
			if(strID == null) {
				numericID = (Long) idStore.eval(insertionScript, 1,
						String.valueOf(termID));
//				java.util.Map<String, String> map = new HashMap<String, String>();
//				map.put("id", String.valueOf(numericID));
//				map.put("str", key);
				Jedis jedis = shardedJedis.getShard(digestValue);
				jedis.set(digestValue, numericID.toString());
				jedis.zadd("keys", 1.0, digestValue);
			}
			else
				numericID = Long.parseLong(strID);
			return numericID;
		}
		
		@Override
		public void close() throws IOException {
			shardedJedis.disconnect();
			idStore.disconnect();
			dictionary.clear();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		String triples = args[0];
		PropertyFileHandler propertyFileHandler = 
			PropertyFileHandler.getInstance();
		JobConf jobConf = new JobConf(this.getClass());
		jobConf.setJobName("IDLoader3");
		
		if(propertyFileHandler == null)
			System.out.println("prop file handler is null");
		System.out.println("Hosts: " + propertyFileHandler.getRedisHosts());
		jobConf.set("RedisHosts", propertyFileHandler.getRedisHosts());
		FileInputFormat.setInputPaths(jobConf, new Path(triples));
		jobConf.setMapperClass(Map.class);
		jobConf.setReducerClass(Reduce.class);
		jobConf.setMapOutputKeyClass(Text.class);
	    jobConf.setMapOutputValueClass(LongWritable.class);
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
		ToolRunner.run(new Configuration(), new IDLoader3(), args);
	}
}
