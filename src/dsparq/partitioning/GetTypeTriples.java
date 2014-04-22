
/**
 * Code given by Jiewen Huang - PhD student at 
 * Yale University. Code written for SW-Store.
 * 
 * Code modified by Raghava Mutharaju. 
 */

package dsparq.partitioning;



import java.io.IOException;
import java.util.Map;
import java.util.Set;

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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;


import dsparq.misc.Constants;
import dsparq.misc.PropertyFileHandler;
import dsparq.util.Util;
        
/**
 * This class is used to separate typed triples from the given set
 * of triples.
 * 
 * @author Jiewen Huang
 * Modified by Raghava
 *
 */
public class GetTypeTriples extends Configured implements Tool{
	
//	private static final Logger log = Logger.getLogger(GetTypeTriples.class); 
	
	private static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> 
	{
		private static String predicateTypeID;
		
		@Override
		public void configure(JobConf conf) {
			predicateTypeID = conf.get("typeID");
		}
		
	    public void map(Text key, Text value, OutputCollector<Text, Text> output, 
	    		Reporter reporter) throws IOException 
	    {
/*	    	
	        String line = key.toString();
	        
	        String[] tokenizer = line.split(Constants.REGEX_DELIMITER);
	        
	        StringBuilder triple = new StringBuilder();
	        triple.append(tokenizer[Constants.POSITION_SUBJECT]).append(Constants.TRIPLE_TERM_DELIMITER). 
			append(tokenizer[Constants.POSITION_PREDICATE]).append(Constants.TRIPLE_TERM_DELIMITER).
			append(tokenizer[Constants.POSITION_OBJECT]);
	        
	        if (tokenizer[Constants.POSITION_PREDICATE].
	        		equals(predicateTypeID))	        	
	        	output.collect(new Text(triple.toString()), 
	        			new Text());
	        else
	        	output.collect(new Text(triple.toString()), new Text());
*/
	    	output.collect(key, value);
	    } 
 
		private static class MultiFileOutput extends MultipleTextOutputFormat<Text, Text> {
		     protected String generateFileNameForKeyValue(Text key, 
		    		 Text value, String name) {
		    	 
		    	 String line = key.toString();
		    	 String[] tokenizer = line.split(Constants.REGEX_DELIMITER);
		    	 if (tokenizer[Constants.POSITION_PREDICATE].
		    			 equals(predicateTypeID))	
		    		 return "typed-triples-" + name;
		    	 else
		    		 return "untyped-triples-" + name;
		     }
		}
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new GetTypeTriples(), args);
		System.exit(res);
	}	

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			String msg = "Incorrect arguments -- requires 2 arguments.\n\t " +
					"1) Triples file \n\t" +
					"2) path to output directory which would hold " +
						"rdf:type and non-rdf:type triples \n\t";
			throw new RuntimeException(msg + this.getClass());
		}

		String triples = args[0];
		String outputDir = args[1];

		Path outputPath = new Path(outputDir);
		Configuration fconf = new Configuration();
		FileSystem fs = FileSystem.get(fconf);

		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
			
		JobConf conf1 = new JobConf(this.getClass());
		conf1.setJobName("GetTypeTriples");
		Jedis jedis = new Jedis("nimbus2", 6479);
		jedis.select(1);
		String type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
		String digest = Util.generateMessageDigest(type);
		String typeID = jedis.hget(digest, "id");
		jedis.disconnect();
		conf1.set("typeID", typeID);
		
		Path p1Output = new Path(outputDir + "");
		
		FileOutputFormat.setOutputPath(conf1, p1Output);
		
		conf1.setInputFormat(KeyValueTextInputFormat.class);
		conf1.setOutputFormat(Map.MultiFileOutput.class);	
		
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(conf1, new Path(triples));

		conf1.setMapperClass(Map.class);
//		conf1.setNumMapTasks(propertyFileHandler.getShardCount());
		conf1.setNumReduceTasks(0);
		
		RunningJob job1 = JobClient.runJob(conf1);

		if (job1.isSuccessful()) {

		} else {
			System.out.println("FAILED!!!");
		}

		return 0;
	}
   
}
