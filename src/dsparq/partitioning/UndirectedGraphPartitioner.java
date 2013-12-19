package dsparq.partitioning;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import dsparq.misc.Constants;

/**
 * This class is used to generate input file for METIS
 * 
 * @author Raghava
 *
 */
public class UndirectedGraphPartitioner extends Configured implements Tool {

	private static final Logger log = Logger.getLogger(UndirectedGraphPartitioner.class);  
	
	private static class Map extends MapReduceBase implements 
			Mapper<Text, Text, LongWritable, Text> {

		@Override
		public void map(Text key, Text value, 
				OutputCollector<LongWritable, Text> output,
				Reporter reporter) throws IOException {
			
			String line = key.toString();	        
	        String[] tokenizer = line.split(Constants.REGEX_DELIMITER);
	        // undirected graph, so consider edges in both directions
	        output.collect(new LongWritable(Long.parseLong(tokenizer[Constants.POSITION_SUBJECT])), 
	        		new Text(tokenizer[Constants.POSITION_OBJECT]));
	        output.collect(new LongWritable(Long.parseLong(tokenizer[Constants.POSITION_OBJECT])), 
	        		new Text(tokenizer[Constants.POSITION_SUBJECT]));
		}
	}
	
	private static class Combiner extends MapReduceBase implements
			Reducer<LongWritable, Text, LongWritable, Text> {

		@Override
		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			Set<String> adjVertices = new HashSet<String>();
			StringBuilder adjVertexStr = new StringBuilder();
			// check whether there are any duplicates in values
			while(values.hasNext()) {
				String s = values.next().toString();
				if(!adjVertices.contains(s)) {
					adjVertices.add(s);
					adjVertexStr.append(s).append(" ");
				}
			}
			output.collect(key, new Text(adjVertexStr.toString()));
		}	
	}
	
	private static class Reduce extends MapReduceBase implements 
			Reducer<LongWritable, Text, Text, Text> {

		private MultipleOutputs multipleOutputs;
		
		@Override
		public void configure(JobConf conf) {
			multipleOutputs = new MultipleOutputs(conf);
		}
		
		@Override
		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
			// due to the way in which triples file in integer form
			// is generated, all the triples with the same subject would
			// occur side-by-side and would be filtered for duplicates 
			// in the combiner
			StringBuilder adjVertexStr = new StringBuilder();
			reporter.progress();
			long itcount= 0;
			// check whether there are any duplicates in values
			while(values.hasNext()) {
				adjVertexStr.append(values.next().toString()).append(" ");
				itcount++;
			}
			
			reporter.progress();
			log.info("Size of iterator: " + itcount);
//			output.collect(key, new Text(adjVertexStr.toString()));
			multipleOutputs.getCollector("vertex", reporter).collect(key, new Text(""));
			multipleOutputs.getCollector("adjvertex", reporter).collect(adjVertexStr, new Text(""));
		}
		
		@Override
		public void close() throws IOException {
			multipleOutputs.close();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			String msg = "Incorrect arguments -- requires 2 arguments.\n\t " +
					"1) Formatted Triples file \n\t" +
					"2) path to output directory which would hold " +
						"the input file for METIS \n\t";
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
			
		Configuration config = new Configuration();
		// doesn't work
//		log.info("mapred.reduce.child.java.opts: " + 
//				config.get("mapred.reduce.child.java.opts"));
		log.info("mapred.child.java.opts: " + config.get("mapred.child.java.opts"));
		config.set("mapred.reduce.child.java.opts", "-Xmx512M");
		config.set("mapred.child.java.opts", "-Xmx512M");
		JobConf conf1 = new JobConf(config, this.getClass());
		conf1.setJobName("UndirectedGraphPartitioner");
		
		Path p1Output = new Path(outputDir + "");
		
		FileOutputFormat.setOutputPath(conf1, p1Output);
		
		conf1.setInputFormat(KeyValueTextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);	
		
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(conf1, new Path(triples));

		conf1.setMapperClass(Map.class);
		conf1.setReducerClass(Reduce.class);
//		conf1.setCombinerClass(Combiner.class);
		conf1.setMapOutputKeyClass(LongWritable.class);
		conf1.setMapOutputValueClass(Text.class);
/*		
		PropertyFileHandler propertyFileHandler = 
			PropertyFileHandler.getInstance();
		int partitions = propertyFileHandler.getShardCount();
		conf1.setNumMapTasks(partitions);
*/		
		// setting reduce to 1 so that output would be in one file
		// METIS input order should be same in vertex & adjvertex files.
		conf1.setNumReduceTasks(1);
		
		MultipleOutputs.addNamedOutput(conf1, "vertex", 
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(conf1, "adjvertex", 
				TextOutputFormat.class, Text.class, Text.class);
		
		RunningJob job1 = JobClient.runJob(conf1);

		if (job1.isSuccessful()) {

		} else {
			System.out.println("FAILED!!!");
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new UndirectedGraphPartitioner(), args);
	}
}
