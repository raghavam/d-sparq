package dsparq.partitioning;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import dsparq.misc.Constants;
import dsparq.misc.PropertyFileHandler;

public class PartitionedTripleSeparator extends Configured implements Tool {

	private static final Logger log = Logger.getLogger(
										TripleSplitter.class);
			
	private static class Map extends MapReduceBase implements 
			Mapper<Text, Text, LongWritable, Text> {
		
		@Override
		public void map(Text key, Text value, 
				OutputCollector<LongWritable, Text> output,
				Reporter reporter) throws IOException {
			// expected input format: vertexID and
			//						  sub|pred|obj|bool
			
			String[] tokens = key.toString().split(Constants.REGEX_DELIMITER);
			if(tokens.length == 1) {
				// this is vertexID
				output.collect(new LongWritable(Long.parseLong(tokens[0])), new Text("00vid"));
			}
			else {
				// this is a triple
				output.collect(new LongWritable(Long.parseLong(
							tokens[Constants.POSITION_SUBJECT])), key);
			}
		}
	}
	
	private static class Reduce extends MapReduceBase implements 
			Reducer<LongWritable, Text, Text, Text> {
		
		@Override
		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// all the triples belonging to this partition would be given by
			// the iterator
			boolean isValid = false;
			List<Text> tripleList = new ArrayList<Text>();
			while(values.hasNext()) {
				Text triple = values.next();
				if(triple.toString().trim().equals("00vid")) {
					isValid = true;
					continue;
				}
				else {
					if(isValid) {
						if(!tripleList.isEmpty()) {
							for(Text t : tripleList)
								output.collect(t, new Text());
							tripleList.clear();
						}
						else
							output.collect(triple, new Text());
					}
					else
						tripleList.add(triple);
				}
			}
			tripleList.clear();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			String msg = "Incorrect arguments -- requires 2 arguments.\n\t " +
					"1) directory containing vertex-partition ID pairs " +
							"and nt file in KV format (sub|pred|obj) \n\t" +
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
		
		JobConf jobConf = new JobConf(this.getClass());
		jobConf.setJobName("PartitionedTripleSeparator");
		
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
		jobConf.setMapOutputValueClass(Text.class);
		
		int shardCount = propertyFileHandler.getShardCount();
		int numReduceTasks = (int)(0.95 * Integer.parseInt(jobConf.get(
				"mapred.tasktracker.reduce.tasks.maximum")) * 
				shardCount);
		jobConf.setNumReduceTasks(numReduceTasks);
		
		RunningJob job = JobClient.runJob(jobConf);

		if (!job.isSuccessful()) {
			System.out.println("Hadoop Job Failed");
		}					
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new PartitionedTripleSeparator(), args);
	}
}
