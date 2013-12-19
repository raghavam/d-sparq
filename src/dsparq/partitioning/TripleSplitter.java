package dsparq.partitioning;

import java.io.IOException;
import java.util.Iterator;

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

import dsparq.misc.PropertyFileHandler;

public class TripleSplitter extends Configured implements Tool {

	private static final Logger log = Logger.getLogger(
										TripleSplitter.class);
			
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
			Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		
		private MultipleOutputs multipleOutputs;
		
		@Override
		public void configure(JobConf conf) {
			multipleOutputs = new MultipleOutputs(conf);
		}
		
		@Override
		public void reduce(LongWritable key, Iterator<LongWritable> values,
				OutputCollector<LongWritable, LongWritable> output, Reporter reporter)
				throws IOException {
			// all the Vertices belonging to this partition would be given by
			// the iterator
			while(values.hasNext()) {
				long vertexID = values.next().get();
				multipleOutputs.getCollector("triples" + key.toString(), 
						reporter).collect(new LongWritable(vertexID), new LongWritable());
						
			}
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
					"1) directory containing vertex-partition ID pairs \n\t" +
					"2) path to output directory which contains triples \n";
			throw new Exception(msg + this.getClass());
		}

		String vertexPartitions = args[0];
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
		jobConf.setJobName("TripleSplitter");
		
		Path pOutput = new Path(outputDir);
		
		FileOutputFormat.setOutputPath(jobConf, pOutput);
		
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);	
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(jobConf, new Path(vertexPartitions));

		jobConf.setMapperClass(Map.class);
		jobConf.setReducerClass(Reduce.class);
		
		jobConf.setMapOutputKeyClass(LongWritable.class);
		jobConf.setMapOutputValueClass(LongWritable.class);
		
		int shardCount = propertyFileHandler.getShardCount();
		int numReduceTasks = (int)(0.95 * Integer.parseInt(jobConf.get(
				"mapred.tasktracker.reduce.tasks.maximum")) * 
				shardCount);
		jobConf.setNumReduceTasks(numReduceTasks);
		
		for(int i=0; i<shardCount; i++)
			MultipleOutputs.addNamedOutput(jobConf, "triples" + i, 
				TextOutputFormat.class, LongWritable.class, LongWritable.class);
		
		RunningJob job = JobClient.runJob(jobConf);

		if (!job.isSuccessful()) {
			System.out.println("Hadoop Job Failed");
		}					
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new TripleSplitter(), args);
	}
}
