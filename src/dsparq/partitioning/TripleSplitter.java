package dsparq.partitioning;

import java.io.IOException;
import java.util.Iterator;

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
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import dsparq.misc.PropertyFileHandler;

/**
 * This MR job, based on the vertexID-partitionID pairs, create separate files 
 * for each partition which contain all vertices belonging to that partition.
 * For example, partition-1 would contain all vertices that have been assigned
 * to that partition by METIS and so on.
 *  
 * @author Raghava Mutharaju
 */
public class TripleSplitter extends Configured implements Tool {

//	private static final Logger log = Logger.getLogger(
//										TripleSplitter.class);
			
	private static class Map extends MapReduceBase implements 
			Mapper<Text, Text, Text, Text> {
		
		@Override
		public void map(Text key, Text value, 
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			// expected input format: vertexID	partitionID
			
			// key is vertexID and value is partitionID
			output.collect(value, key);
		}
	}
	
	private static class Reduce extends MapReduceBase implements 
			Reducer<Text, Text, Text, NullWritable> {
		
		private MultipleOutputs multipleOutputs;
		
		@Override
		public void configure(JobConf conf) {
			multipleOutputs = new MultipleOutputs(conf);
		}
		
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			// all the Vertices belonging to this partition would be given by
			// the iterator
			while(values.hasNext()) {
				multipleOutputs.getCollector("triples" + key.toString(), 
						reporter).collect(values.next(), null);
						
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
		FileInputFormat.setInputPaths(jobConf, new Path(vertexPartitions));
		jobConf.setMapperClass(Map.class);
		jobConf.setReducerClass(Reduce.class);
		
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(NullWritable.class);
		
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
