package dsparq.partitioning;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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

import dsparq.misc.Constants;
import dsparq.misc.PropertyFileHandler;

public class PartitionFilesJoiner extends Configured implements Tool {

	private static class Map extends MapReduceBase implements 
			Mapper<Text, Text, LongWritable, Text> {

		private MultipleOutputs multipleOutputs;
		
		@Override
		public void configure(JobConf conf) {
			multipleOutputs = new MultipleOutputs(conf);
		}
		
		@Override
		public void map(Text key, Text value, 
				OutputCollector<LongWritable, Text> output,
				Reporter reporter) throws IOException {
			multipleOutputs.getCollector("vertex", reporter).collect(
					new LongWritable(Long.parseLong(key.toString())), new Text(""));
			String[] tokens = value.toString().split(Constants.REGEX_DELIMITER);
			StringBuilder adjVertices = new StringBuilder();
			for(String s : tokens)
				adjVertices.append(s).append(" ");
			multipleOutputs.getCollector("adjvertex", reporter).collect(adjVertices, new Text(""));
		}
		
		@Override
		public void close() throws IOException {
			multipleOutputs.close();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			String msg = "Incorrect arguments -- requires 2 arguments.\n\t " +
					"1) Input folder \n\t" +
					"2) path to output directory which would hold " +
						"the input file for METIS \n\t";
			throw new RuntimeException(msg + this.getClass());
		}

		String inputDir = args[0];
		String outputDir = args[1];

		Path outputPath = new Path(outputDir);
		Path inputPath = new Path(inputDir);
		Configuration fconf = new Configuration();
		FileSystem fs = FileSystem.get(fconf);

		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
			
		JobConf jobConf = new JobConf(this.getClass());
		jobConf.setJobName("PartitionFilesJoiner");
		
		Path p1Output = new Path(outputDir + "");
		
		FileOutputFormat.setOutputPath(jobConf, p1Output);
		
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);	
		
		FileInputFormat.setInputPaths(jobConf, inputPath);

		jobConf.setMapperClass(Map.class);
		jobConf.setNumReduceTasks(0);
		jobConf.setMapOutputKeyClass(LongWritable.class);
		jobConf.setMapOutputValueClass(Text.class);
		
		MultipleOutputs.addNamedOutput(jobConf, "vertex", 
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(jobConf, "adjvertex", 
				TextOutputFormat.class, Text.class, Text.class);
		
		RunningJob job1 = JobClient.runJob(jobConf);

		if (job1.isSuccessful()) {

		} else {
			System.out.println("FAILED!!!");
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new PartitionFilesJoiner(), args);
	}
}
