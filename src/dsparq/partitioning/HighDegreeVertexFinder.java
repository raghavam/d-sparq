package dsparq.partitioning;

import java.io.IOException;
import java.util.Iterator;

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

import dsparq.misc.Constants;

/**
 * This class takes in triples having high degree vertex as subject
 * and extracts only the high degree vertices out of the triples. This
 * is required for the function isPWOC() later on.
 * 
 * @author Raghava
 *
 */
public class HighDegreeVertexFinder extends Configured implements Tool {

	private static class Map extends MapReduceBase implements 
			Mapper<Text, Text, Text, Text> {
		
		@Override
		public void map(Text key, Text value, 
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			
			// expected input format: subject|predicate|object|bool
			String[] tokens = key.toString().split(Constants.REGEX_DELIMITER);
			output.collect(new Text(tokens[Constants.POSITION_SUBJECT]), new Text(""));
		}
	}
	
	private static class Reduce extends MapReduceBase implements 
			Reducer<Text, Text, Text, Text> {
	
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
			output.collect(key, new Text(""));
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			String msg = "Incorrect arguments -- requires 2 arguments.\n\t " +
					"1) directory containing high degree triples \n\t" +
					"2) path to output directory where high degree vertices would be written \n";
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
			
		JobConf jobConf = new JobConf(this.getClass());
		
		Path pOutput = new Path(outputDir + "");
		
		FileOutputFormat.setOutputPath(jobConf, pOutput);
		
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);	
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(jobConf, new Path(triples));

		jobConf.setMapperClass(Map.class);
		jobConf.setReducerClass(Reduce.class);
		
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
		
		RunningJob job = JobClient.runJob(jobConf);

		if (!job.isSuccessful())
			System.out.println("Hadoop Job Failed");
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new HighDegreeVertexFinder(), args);
	}
}
