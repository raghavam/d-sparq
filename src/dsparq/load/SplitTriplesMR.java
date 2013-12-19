package dsparq.load;

import java.io.IOException;

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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SplitTriplesMR extends Configured implements Tool {

//	private static final Logger log = Logger.getLogger(SplitTriplesMR.class); 
	
	private static class Map extends MapReduceBase implements 
			Mapper<Text, Text, Text, Text> {
		
		@Override
		public void map(Text key, Text value, 
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			output.collect(key, value);	
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			String message = "Incorrect arguments -- requires 2 argument.\n\t " +
			"1) directory containing N-triples \n\t" +
			"2) output directory path ";
			throw new Exception(message);
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
		jobConf.setJobName("SplitTriplesMR");
		
		Path pOutput = new Path(outputDir);
		
		FileOutputFormat.setOutputPath(jobConf, pOutput);
		
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);	
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(jobConf, new Path(triples));

		jobConf.setMapperClass(Map.class);
		jobConf.setNumReduceTasks(0);
//		jobConf.setReducerClass(Reduce.class);
		
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
		
		RunningJob job = JobClient.runJob(jobConf);

		if (!job.isSuccessful())
			System.out.println("Hadoop Job Failed");
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new SplitTriplesMR(), args);
	}
}
