package dsparq.query;

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

public class QueryJoiner extends Configured implements Tool {

	private String coreVertex;
	
	public QueryJoiner(String coreVertex) {
		this.coreVertex = coreVertex;
	}
	
	private class Map extends MapReduceBase implements 
			Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key, Text value, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			
			//TODO: what would be the format of output?
			// for now, assume that key would be the coreVertex
			if(key.toString().equals(coreVertex))
				output.collect(key, value);
		}
	}
	
	private class Reduce extends MapReduceBase implements 
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
			//TODO: format should be the same as the input format - so that
			// 		this MR job can be run iteratively.
			
			while(values.hasNext()) {
				output.collect(values.next(), new Text(""));
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length == 0) {
			String msg = "Incorrect arguments -- specify at least 3 arguments.\n\t" +
					"1) Input directory \n\t" +
					"2) Output directory \n\t" +
					"3) core vertex (or more than one) \n\t";
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
			
		// phase 1
		JobConf conf1 = new JobConf(this.getClass());
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
		
		RunningJob job1 = JobClient.runJob(conf1);

		if (job1.isSuccessful()) {

		} else {
			System.out.println("FAILED!!!");
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		// TODO: find a way to get the core vertices -- may be use DB
		// and need to iterate over the core vertices -- as many
		// joins as the number of core vertices.
		String testCoreVertex = "?club";
		ToolRunner.run(new Configuration(), new QueryJoiner(testCoreVertex), args);
	}}
