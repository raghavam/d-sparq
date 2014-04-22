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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import dsparq.misc.Constants;

public class UndirectedGraphPartitioner2 extends Configured implements Tool {

	private static class Map extends MapReduceBase implements 
			Mapper<Text, Text, LongWritable, LongWritable> {

		@Override
		public void map(Text key, Text value, 
				OutputCollector<LongWritable, LongWritable> output,
				Reporter reporter) throws IOException {
			
			String line = key.toString();	        
	        String[] tokenizer = line.split(Constants.REGEX_DELIMITER);
	        // undirected graph, so consider edges in both directions
	        output.collect(new LongWritable(Long.parseLong(tokenizer[Constants.POSITION_SUBJECT])), 
	        		new LongWritable(Long.parseLong(tokenizer[Constants.POSITION_OBJECT])));
	        output.collect(new LongWritable(Long.parseLong(tokenizer[Constants.POSITION_OBJECT])), 
	        		new LongWritable(Long.parseLong(tokenizer[Constants.POSITION_SUBJECT])));
		}
	}
	
	private static class Reduce extends MapReduceBase implements 
			Reducer<LongWritable, LongWritable, LongWritable, Text> {
		
		@Override
		public void reduce(LongWritable key, Iterator<LongWritable> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			
			Set<Long> adjVertices = new HashSet<Long>();
			StringBuilder adjVertexStr = new StringBuilder();
			// check whether there are any duplicates in values
			while(values.hasNext()) {
				long s = values.next().get();
				if(!adjVertices.contains(s)) {
					adjVertices.add(s);
					adjVertexStr.append(s).append(Constants.TRIPLE_TERM_DELIMITER);
				}
			}
			adjVertexStr.deleteCharAt(adjVertexStr.length()-1);
			output.collect(key, new Text(adjVertexStr.toString()));
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
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
			
		// phase 1
		JobConf conf1 = new JobConf(this.getClass());
		conf1.setJobName("UndirectedGraphPartitioner2");
		
		Path p1Output = new Path(outputDir + "");
		
		FileOutputFormat.setOutputPath(conf1, p1Output);
		
		conf1.setInputFormat(KeyValueTextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);	
		
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(conf1, new Path(triples));

		conf1.setMapperClass(Map.class);
		conf1.setReducerClass(Reduce.class);
		conf1.setMapOutputKeyClass(LongWritable.class);
		conf1.setMapOutputValueClass(LongWritable.class);
		
		RunningJob job1 = JobClient.runJob(conf1);

		if (job1.isSuccessful()) {

		} else {
			System.out.println("FAILED!!!");
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new UndirectedGraphPartitioner2(), args);
	}

}
