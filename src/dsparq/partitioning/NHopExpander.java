package dsparq.partitioning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import dsparq.misc.Constants;
import dsparq.misc.PropertyFileHandler;


/**
 * For each source (target) vertex of an edge, it includes the corresponding 
 * target (source) in the same partition as that of the source (target) vertex.
 * This is propagated to the adjacent edges for n-hops.
 * 
 * @author Raghava Mutharaju
 */
public class NHopExpander extends Configured implements Tool {
	
	private static final Logger log = Logger.getLogger(
			NHopExpander.class);

	private static class Map extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// input is either a triple in subject|predicate|object or 
			// (VertexID, PartitionID) pair
			String[] tripleFragments = 
					key.toString().split(Constants.REGEX_DELIMITER);
			if(tripleFragments.length == 3) {
				output.collect(new Text(tripleFragments[0]), key);
				output.collect(new Text(tripleFragments[2]), key);
			}
			else if(tripleFragments.length == 1) {
				output.collect(key, value);
			}
			else
				throw new IOException("unexpected triple length: " + key);
		}
	}
	
	
	private static class Reduce extends MapReduceBase implements 
	Reducer<Text, Text, Text, Text> {
		
		private MultipleOutputs multipleOutputs;
		
		@Override
		public void configure(JobConf conf) {
			multipleOutputs = new MultipleOutputs(conf);
		}
		
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<String[]> edgeList = new ArrayList<String[]>();
			List<Text> partitionIDs = new ArrayList<Text>();
			// separate edges and partition ID
			while(values.hasNext()) {
				Text valueT = values.next();
				String value = valueT.toString();
				String[] tripleFragments = 
						value.split(Constants.REGEX_DELIMITER);
				if(tripleFragments.length == 3) 
					edgeList.add(tripleFragments);
				else if(tripleFragments.length == 1)
					partitionIDs.add(new Text(value + "@"));
				else
					throw new IOException("unknown type: " + value);
			}
			log.debug("PartitionIDs len: " + partitionIDs.size());
			String keyStr = key.toString();
			for(String[] edge : edgeList) {
				for(Text partitionID : partitionIDs) {
					multipleOutputs.getCollector("EdgePartitions", reporter).
						collect(edgeStrToKVEdge(edge), partitionID);
					if(edge[0].equals(keyStr)) 
						multipleOutputs.getCollector("VertexPartitions", reporter).
						collect(edge[2], partitionID);
					else if(edge[2].equals(keyStr)) 
						multipleOutputs.getCollector("VertexPartitions", reporter).
						collect(edge[0], partitionID);
				}
			}
		}
		
		private Text edgeStrToKVEdge(String[] edge) {
			StringBuilder kvEdge = new StringBuilder(edge[0]).
					append(Constants.TRIPLE_TERM_DELIMITER).append(edge[1]).
					append(Constants.TRIPLE_TERM_DELIMITER).append(edge[2]);
			return new Text(kvEdge.toString());
		}
		
		@Override
		public void close() throws IOException {
			multipleOutputs.close();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		String triplesAndVertexPartitionsIn = args[0];
		String edgeVeretxPartitionsOut = args[1];

		Path outputPath = new Path(edgeVeretxPartitionsOut);
		Configuration fconf = new Configuration();
		FileSystem fs = FileSystem.get(fconf);

		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
			
		PropertyFileHandler propertyFileHandler = 
			PropertyFileHandler.getInstance();
		
		JobConf jobConf = new JobConf(this.getClass());
		jobConf.setJobName("NHopExpander");
		
		FileOutputFormat.setOutputPath(jobConf, outputPath);
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);			
		FileInputFormat.setInputPaths(jobConf, 
				new Path(triplesAndVertexPartitionsIn));
		jobConf.setMapperClass(Map.class);
		jobConf.setReducerClass(Reduce.class);
		
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		
		int shardCount = propertyFileHandler.getShardCount();
		int numReduceTasks = (int)(0.95 * Integer.parseInt(jobConf.get(
				"mapred.tasktracker.reduce.tasks.maximum")) * 
				shardCount);
		jobConf.setNumReduceTasks(numReduceTasks);
		
		MultipleOutputs.addNamedOutput(jobConf, "EdgePartitions", 
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(jobConf, "VertexPartitions", 
				TextOutputFormat.class, Text.class, Text.class);
		RunningJob job = JobClient.runJob(jobConf);

		if (!job.isSuccessful()) {
			log.error("Hadoop Job Failed");		
			return -1;
		}
		else
			return 0;
	}

	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			String msg = "Incorrect arguments -- requires 2 arguments.\n\t " +
					"1) directory containing triples in key-value format and " +
						"vertex-partition ID pairs \n\t" +
					"2) path to output directory that contains edge-partition " +
						"ID pairs and vertex-partition ID pairs after expansion";
			
			throw new Exception(msg);
		}
		ToolRunner.run(new Configuration(), new NHopExpander(), args);
	}
}
