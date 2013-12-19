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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import dsparq.misc.Constants;


/**
 * This class is used to separate high degree vertices from the 
 * triples. Reads in the file containing triples and (vertexID, degree)
 * pairs (of rdf:type) as inputs and filters vertices on that basis. 
 * Avg. degree and standard deviation of vertices are available from DB.
 * 
 * @author Raghava
 *
 */
public class HighDegreeTripleSeparator extends Configured implements Tool {

	private static class Map extends MapReduceBase implements 
			Mapper<Text, Text, Text, Text> {

		private long degreeThreshold;
		
		@Override
		public void configure(JobConf jobConf) {
			degreeThreshold = Long.parseLong(jobConf.get(Constants.DEGREE_THRESHOLD));
		}
		
		@Override
		public void map(Text key, Text value, 
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			
			// input could be either a triple or a (vertexID, degree) pair
			String input = key.toString();
			String[] tokens = input.split(Constants.REGEX_DELIMITER);
			if(tokens.length == 1) {
				// this is (vertexID, degree) pair
				long vertexDegree = Long.parseLong(value.toString());
				String degreeType = (vertexDegree >= degreeThreshold) ? "high" : "low";
				output.collect(key, new Text(degreeType));
			}
			else {
				// this is a triple
				
				// both the in & out edges of the high degree type vertex should
				// be considered
				output.collect(new Text(tokens[Constants.POSITION_SUBJECT]), key);
				output.collect(new Text(tokens[Constants.POSITION_OBJECT]), key);
			}
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
			
			int degreeType = 0;
			List<String> triples = new ArrayList<String>();
			while(values.hasNext()) {
				String value = values.next().toString();
				if(value.equals("high"))
					degreeType = 1;
				else if(value.equals("low"))
					degreeType = 2;
				else
					triples.add(value);
			}
			// if degreeType is 0, then these are not triples related to any type
			if(degreeType == 1) {
				for(String t : triples)
					multipleOutputs.getCollector(Constants.HIGH_DEGREE_TAG, reporter).
							collect(new Text(t), new Text(""));
				multipleOutputs.getCollector(Constants.HIGH_DEGREE_TYPE, reporter).
				collect(key, new Text(""));
			}
			else if(degreeType == 2) {
				for(String t : triples)
					multipleOutputs.getCollector(Constants.LOW_DEGREE_TAG, reporter).
							collect(new Text(t), new Text(""));
			}
		}
		
		@Override
		public void close() throws IOException {
			multipleOutputs.close();
		}
}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 3 && args.length != 4) {
			String msg = "Incorrect arguments -- requires 3 or 4 arguments.\n\t " +
					"1) directory containing input triples (typed triples removed) \n\t" +
					"2) directory containing typed vertex-degree pairs \n\t" +
					"3) output directory \n\t" +
					"4) degree threshold (optional) \n";
			throw new RuntimeException(msg + this.getClass());
		}

		String triples = args[0];
		String vertexDegreePairs = args[1];
		String outputDir = args[2];
		long degreeThreshold;
		
		if(args.length == 4)
			degreeThreshold = Long.parseLong(args[3]);
		else {
			Mongo mongo = new Mongo(Constants.MONGO_LOCAL_HOST, Constants.MONGO_PORT);
			DB db = mongo.getDB(Constants.MONGO_DB);
			DBCollection graphInfoCollection = db.getCollection(
					Constants.MONGO_GRAPHINFO_COLLECTION);
			BasicDBObject queryDoc = new BasicDBObject();
			queryDoc.put(Constants.TOTAL_VERTICES, new BasicDBObject("$gte", 1));
			DBObject resultDoc = graphInfoCollection.findOne(queryDoc);
			long totalVertices = (Long) resultDoc.get(Constants.TOTAL_VERTICES);
			long totalDegree = (Long) resultDoc.get(Constants.TOTAL_DEGREE);
			long stdDev = (Long) resultDoc.get(Constants.STD_DEV_DEGREE);
			long avgDegree = Math.round((double)totalDegree/totalVertices);
			// this is the default degree threshold (from the paper)
			degreeThreshold = avgDegree + (3 * stdDev);
		}

		Path outputPath = new Path(outputDir);
		Configuration fconf = new Configuration();
		FileSystem fs = FileSystem.get(fconf);

		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
			
		// phase 1
		JobConf jobConf = new JobConf(this.getClass());
		jobConf.setJobName("HighDegreeTripleSeparator");
		jobConf.setLong(Constants.DEGREE_THRESHOLD, degreeThreshold);
		
		Path p1Output = new Path(outputDir + "");
		
		FileOutputFormat.setOutputPath(jobConf, p1Output);
		
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);	
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(jobConf, new Path(triples), 
				new Path(vertexDegreePairs));

		jobConf.setMapperClass(Map.class);
		jobConf.setReducerClass(Reduce.class);
		
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
		
		MultipleOutputs.addNamedOutput(jobConf, Constants.HIGH_DEGREE_TAG, 
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(jobConf, Constants.LOW_DEGREE_TAG, 
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(jobConf, Constants.HIGH_DEGREE_TYPE, 
				TextOutputFormat.class, Text.class, Text.class);
		
		RunningJob job = JobClient.runJob(jobConf);

		if (!job.isSuccessful())
			System.out.println("Hadoop Job Failed");
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new HighDegreeTripleSeparator(), args);
	}
}
