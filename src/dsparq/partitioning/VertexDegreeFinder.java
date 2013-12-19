package dsparq.partitioning;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
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
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

import dsparq.misc.Constants;

/**
 * This class is used to find the degree of each type vertex as well as
 * the total degree and total vertices of those vertices, which can be used
 * to find the average degree. 
 * 
 * Example of type vertex: In the triple, 
 * Steve_Jobs <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> Person
 * Steve_Jobs is a type vertex.
 * 
 * @author Raghava
 *
 */
public class VertexDegreeFinder extends Configured implements Tool {
	
	static enum VertexInfo {
		TOTAL_TYPE_DEGREE,
		TOTAL_TYPE_VERTICES
	}

	private static class Map extends MapReduceBase implements 
			Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key, Text value, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			
			// expected input format: subject|predicate|object|bool
			//						  bool can true or false
			
			String line = key.toString();	        
	        String[] tokenizer = line.split(Constants.REGEX_DELIMITER);
	        // undirected graph, so consider edges in both directions
	        output.collect(new Text(tokenizer[Constants.POSITION_SUBJECT]), 
	        		new Text(tokenizer[Constants.POSITION_PREDICATE]));
	        output.collect(new Text(tokenizer[Constants.POSITION_OBJECT]), 
	        		new Text(tokenizer[Constants.POSITION_PREDICATE]));
		}
	}
	
	private static class Reduce extends MapReduceBase implements 
			Reducer<Text, Text, Text, IntWritable> {
	
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			int degreeCount = 0;
			boolean isTypeTriple = false;
			while(values.hasNext()) {
				if(values.next().toString().equals(Constants.PREDICATE_TYPE))
					isTypeTriple = true;
				degreeCount++;
			}
			// if type triple then these are its in & out degrees
			if(isTypeTriple) {
				reporter.getCounter(VertexInfo.TOTAL_TYPE_DEGREE).increment(degreeCount);
				reporter.getCounter(VertexInfo.TOTAL_TYPE_VERTICES).increment(1);
				output.collect(key, new IntWritable(degreeCount));
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			String msg = "Incorrect arguments -- requires 2 arguments.\n\t " +
					"1) Formatted Triples file \n\t" +
					"2) path to output directory which would hold " +
						"the type vertex-degree pairs \n\t";
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
		conf1.setJobName("VertexDegreeFinder");
		
		Path p1Output = new Path(outputDir + "");
		
		FileOutputFormat.setOutputPath(conf1, p1Output);
		
		conf1.setInputFormat(KeyValueTextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);	
		
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(conf1, new Path(triples));

		conf1.setMapperClass(Map.class);
		conf1.setReducerClass(Reduce.class);
		
		RunningJob job = JobClient.runJob(conf1);

		if (job.isSuccessful()) {
			Counters counters = job.getCounters();
			Counter totalVertices = counters.findCounter(VertexInfo.TOTAL_TYPE_VERTICES);
			Counter totalDegree = counters.findCounter(VertexInfo.TOTAL_TYPE_DEGREE);
			
			Mongo mongo = new Mongo(Constants.MONGO_LOCAL_HOST, Constants.MONGO_PORT);
			DB db = mongo.getDB(Constants.MONGO_DB);
			DBCollection graphInfoCollection = db.getCollection(
									Constants.MONGO_GRAPHINFO_COLLECTION);
			
			graphInfoCollection.drop();
			BasicDBObject graphInfoDocument = new BasicDBObject();
			graphInfoDocument.put(Constants.TOTAL_VERTICES, totalVertices.getValue());
			graphInfoDocument.put(Constants.TOTAL_DEGREE, totalDegree.getValue());
			graphInfoCollection.insert(graphInfoDocument);
			
			mongo.close();
			
			System.out.println("Put total vertices and total degree values in DB");
			System.out.println("Avg vertex degree: " + 
					Math.round(((double)totalDegree.getValue()/totalVertices.getValue())));
		} else {
			System.out.println("FAILED!!!");
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new VertexDegreeFinder(), args);
	}
}
