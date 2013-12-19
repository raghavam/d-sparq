package dsparq.partitioning;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Scanner;

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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import dsparq.misc.Constants;

public class StandardDeviationCalculator extends Configured implements Tool {

	private static class Map extends MapReduceBase implements 
			Mapper<Text, Text, Text, LongWritable> {

		private Text sdKey = new Text("StdDev"); 
		private long avgDegree;
		
		@Override
		public void configure(JobConf jobConf) {
			avgDegree = Long.parseLong(jobConf.get("AvgDegree"));
		}
		
		@Override
		public void map(Text key, Text value, 
				OutputCollector<Text, LongWritable> output,
				Reporter reporter) throws IOException {
			
			// expected input format: vertexID degree
			long squareVal = Long.parseLong(value.toString()) - avgDegree;
			output.collect(sdKey, new LongWritable(squareVal * squareVal));
		}
	}
	
	private static class Reduce extends MapReduceBase implements 
			Reducer<Text, LongWritable, Text, LongWritable> {
	
		@Override
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			
			long degreeCount = 0;
			long vertexCount = 0;
			while(values.hasNext()) {
				degreeCount  = degreeCount + values.next().get();
				vertexCount++;
			}
			double stdDev = Math.sqrt((double)degreeCount/vertexCount);
			output.collect(key, new LongWritable(Math.round(stdDev)));
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			String msg = "Incorrect arguments -- requires arguments.\n\t " +
					"1) directory containing vertex-degree pairs \n\t" +
					"2) path to output directory where StdDev would be written \n";
			throw new RuntimeException(msg + this.getClass());
		}

		String triples = args[0];
		String outputDir = args[1];
		
		Mongo mongo = new Mongo(Constants.MONGO_LOCAL_HOST, Constants.MONGO_PORT);
		DB db = mongo.getDB(Constants.MONGO_DB);
		DBCollection graphInfoCollection = db.getCollection(
				Constants.MONGO_GRAPHINFO_COLLECTION);
		BasicDBObject queryDoc = new BasicDBObject();
		queryDoc.put(Constants.TOTAL_VERTICES, new BasicDBObject("$gte", 1));
		DBObject resultDoc = graphInfoCollection.findOne(queryDoc);
		long totalDegree = (Long) resultDoc.get(Constants.TOTAL_DEGREE);
		long totalVertices = (Long) resultDoc.get(Constants.TOTAL_VERTICES);
		
		long avgDegree = Math.round((double)totalDegree/totalVertices);

		Path outputPath = new Path(outputDir);
		Configuration fconf = new Configuration();
		FileSystem fs = FileSystem.get(fconf);

		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
			
		// phase 1
		JobConf conf1 = new JobConf(this.getClass());
		conf1.setJobName("StandardDeviationCalculator");
		conf1.setLong("AvgDegree", avgDegree);
		
		Path p1Output = new Path(outputDir + "");
		
		FileOutputFormat.setOutputPath(conf1, p1Output);
		
		conf1.setInputFormat(KeyValueTextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);	
		
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(conf1, new Path(triples));

		conf1.setMapperClass(Map.class);
		conf1.setReducerClass(Reduce.class);
		
		conf1.setMapOutputKeyClass(Text.class);
		conf1.setMapOutputValueClass(LongWritable.class);
		
		RunningJob job = JobClient.runJob(conf1);

		if (job.isSuccessful()) {
			// read the standard deviation from the file and write it to DB
			
			// hard coding the file name - other way is to extend OutputFormat
			// class and give a custom file name and then use it here.
			File stdDevFile = new File(outputDir + "/part-00000");			
			Scanner scanner = new Scanner(stdDevFile);
			String kv = scanner.nextLine();
			String[] stdDev = kv.split("\\s");
			
			BasicDBObject queryPredicate = new BasicDBObject();
			queryPredicate.put(Constants.TOTAL_VERTICES, new BasicDBObject("$gte", 1));
			BasicDBObject addField = new BasicDBObject();
			addField.put("$set", new BasicDBObject(
					Constants.STD_DEV_DEGREE, Long.parseLong(stdDev[1])));
			graphInfoCollection.update(queryPredicate, addField);	
			
			mongo.close();
			System.out.println("Saved standard deviation to DB");
		}
		else
			System.out.println("Hadoop Job Failed");
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new StandardDeviationCalculator(), args);
	}
}
