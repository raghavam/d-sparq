package dsparq.partitioning;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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

import com.google.common.collect.ArrayListMultimap;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import dsparq.misc.Constants;

/**
 * This class is used to find the partition IDs for triples
 * which have been excluded during the partitioning phase (not
 * given to METIS). These are high degree vertices and are placed
 * after the partitioning of rest of the graph is done.
 * 
 * @author Raghava
 *
 */
public class HighDegreeTriplePlacer extends Configured implements Tool {

	private static class Map extends MapReduceBase implements 
			Mapper<Text, Text, Text, Text> {
		
		@Override
		public void map(Text key, Text value, 
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			
			// expected input format: subject|predicate|object|bool
			String[] tokens = key.toString().split(Constants.REGEX_DELIMITER);
			if(tokens[Constants.POSITION_PREDICATE].equals(Constants.PREDICATE_TYPE))
				output.collect(new Text(tokens[Constants.POSITION_OBJECT]), 
								new Text(tokens[Constants.POSITION_SUBJECT] + 
										Constants.TRIPLE_TERM_DELIMITER + 
										Constants.PREDICATE_TYPE));
			else
				output.collect(new Text(tokens[Constants.POSITION_SUBJECT]), 
						new Text(tokens[Constants.POSITION_OBJECT]));
		}
	}
	
	private static class Reduce extends MapReduceBase implements 
			Reducer<Text, Text, Text, Text> {
	
		// read Vertex ID of subject from DB.
		private Mongo mongo;
		private DBCollection dbCollection;

		@Override
		public void configure(JobConf jobConf) {
			System.out.println("Configuring Mongo...");
			try {
				mongo = new Mongo(Constants.MONGO_LOCAL_HOST, Constants.MONGO_PORT);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			DB db = mongo.getDB(Constants.MONGO_DB);
			dbCollection = db.getCollection(
					Constants.MONGO_VERTEX_PARTITIONID_COLLECTION);
		}
		
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
			Set<String> adjVertices = new HashSet<String>();
			ArrayListMultimap<String, String> partitionVertexID = 
				ArrayListMultimap.create();
									
			boolean isType = false;
			while(values.hasNext()) {
				String[] tokens = values.next().toString().split(
										Constants.REGEX_DELIMITER);
				if(tokens.length == 2) 
					isType = true;
				adjVertices.add(tokens[0]);
			}
			// if its a type vertex then find partition IDs of all adjVertices
			// using the underlying DB.
			if(isType) {
				for(String adjVID : adjVertices) {
					BasicDBObject queryDoc = new BasicDBObject();
					queryDoc.put(Constants.FIELD_VERTEX_ID, adjVID);
					DBObject resultDoc = dbCollection.findOne(queryDoc);
					partitionVertexID.put((String)resultDoc.get(
							Constants.FIELD_PARTITION_ID), adjVID);
				}
				// retrieve all the partition IDs and see whose value list
				// size is the max.
				String pid = "";
				int maxEdgesInPartition = 0;
				int currSize;
				Set<String> keys = partitionVertexID.keySet();
				for(String k : keys) {
					currSize = partitionVertexID.get(k).size();
					if(currSize > maxEdgesInPartition)
						pid = k;
				}
				output.collect(key, new Text(pid));
			}
			else
				adjVertices.clear();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			String message = "Incorrect arguments -- requires 2 argument.\n\t " +
			"1) directory containing high degree triples \n\t" +
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
		jobConf.setJobName("HighDegreeTriplePlacer");
		
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
		ToolRunner.run(new Configuration(), new HighDegreeTriplePlacer(), args);
	}

}

/*
 * public void generatePIDForHDTriples(String highDegreeTriplesPath) {
		// for each object type, group by subject
		// for each subject, check the partition ID
			// also check triples which have the same subject as this object
			// i.e. check for in & out edges of this object
		// to the partition which covers most subjects, add the object as well
		
		
	}
 * 
 */
