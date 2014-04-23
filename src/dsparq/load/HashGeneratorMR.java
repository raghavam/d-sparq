package dsparq.load;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
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
import org.apache.log4j.Logger;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import dsparq.misc.Constants;
import dsparq.misc.PropertyFileHandler;
import dsparq.util.Util;

/**
 * This class generates SHA-1 for each part of the 
 * triple (subject/predicate/object) and inserts into DB.
 * 
 * @author Raghava
 *
 */
public class HashGeneratorMR extends Configured implements Tool {

	private static final Logger log = Logger.getLogger(HashGeneratorMR.class); 
	
	private static class Map extends MapReduceBase implements 
			Mapper<Text, Text, Text, Text> {
		
		@Override
		public void map(Text key, Text value, 
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			
			// expected input format: subject predicate object
			
			StringReader reader = new StringReader(key.toString());
			NxParser nxParser = new NxParser(reader);
			Node[] nodes = nxParser.next();
			StringBuilder strBuilder = new StringBuilder();
			strBuilder.append(nodes[1].toString()).
						append(Constants.TRIPLE_TERM_DELIMITER).
						append(nodes[2].toString());
			//group by subject
			output.collect(new Text(nodes[0].toString()), 
					new Text(strBuilder.toString()));
		}
	}
	
	private static class Reduce extends MapReduceBase implements
	Reducer<Text, Text, Text, Text> {
		
		// read Vertex ID of subject from DB.
				private Mongo mongo;
				private DBCollection idValCollection;

				@Override
				public void configure(JobConf jobConf) {
					log.info("Configuring Mongo...");
					try {
						String[] hostPort = jobConf.get("mongo.router").split(":");
						mongo = new MongoClient(hostPort[0], 
								Integer.parseInt(hostPort[1]));
						DB db = mongo.getDB(Constants.MONGO_RDF_DB);
						idValCollection = db.getCollection(
								Constants.MONGO_IDVAL_COLLECTION);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
		
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter) {
			try {
				List<DBObject> docs = new ArrayList<DBObject>();
				String keyStr = key.toString();
				String subjectDigestValue = Util.generateMessageDigest(keyStr);
				BasicDBObject subDoc = new BasicDBObject();
				subDoc.put(Constants.FIELD_HASH_VALUE, subjectDigestValue);
				subDoc.put(Constants.FIELD_STR_VALUE, keyStr);
				docs.add(subDoc);
				while(values.hasNext()) {
					Text predObjText = values.next();
					String[] predObj = predObjText.toString().split(
							Constants.REGEX_DELIMITER);
					for(String term : predObj) {
						String termDigestValue = 
								Util.generateMessageDigest(term);
						BasicDBObject termDoc = new BasicDBObject();
						termDoc.put(Constants.FIELD_HASH_VALUE, 
								termDigestValue);
						termDoc.put(Constants.FIELD_STR_VALUE, term);
						docs.add(termDoc);
					}
				}
				idValCollection.insert(docs);
				docs.clear();
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void close() throws IOException {
			mongo.close();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 1) {
			String message = "Incorrect arguments -- requires 1 argument.\n\t " +
			"1) directory containing N-triples. " +
			"Also check the value of mongos in property file";
			throw new Exception(message);
		}

		String triples = args[0];		
		JobConf jobConf = new JobConf(this.getClass());
		jobConf.setJobName("HashGeneratorMR");
		
		PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
		jobConf.set("mongo.router", propertyFileHandler.getMongoRouter());
		
		log.info("task timeout: " + jobConf.get("mapreduce.task.timeout"));
		// default is 10 mins, setting it to 30 mins
//		jobConf.setLong("mapreduce.task.timeout", 1800000);
		
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);	
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(jobConf, new Path(triples));

		jobConf.setMapperClass(Map.class);
//		jobConf.setNumReduceTasks(0);
		jobConf.setReducerClass(Reduce.class);
		
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);		
		RunningJob job = JobClient.runJob(jobConf);
		if (!job.isSuccessful())
			System.out.println("Hadoop Job Failed");		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new HashGeneratorMR(), args);
	}
}
