package dsparq.load;

import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.log4j.Logger;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

import dsparq.misc.Constants;
import dsparq.misc.HostInfo;
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
		
		// read Vertex ID of subject from DB.
		private Mongo mongo;
		private DBCollection idValCollection;
		private DBCollection eidValCollection;
		private Model model;

		@Override
		public void configure(JobConf jobConf) {
			log.info("Configuring Mongo...");
			try {
				model = ModelFactory.createDefaultModel();
				String hostName = InetAddress.getLocalHost().getHostName();
				// hostName starts with nimbus. Extracting the number after "nimbus"
				Pattern pattern = Pattern.compile("\\d+");
				Matcher matcher = pattern.matcher(hostName);
				matcher.find();
				int mongosNum = Integer.parseInt(matcher.group());
				int mongosCount = Integer.parseInt(jobConf.get("mongosCount"));
				List<HostInfo> mongosList = new ArrayList<HostInfo>(mongosCount);
				for(int i=0; i<mongosCount; i++) {
					String[] hostPort = jobConf.get("mongos"+i).split(":");
					mongosList.add(new HostInfo(hostPort[0], 
									Integer.parseInt(hostPort[1])));
				}
				MongoOptions mongoOptions = new MongoOptions();
				// default is 10. increased it to avoid "Connection refused" exception
				mongoOptions.setConnectionsPerHost(20);
				ServerAddress serverAddress = new ServerAddress(
						mongosList.get(mongosNum%mongosCount).getHost(),
						mongosList.get(mongosNum%mongosCount).getPort());
				mongo = new Mongo(serverAddress, mongoOptions);
				log.info("Configuration done (Mongos identified)");
			} catch (Exception e) {
				e.printStackTrace();
			}
			DB db = mongo.getDB(Constants.MONGO_RDF_DB);
			idValCollection = db.getCollection(
					Constants.MONGO_IDVAL_COLLECTION);
			eidValCollection = db.getCollection(Constants.MONGO_EIDVAL_COLLECTION);
		}
		
		@Override
		public void map(Text key, Text value, 
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			
			// expected input format: subject predicate object
			reporter.progress();
			
			StringReader reader = new StringReader(key.toString());
			model.read(reader, Constants.BASE_URI, "N-TRIPLE");
			StmtIterator stmtIterator = model.listStatements();
			Triple triple = stmtIterator.next().asTriple();
			reader.close();
			model.removeAll();
			// subject & object go into idValCollection
			// predicate goes into eidValCollection
			try {
				String subject = triple.getSubject().toString();
				String hashSub = Util.generateMessageDigest(subject);
				BasicDBObject subDoc = new BasicDBObject();
				subDoc.put(Constants.FIELD_STR_VALUE, subject);
				subDoc.put(Constants.FIELD_HASH_VALUE, hashSub);
				subDoc.put("isType", "false");
				try {
					idValCollection.insert(subDoc, WriteConcern.SAFE);
				} catch(MongoException.DuplicateKey e) {
					log.info("Dup val in subject: " + subject); 
				}
				
				log.info("Processing triple with subject: " + subject);
				
				String predicate = triple.getPredicate().toString();
				String hashPred = Util.generateMessageDigest(predicate);
				boolean isType = false;
				if(predicate.equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") ||
						   predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") ||
						   predicate.equals("rdf:type"))
					isType = true;
				BasicDBObject subPred = new BasicDBObject();
				subPred.put(Constants.FIELD_STR_VALUE, predicate);
				subPred.put(Constants.FIELD_HASH_VALUE, hashPred);
				try {
					eidValCollection.insert(subPred, WriteConcern.SAFE);
				} catch(MongoException.DuplicateKey e) { 
					log.info("Dup val in predicate: " + predicate); 
				}
				
				String object = triple.getObject().toString();
				String hashObj = Util.generateMessageDigest(object);
				BasicDBObject subObj = new BasicDBObject();
				subObj.put(Constants.FIELD_STR_VALUE, object);
				subObj.put(Constants.FIELD_HASH_VALUE, hashObj);
				subObj.put("isType", isType);
				try {
					idValCollection.insert(subObj, WriteConcern.SAFE);
				} catch(MongoException.DuplicateKey e) { 
					log.info("Dup val in obj: " + object); 
				}
			}
			catch(Exception e) {
				log.error("Exception in Map: " + e.getMessage());
				e.printStackTrace();
			}
			reporter.progress();
/*			
			if(tokens[Constants.POSITION_PREDICATE].equals(Constants.PREDICATE_TYPE))
				output.collect(new Text(tokens[Constants.POSITION_OBJECT]), 
								new Text(tokens[Constants.POSITION_SUBJECT] + 
										Constants.OUTPUT_DELIMITER + 
										Constants.PREDICATE_TYPE));
			else
				output.collect(new Text(tokens[Constants.POSITION_SUBJECT]), 
						new Text(tokens[Constants.POSITION_OBJECT]));
*/						
		}
		
		@Override
		public void close() throws IOException {
			mongo.close();
			model.close();
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
		jobConf.setJobName("HashGeneratorMR");
		
		PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
		int mongosCount = propertyFileHandler.getMongosCount();
		List<HostInfo> mongosList = propertyFileHandler.getAllMongoRouters();
		jobConf.setInt("mongosCount", mongosCount);
		for(int i=0; i<mongosList.size(); i++)
			jobConf.set("mongos" + i, mongosList.get(i).toString());
		
		log.info("task timeout: " + jobConf.get("mapreduce.task.timeout"));
		// default is 10 mins, setting it to 30 mins
		jobConf.setLong("mapreduce.task.timeout", 1800000);
		
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
		ToolRunner.run(new Configuration(), new HashGeneratorMR(), args);
	}
}
