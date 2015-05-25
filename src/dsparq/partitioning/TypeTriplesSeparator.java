
package dsparq.partitioning;

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
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import dsparq.misc.Constants;
import dsparq.misc.HostInfo;
import dsparq.misc.PropertyFileHandler;
import dsparq.util.Util;
        
/**
 * This class is used to separate typed triples from the given set
 * of triples.
 * 
 * @author Jiewen Huang  - PhD student at 
 * Yale University. Code written for SW-Store.
 * 
 * Modified by Raghava Mutharaju
 *
 */
public class TypeTriplesSeparator extends Configured implements Tool{
	
	private static final Logger log = Logger.getLogger(TypeTriplesSeparator.class); 
	
	private static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> 
	{
		private static String predicateTypeID;
		
		@Override
		public void configure(JobConf conf) {
			predicateTypeID = conf.get("typeID");
		}
		
	    public void map(Text key, Text value, OutputCollector<Text, Text> output, 
	    		Reporter reporter) throws IOException 
	    {
	    	output.collect(key, value);
	    } 
 
		private static class MultiFileOutput 
			extends MultipleTextOutputFormat<Text, Text> {
		     protected String generateFileNameForKeyValue(Text key, 
		    		 Text value, String name) {
		    	 
		    	 String line = key.toString();
		    	 String[] tokenizer = line.split(Constants.REGEX_DELIMITER);
		    	 if (tokenizer[Constants.POSITION_PREDICATE].
		    			 equals(predicateTypeID))	
		    		 return "typed-triples-" + name;
		    	 else
		    		 return "untyped-triples-" + name;
		     }
		}
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TypeTriplesSeparator(), args);
		System.exit(res);
	}	

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			String msg = "Incorrect arguments -- requires 2 arguments.\n\t " +
					"1) Triples file \n\t" +
					"2) path to output directory which would hold " +
						"rdf:type and non-rdf:type triples \n\t";
			throw new RuntimeException(msg + this.getClass());
		}

		String triples = args[0];
		String outputDir = args[1];

		Path outputPath = new Path(outputDir);
		Configuration fconf = new Configuration();
		FileSystem fs = FileSystem.get(fconf);

		if (fs.exists(outputPath)) 
			fs.delete(outputPath, true);
			
		JobConf jobConf = new JobConf(this.getClass());
		jobConf.setJobName("TypeTriplesSeparator");
		//get the ID of rdf:type and make it available to MR job
		PropertyFileHandler propertyFileHandler = 
				PropertyFileHandler.getInstance();
		HostInfo routerHostInfo = propertyFileHandler.getMongoRouterHostInfo();
		Mongo mongoRouter = new MongoClient(routerHostInfo.getHost(), 
				routerHostInfo.getPort());
		DB db = mongoRouter.getDB(Constants.MONGO_RDF_DB);
		DBCollection idValCollection = db.getCollection(
				Constants.MONGO_IDVAL_COLLECTION);
		String typeID = getPredicateIDFromDB(idValCollection);
		mongoRouter.close();
		jobConf.set("typeID", typeID);
		
		FileOutputFormat.setOutputPath(jobConf, outputPath);		
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(Map.MultiFileOutput.class);			
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);		
		FileInputFormat.setInputPaths(jobConf, new Path(triples));
		jobConf.setMapperClass(Map.class);
//		jobConf.setNumMapTasks(propertyFileHandler.getShardCount());
		jobConf.setNumReduceTasks(0);
		
		RunningJob job = JobClient.runJob(jobConf);
		if (!job.isSuccessful())
			log.error("FAILED!!!");

		return 0;
	}
	
	private String getPredicateIDFromDB(DBCollection idValCollection) 
			throws Exception {
		String digest = Util.generateMessageDigest(Constants.RDF_TYPE_URI);
		BasicDBObject queryDoc = new BasicDBObject();
		queryDoc.put(Constants.FIELD_HASH_VALUE, digest);
		BasicDBObject projectionDoc = new BasicDBObject();
		projectionDoc.put(Constants.FIELD_NUMID, 1);
		DBObject resultDoc = idValCollection.findOne(queryDoc, 
				projectionDoc);
		if(resultDoc == null)
			throw new Exception("ID not present for rdf:type");
		Long numID = (Long) resultDoc.get(Constants.FIELD_NUMID);
		if(numID == null)
			throw new Exception("numID is null for " + digest);
		return numID.toString();
	}
   
}
