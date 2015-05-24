package dsparq.sample;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.log4j.Logger;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import dsparq.misc.Constants;
import dsparq.util.Util;


public class SampleMR extends Configured implements Tool {

	private static final Logger log = Logger.getLogger(SampleMR.class);
	
	private static class Map extends MapReduceBase implements 
	Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text key, Text value, 
		OutputCollector<Text, Text> output,
		Reporter reporter) throws IOException {
		
		StringReader reader = new StringReader(key.toString());
		NxParser nxParser = new NxParser(reader);
		Node[] nodes = nxParser.next();		
		String object = null;
		if(nodes[2] instanceof Literal) {
			//make this in the same form as Jena sees it.
			//Eg: "Journal 1 (1940)"^^http://www.w3.org/2001/XMLSchema#string
			Literal literal = (Literal) nodes[2];
			StringBuilder sb = new StringBuilder();
			sb.append("\"").append(literal.getData()).append("\"");
			if(literal.getDatatype() != null)
				sb.append("^^").append(literal.getDatatype().toString());
			object = sb.toString();
		}
		else
			object = nodes[Constants.POSITION_OBJECT].toString();
		output.collect(new Text(nodes[Constants.POSITION_SUBJECT].toString()), 
				new Text(nodes[Constants.POSITION_PREDICATE].toString() +
				         Constants.TRIPLE_TERM_DELIMITER + object));
		
		reader.close();
	}
	}

	private static class Reduce extends MapReduceBase implements 
	Reducer<Text, Text, Text, NullWritable> {
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			try {
				String subDigest = Util.generateMessageDigest(key.toString());
				output.collect(new Text("Subj: " + key.toString() + " !!! " + 
						subDigest), null);
				while(values.hasNext()) {
					String[] predObj = 
							values.next().toString().split(Constants.REGEX_DELIMITER);
					String predDigest = Util.generateMessageDigest(predObj[0]);
					String objDigest = Util.generateMessageDigest(predObj[1]);
					output.collect(new Text("Pred: " + predObj[0] + " !!! " + 
							predDigest), null);
					output.collect(new Text("Obj: " + predObj[1] + " !!! " + 
							objDigest), null);
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Configuration fconf = new Configuration();
		FileSystem fs = outputPath.getFileSystem(fconf);

		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		JobConf jobConf = new JobConf(this.getClass());
		jobConf.setJobName("SampleMR");
		jobConf.set("mongo.router", args[2]);
		FileOutputFormat.setOutputPath(jobConf, outputPath);
		FileInputFormat.setInputPaths(jobConf, inputPath);
		jobConf.setInputFormat(KeyValueTextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);	
		jobConf.setMapperClass(Map.class);
		jobConf.setReducerClass(Reducer.class);
		jobConf.setMapOutputKeyClass(Text.class);
	    jobConf.setMapOutputValueClass(Text.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(NullWritable.class);
		
		RunningJob job = JobClient.runJob(jobConf);
		if(!job.isSuccessful()) {
			log.error("FAILED!!!");
			return -1;
		}
		else
			return 0;
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			String msg = "Incorrect arguments -- requires 3 arguments.\n\t " +
					"1) NT Triples file \n\t" +
					"2) Output path \n\t" +
					"3) Mongo router in host:port format \n\t";
			throw new Exception(msg);
		}
		ToolRunner.run(new Configuration(), new SampleMR(), args);
	}
}
