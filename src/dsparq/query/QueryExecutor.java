package dsparq.query;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.Mongo;
import com.mongodb.MapReduceCommand.OutputType;

import dsparq.misc.Constants;

public class QueryExecutor {

	
	public void runTestQuery1() throws Exception {
		/*
		 * sparql: select ?s ?o ?dob ?n where {
				   ?s <http://dbpedia.org/property/influenced> ?o
				   ?o <http://dbpedia.org/property/dateOfBirth> ?dob
				   ?o <http://dbpedia.org/property/name> ?n 
				   } 
		 */
		
		Mongo mongo = new Mongo(Constants.MONGO_LOCAL_HOST, Constants.MONGO_PORT);
		DB db = mongo.getDB(Constants.MONGO_DB);
		DBCollection collection = db.getCollection(Constants.MONGO_TRIPLE_COLLECTION);
		
		// using MapReduce feature of MongoDB to run the above query
		String map = "function() { " +
				"if(this.predicate === \"<http://dbpedia.org/property/influenced>\") " +
					"emit(this.object, {s: this.subject, o: this.object, dob: \"\", " +
					" n: \"\", predicate: this.predicate}); " +
				"else if(this.predicate === \"<http://dbpedia.org/property/dateOfBirth>\") " +
					"emit(this.subject, {s: \"\", o: this.subject, dob: this.object, " +
					" n: \"\", predicate: this.predicate});  " +
				"else if(this.predicate === \"<http://dbpedia.org/property/name>\") " +
					"emit(this.subject, {s: \"\", o: this.subject, dob: \"\", " +
					" n: this.object, predicate: this.predicate});  " +
				"} ";
		
		String reduce = "function(key, values) { " +
				"var result = {s: \"\", o: \"\", dob: \"\", " +
					" n: \"\", predicate: \"\"};" +
				"values.forEach(function(value) { " +
					"if(value.predicate === \"<http://dbpedia.org/property/influenced>\") { " +
						"if(!result.s) " +
							"result.s = value.s; " +
						"if(!result.o) " +
							"result.o = value.o; " +
					"} " +
					"else if(value.predicate === \"<http://dbpedia.org/property/dateOfBirth>\") { " +
						"if(!result.o) " +
							"result.o = value.o; " +
						"if(!result.dob) " +
							"result.dob = value.dob; " +
					"} " +
					"else if(value.predicate === \"<http://dbpedia.org/property/name>\") { " +
						"if(!result.o) " +
							"result.o = value.o; " +
						"if(!result.n) " +
							"result.n = value.n; " +
					"} " +
				"}); " +
				"return result; " +
				"} ";
		
		DBObject mrQuery = new BasicDBObject();
		DBObject clause1 = new BasicDBObject("predicate", 
				"<http://dbpedia.org/property/influenced>");
		DBObject clause2 = new BasicDBObject("predicate", 
				"<http://dbpedia.org/property/dateOfBirth>");
		DBObject clause3 = new BasicDBObject("predicate", 
				"<http://dbpedia.org/property/name>");
		BasicDBList orList = new BasicDBList();
		orList.add(clause1);
		orList.add(clause2);
		orList.add(clause3);
	
		mrQuery.put("$or", orList);
		
		System.out.println("Starting MapReduce...");
		MapReduceOutput mrOutput = collection.mapReduce(map, reduce, 
				"query1_results", OutputType.REDUCE, mrQuery);
		
//		MapReduceCommand mrCommand = new MapReduceCommand(collection, map, 
//				reduce, "query1_results", OutputType.REDUCE, mrQuery);
//		MapReduceOutput mrOutput = collection.mapReduce(mrCommand);
		
		DBCollection outputCollection = mrOutput.getOutputCollection();
		System.out.println("No of documents in output: " + outputCollection.count());
		
		mongo.close();
	}
	
	public static void main(String[] args) throws Exception {
		QueryExecutor queryExecutor = new QueryExecutor();
		queryExecutor.runTestQuery1();
	}

}
