package dsparq.sample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.jgrapht.graph.DirectedMultigraph;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.openjena.riot.RiotReader;
import org.openjena.riot.lang.LangNTriples;
import org.semanticweb.yars.nx.parser.NxParser;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.util.Hashing;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_Literal;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.util.FmtUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import dsparq.misc.Constants;
import dsparq.misc.HostInfo;
import dsparq.misc.PropertyFileHandler;
import dsparq.util.Util;

public class MongoConnector {

	public static void connectToMongo() throws Exception{
		Mongo mongo = new Mongo("localhost", 27017);
		DB db = mongo.getDB("testDB");
		DBCollection collection = db.getCollection("testCollection");
		BasicDBObject document = new BasicDBObject();
		
		collection.drop();
		
		document.put("name", "MongoDB");
		document.put("type", "database");
		document.put("count", 1);

        BasicDBObject info = new BasicDBObject();

        info.put("x", 203);
        info.put("y", 102);

        document.put("info", info);
        collection.insert(document);
        
        collection.save(document);
        
        System.out.println("Document inserted");
        System.out.println("\nPrinting out the results\n");
        
        DBObject myDoc = collection.findOne();
        System.out.println(myDoc);
        mongo.close();
	}
	
	
	public static void testIndexes() throws Exception {
		Mongo mongo = new Mongo("localhost", 27017);
		DB db = mongo.getDB("testDB");
		DBCollection collection = db.getCollection("testCollection");
		BasicDBObject index = new BasicDBObject();
		
		index.put("subject", 1);
		collection.ensureIndex(index);
		
		index = new BasicDBObject();
		index.put("predicate", 1);
		collection.ensureIndex(index);
		
		index = new BasicDBObject();
		index.put("object", 1);
		collection.ensureIndex(index);
		
		System.out.println("Indexes created");
		mongo.close();
	}
	
	public static void testDocInserts() throws Exception {
		Mongo mongo = new Mongo(Constants.MONGO_LOCAL_HOST, Constants.MONGO_PORT);
		DB db = mongo.getDB(Constants.MONGO_DB);
		DBCollection collection = db.getCollection(Constants.MONGO_TRIPLE_COLLECTION);
		BasicDBObject document = new BasicDBObject();
		
		collection.drop();
		
		document.put("subject", 1);
		document.put("predicate", "<http://dbpedia.org/property/influenced>");
		document.put("object", 21);		
		collection.insert(document);
		
		document = new BasicDBObject();		
		document.put("subject", 21);
		document.put("predicate", "<http://dbpedia.org/property/dateOfBirth>");
		document.put("object", "27-10-1982");		
		collection.insert(document);    
		
		document = new BasicDBObject();		
		document.put("subject", 21);
		document.put("predicate", "<http://dbpedia.org/property/name>");
		document.put("object", "no-name");		
		collection.insert(document);  
		
		document = new BasicDBObject();
		document.put("subject", 100);
		document.put("predicate", "<http://dbpedia.org/property/influenced>");
		document.put("object", 210);		
		collection.insert(document);
		
		document = new BasicDBObject();		
		document.put("subject", 210);
		document.put("predicate", "<http://dbpedia.org/property/dateOfBirth>");
		document.put("object", "31-10-1985");		
		collection.insert(document);    
		
		document = new BasicDBObject();		
		document.put("subject", 210);
		document.put("predicate", "<http://dbpedia.org/property/name>");
		document.put("object", "no-name-yet");		
		collection.insert(document);  
		
        System.out.println("Documents inserted");
        mongo.close();
	}
	
	public static void readRDFTriples(String inputPath) throws Exception {
//		Model rdfModel = ModelFactory.createDefaultModel();
//		InputStream in = FileManager.get().open(inputPath);
//		BufferedInputStream tripleReader = new BufferedInputStream(in);
//		rdfModel.read(in, null, "N-TRIPLE");
		
		int count = 0;
		FileInputStream in = new FileInputStream(inputPath);
		LangNTriples ntriples = RiotReader.createParserNTriples(in, null);
		while(ntriples.hasNext()) {
			Triple t = ntriples.next();
			System.out.print(FmtUtils.stringForNode(t.getSubject()) + " ");
			System.out.print(FmtUtils.stringForNode(t.getPredicate()) + " ");
			Node object = t.getObject();
			boolean isLiteral = object.isLiteral();
			if(isLiteral)
				System.out.println(FmtUtils.stringForLiteral((Node_Literal)object, null) + " " + isLiteral);
			else
				System.out.println(FmtUtils.stringForNode(object) + " " + isLiteral);
			
			count++;
			if(count >= 100)
				break;
		}
/*		
		Statement triple;
		StmtIterator stmtIterator = rdfModel.listStatements();
		
		while(stmtIterator.hasNext()) {
			triple = stmtIterator.next();
			Resource subject = triple.getSubject();
			System.out.print(subject.getLocalName() + " ");
			Property predicate = triple.getPredicate();
			System.out.print(predicate.getLocalName() + " ");
			RDFNode object = triple.getObject();
			System.out.println(object.toString() + " " + object.isLiteral());
			
			count++;
			if(count >= 100)
				break;
		}
		
		tripleReader.close();
		rdfModel.close();
*/		
		in.close();
	}
	
	public static void testRandomAccess(String path) throws Exception {
		RandomAccessFile rf = new RandomAccessFile(path, "rw");
		rf.writeBytes("34 33 \r");
		rf.close();
		System.out.println("Done");
	}
	
	public static void testFindAndModify() throws Exception {
		Mongo mongo = new Mongo(Constants.MONGO_LOCAL_HOST, Constants.MONGO_PORT);
		DB db = mongo.getDB("testdb");
		DBCollection collection = db.getCollection("testcol");
//		collection.insert(new BasicDBObject("seqid", 0));
		DBObject result = collection.findAndModify(
				new BasicDBObject("seqid", new BasicDBObject("$gte", 0)), 
				new BasicDBObject("seqid", 1), null, false, 
				new BasicDBObject("$inc", new BasicDBObject("seqid",1)), 
				true, false);
		if(result == null)
			System.out.println("Result is null");
		else
			System.out.println(result.get("seqid"));
//		db.dropDatabase();
		mongo.close();
	}
	
	public static void testUniqueIndex() throws Exception {
		Mongo mongo = new Mongo(Constants.MONGO_LOCAL_HOST, Constants.MONGO_PORT);
		DB db = mongo.getDB("testdb");
		DBCollection collection = db.getCollection("testcol");
		collection.ensureIndex(new BasicDBObject("id", 1), 
				new BasicDBObject("unique", true));
		
		for(int i=0; i<=10; i++)
			collection.insert(new BasicDBObject("id", i), WriteConcern.SAFE);
		
		
		for(int i=9; i<=20; i++) {
			try {
			collection.insert(new BasicDBObject("id", i), WriteConcern.SAFE);
			} catch(MongoException.DuplicateKey e) {
				System.out.println("Duplicate element: " + i);
			}
		}
		
		mongo.close();
		System.out.println("Done");
	}
	
	public static void testFileMerge() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FileUtil.copyMerge(fs, new Path("/mnt/hadoop/merge-input"), fs, 
				new Path("/mnt/hadoop/merge-output"), false, conf, "merged");
		System.out.println("Done");
	}
	
	public static void testSharding() throws Exception {		
		// initialize
		PropertyFileHandler propertyFileHandler = 
			PropertyFileHandler.getInstance();

		HostInfo mongoSHostPort = propertyFileHandler.getMongoRouterHostInfo();
		Mongo mongo = new Mongo(mongoSHostPort.getHost(), mongoSHostPort.getPort());
		//HostInfo mongoSHostPort = new HostInfo("ip-10-50-73-199.eu-west-1.compute.internal", 27017);
/*		
		DB adminDB = mongo.getDB(Constants.MONGO_ADMIN_DB);
		List<HostInfo> shardsHostInfo = propertyFileHandler.getAllShardsInfo();
		
		// add shards
		for(HostInfo hinfo : shardsHostInfo) {
		adminDB.command(new BasicDBObject(Constants.MONGO_CONFIG_ADD_SHARD, 
				hinfo.getHost() + ":" + hinfo.getPort()));
		}
		// enable sharding of the db
		adminDB.command(new BasicDBObject(Constants.MONGO_ENABLE_SHARDING, 
			"testdb"));
		
		// enable sharding of specific collections - idvals, triples collection
		BasicDBObject optionsDoc = new BasicDBObject();
		optionsDoc.put(Constants.MONGO_SHARD_COLLECTION, "testdb.tcol");
		optionsDoc.put("key", "field1");
		adminDB.command(optionsDoc);
*/		
		DB db = mongo.getDB("tdb");
		DBCollection testCollection = db.getCollection("tcol");
		testCollection.ensureIndex(new BasicDBObject("f1", 1), 
				new BasicDBObject(Constants.MONGO_UNIQUE_INDEX, true));
	
		System.out.println("Starting insertion of docs");
		// insert documents
		for(long i=0; i < 3000000; i++) {
			BasicDBObject doc = new BasicDBObject();
			doc.put("f1", i);
			i++;
			doc.put("f2", i);
			i++;
			doc.put("f3", i);
			i++;
			testCollection.insert(doc);
		}
		System.out.println("Done");
		mongo.close();
	}
	
	private static void testUUID() {
//		String uri1 = "http://lubm.example.org/class1";
//		String uri2 = "http://lubm.example.org/class2";
//		UUID u1 = UUID.fromString(uri1);
//		UUID u2 = UUID.fromString(uri2);
//		UUID u3 = UUID.fromString(uri1);
		
		System.out.println(UUID.randomUUID());
		BigInteger i; 
/*		
		System.out.println("UUID1: " + u1.toString());
		System.out.println("UUID2: " + u2.toString());
		System.out.println("UUID3: " + u3.toString());
		System.out.println("UUID1 equals UUID3: " + u1.equals(u3));
*/		
	}
	
	private static void testRDFReader() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		String triple1 = "<http://www.Department0.University0.edu/FullProfessor7> " +
				"<http://lubm.example.org#teacherOf> " +
				"<http://www.Department0.University0.edu/GraduateCourse12> .";
		String triple2 = "<http://www.Department0.University0.edu/FullProfessor7> " +
				"<http://lubm.example.org#emailAddress> " +
				"\"FullProfessor7@Department0.University0.edu\" .";
//		ByteArrayInputStream s = new ByteArrayInputStream(triple1.getBytes("UTF-8"));
		StringReader sr = new StringReader(triple1);
		model.read(sr, Constants.BASE_URI, "N-TRIPLE");
		StmtIterator stmtIterator = model.listStatements();
		while(stmtIterator.hasNext()) {
			Triple triple = stmtIterator.next().asTriple();
			System.out.println(triple.getSubject());
			System.out.println(triple.getPredicate());
			System.out.println(triple.getObject());
		}
//		s.close();
		sr.close();
	}
	
	private static void testServerScripting() throws Exception {
		Mongo mongo = new Mongo("localhost", 10000);
		DB db = mongo.getDB("rdfdb");
		
		String testScript = 
			"function (x) { " +
				"var collection = db.getCollection('ptriples'); " +
				"var cursor = collection.find({ 'subject' : x, 'predicate' : y, 'object' : z }); " +
				"var result = new Array(); " +
				"var i = 0; " +
				"while(cursor.hasNext()) { " +
					"var doc = cursor.next(); " +
					"result[i] = doc['subject']; " +
					"i++; " +
					"result[i] = doc['predicate']; " +
					"i++; " +
					"result[i] = doc['object']; " +
					"i++; " +
				"} " +
				"return result; " +
			"} ";
		
		String funcTest = 
			"function () { " +
				"return arguments[1]; " +
			"} ";
		
		CommandResult result = db.doEval(funcTest, new Long(52718665), 
				new Long(12), new Long(109));
		if(result != null)
			System.out.println("Result not null");
		Set<Entry<String, Object>> entries = result.entrySet();
		for(Entry<String, Object> entry : entries) {
			System.out.println("Key: " + entry.getKey() + "  Value: " + 
					entry.getValue().getClass().getCanonicalName());
		}
		System.out.println();
		BasicDBObject obj = (BasicDBObject)result.get("retval");
		System.out.println(obj.get("value"));

/*		
		System.out.println();
		BasicDBList resultDoc = (BasicDBList) result.get("retval");
		System.out.println("Result len: " + resultDoc.size());
		for(Object tripleResult : resultDoc) {
			Long l = (Long) tripleResult;
			System.out.println(l);
		}
*/		
	}
	
	private static void testArrayInsertion() throws Exception {
		Mongo mongo = new Mongo("nimbus3", 10000);
		DB db = mongo.getDB("rdfdb");
		DBCollection sample = db.getCollection("sample1");
		BasicDBObject doc = new BasicDBObject();
		List<Long> predicates = new ArrayList<Long>();
		List<Long> objects = new ArrayList<Long>();
		doc.put("subject", new Long(10));
		predicates.add(new Long(2));
		predicates.add(new Long(3));
		predicates.add(new Long(4));
		objects.add(new Long(20));
		objects.add(new Long(30));
		objects.add(new Long(40));
		doc.put("predicate", predicates);
		doc.put("object", objects);
		sample.insert(doc);
		
		// schema-2
		sample = db.getCollection("sample2");
		BasicDBObject doc2 = new BasicDBObject();
		doc2.put("subject", new Long(10));
		List<BasicDBObject> predObjList = new ArrayList<BasicDBObject>();
		for(int i=1; i<=3; i++) {
			BasicDBObject predObj = new BasicDBObject();
			predObj.put("predicate", new Long(2*i));
			predObj.put("object", new Long(2*i*10));
			predObjList.add(predObj);
		}
		doc2.put("predobj", predObjList);
		sample.insert(doc2);
		
		BasicDBObject doc3 = new BasicDBObject();
		doc3.put("subject", new Long(100));
		List<BasicDBObject> predObjList3 = new ArrayList<BasicDBObject>();
		for(int i=1; i<=3; i++) {
			BasicDBObject predObj = new BasicDBObject();
			predObj.put("predicate", new Long(3*i));
			predObj.put("object", new Long(3*i*10));
			predObjList3.add(predObj);
		}
		BasicDBObject predObj = new BasicDBObject();
		predObj.put("predicate", new Long(3));
		predObj.put("object", new Long(40));
		predObjList3.add(predObj);
		doc3.put("predobj", predObjList3);
		sample.insert(doc3);
		
		
		mongo.close();
		System.out.println("Done");
	}
	
	private static void testRedisScript() {
		Jedis localStore = new Jedis("localhost", 6479);
		localStore.connect();
		// Key is the digest value, fields are Str and ID
		String insertionScript = 
			"termID = tonumber(KEYS[1]) " +
			"if(termID == 1) then " +
				"numericID = redis.call('INCR', 'subCount') " +
			"elseif(termID == 2) then " +
				"numericID = redis.call('INCR', 'propCount') " +
			"elseif(termID == -1) then " +
				"numericID = redis.call('INCRBY', 'typeCount', -1) " +
			"end " +
//			"redis.call('HMSET', digestValue, 'id', numericID, 'str', strValue) " +
			"return numericID ";
		Long returnVal = (Long) localStore.eval(insertionScript, 1, String.valueOf(-1));
		System.out.println("Return value: " + returnVal.longValue());
		localStore.disconnect();
	}
	
	private static void findGraphCluster() {
		DirectedMultigraph<String, String> graph = 
			new DirectedMultigraph<String, String>(String.class);
		
		graph.addVertex("s1");
		graph.addVertex("o1");
		graph.addEdge("s1", "o1", "p1");
		
		graph.addVertex("s1");
		graph.addVertex("o2");
		graph.addEdge("s1", "o2", "p2");
		
		graph.addVertex("s1");
		graph.addVertex("o3");
		graph.addEdge("s1", "o3", "p3");
		
		graph.addVertex("s2");
		graph.addVertex("s1");
		graph.addEdge("s2", "s1", "p4");
		
		Set<String> vertices = graph.vertexSet();
		
		//find out source vertices
		List<String> sourceVertices = new ArrayList<String>();
		int inDegree;
		for(String v : vertices) {
			inDegree = graph.inDegreeOf(v);
			if(inDegree == 0)
				sourceVertices.add(v);
		}
		
		//do a breadth-first starting at sourceVertices
		int outDegree;
		BreadthFirstIterator<String, String> bfIterator;
		for(String startVertex : sourceVertices) {
			bfIterator = new BreadthFirstIterator<String, String>(
					graph, startVertex);
			while(bfIterator.hasNext()) {
				String v = bfIterator.next();
				System.out.println("Current Vertex: " + v);
				outDegree = graph.outDegreeOf(v);
				inDegree = graph.inDegreeOf(v);
				if(outDegree > 1) {
					//this is a cluster
					System.out.println("Cluster at: " + v);
					Set<String> outGoingEdges = graph.outgoingEdgesOf(v);
					for(String e : outGoingEdges) {
						System.out.println("\t" + e + "  " + graph.getEdgeTarget(e));
					}
					if(inDegree >= 1) {
						//could be a pipeline
						System.out.println("Pipeline:");
						Set<String> incomingEdges = graph.incomingEdgesOf(v);
						//now make the connection with the outgoing edge
						System.out.println("Connecting point: " + v);
						System.out.println("Incoming: ");
						for(String ie : incomingEdges)
							System.out.println("\t" + ie + "  " + graph.getEdgeSource(ie));
					}
				}
				else if(outDegree == 1) {
					if(inDegree >= 1) {
						Set<String> outGoingEdges = graph.outgoingEdgesOf(v);
						System.out.println("Pipeline with outDegree 1");
						for(String e : outGoingEdges)
							System.out.println("out edge: " + e + 
									"   " + graph.getEdgeTarget(v));
						//could be a pipeline
						Set<String> incomingEdges = graph.incomingEdgesOf(v);
						//now make the connection with the outgoing edge
						System.out.println("Connecting point: " + v);
						System.out.println("Incoming: ");
						for(String ie : incomingEdges)
							System.out.println("\t" + ie + "  " + graph.getEdgeSource(ie));
					}
				}
			}
		}
	}
	
	public static void testShardedPipeline() {
		List<JedisShardInfo> shards = 
				new ArrayList<JedisShardInfo>(3);
		shards.add(new JedisShardInfo("nimbus2", 6379, Constants.INFINITE_TIMEOUT));
		shards.add(new JedisShardInfo("nimbus3", 6379, Constants.INFINITE_TIMEOUT));
		shards.add(new JedisShardInfo("nimbus4", 6379, Constants.INFINITE_TIMEOUT));
		ShardedJedis shardedJedis = new ShardedJedis(shards, Hashing.MURMUR_HASH);
		ShardedJedisPipeline p = shardedJedis.pipelined();
		Response<String> id1 = p.hget("29eebb99bc01c0103f821e38ba186d0ff62ddbf3", "id");
		Response<String> id2 = p.hget("70045cbc8422c99c3c4b1a1a53f07e0eaf30cbc1", "id");
		Response<String> id3 = p.hget("f647b41d4767fb9389acfb2f2bfd9f0a5b375127", "id");
		p.sync();
		shardedJedis.disconnect();
		System.out.println("ID1: " + id1.get());
		System.out.println("ID2: " + id2.get());
		System.out.println("ID3: " + id3.get());
	}
	
	private static void findID() {
/*		
		String script = 
			"local keys = redis.call('KEYS', '*') " +
			"local id " +	
			"for index,value in pairs(keys) do " +
				"if(redis.call('TYPE', value) == 'hash') then " +
					"id = redis.call('HGET', value, 'id') " +
					"if(tonumber(id) == 12998309) then " +
						"return redis.call('HGETALL', value) " +
					"end " +
				"end " +
			"end ";				
		Jedis jedis = new Jedis("localhost", 6379, Constants.INFINITE_TIMEOUT);
		jedis.connect();
		Object obj = jedis.eval(script);
		if(obj != null) {
			System.out.println(obj.getClass().getCanonicalName());
			System.out.println(obj.toString());
		}
		else
			System.out.println("Null is returned");
		jedis.disconnect();
*/		
		List<JedisShardInfo> shards = 
				new ArrayList<JedisShardInfo>(3);
		shards.add(new JedisShardInfo("nimbus2", 6379, Constants.INFINITE_TIMEOUT));
		shards.add(new JedisShardInfo("nimbus3", 6379, Constants.INFINITE_TIMEOUT));
		shards.add(new JedisShardInfo("nimbus4", 6379, Constants.INFINITE_TIMEOUT));
		for(JedisShardInfo shard : shards) {
			Jedis jedis = new Jedis(shard);
			Set<String> keys = jedis.keys("*");
			System.out.println("Total keys: " + keys.size());
			for(String key : keys) {
				try {
					Map<String, String> map = jedis.hgetAll(key);
					String id = map.get("id");
					if(id.equals("12998309")) {
						System.out.println("str: " + map.get("str"));
						System.out.println("Key: " + key);
						jedis.disconnect();
						System.exit(0);
					}
				}catch (Exception e) { }
			}
			jedis.disconnect();
			System.out.println("Done with " + shard.toString());
		}		
	}
	
	private static void findDupSubject(String ntFilePath) throws Exception {
		BufferedReader reader = new BufferedReader(
				new FileReader(new File(ntFilePath)));
		NxParser nxParser = new NxParser(reader);
		int i = 0;
		while(nxParser.hasNext()) {
			org.semanticweb.yars.nx.Node[] nodes = nxParser.next();			
			if(nodes[0].toString().equals(
					"http://localhost/publications/articles/Journal1/1940/Article12")) {
				System.out.println(nodes[0] + "  " + nodes[1] + "  " + nodes[2]);
			}
			i++;
			if(i%1000000 == 0)
				System.out.println("Reached " + i);				
		}
		reader.close();
	}
	
	private static void testMongoDupID() {
		try {
			Mongo mongo = new MongoClient("nimbus2", 10000);
			DB localDB = mongo.getDB(Constants.MONGO_RDF_DB);
			DBCollection starSchemaCollection = 
					localDB.getCollection(Constants.MONGO_STAR_SCHEMA);
			DBObject doc = new BasicDBObject();
			doc.put("_id", "123");
			doc.put("predicate", "900");
			starSchemaCollection.insert(doc);
			try {
			DBObject doc1 = new BasicDBObject();
			doc1.put("_id", "123");
			doc1.put("predicate", "900");
			starSchemaCollection.insert(doc1);
			} catch(Exception e) { e.printStackTrace(); }
			DBObject doc2 = new BasicDBObject();
			doc2.put("_id", "789");
			doc2.put("predicate", "800");
			starSchemaCollection.insert(doc2);
			mongo.close();
		}
		catch(Exception e) { 
			System.out.println("Exception");
		}
	}
	
	private static void deleteRedisHashField() {
		String script = 
				"redis.call('DEL', 'subCount', 'typeCount') " +
				"local keys = redis.call('KEYS', '*') " +
				"for index, value in pairs(keys) do " +
					"redis.call('HDEL', value, 'str') " +
				"end ";
		Jedis localJedis = new Jedis("localhost", 6379, 
				Constants.INFINITE_TIMEOUT);
		localJedis.connect();
		long startTime = System.nanoTime();
		localJedis.eval(script, Collections.singletonList("1"), 
				Collections.singletonList("1"));
		localJedis.disconnect();
		double secs = Util.getElapsedTime(startTime);
		System.out.println("Total secs: " + secs);
	}
	
	private static void testRedisReadSpeed() {
		Jedis localJedis = new Jedis("localhost", 6379, 
				Constants.INFINITE_TIMEOUT);
		Pipeline p = localJedis.pipelined();
		System.out.println("Deleting some keys...");
		long start1 = System.nanoTime();
		localJedis.del("subCount", "typeCount", "propCount");
		System.out.println("Time taken: " + Util.getElapsedTime(start1));
		System.out.println("Retrieving all keys...");
		long startTime = System.nanoTime();
		Set<String> keys = localJedis.keys("*");
		double secs = Util.getElapsedTime(startTime);
		System.out.println("No of keys: " + keys.size());
		System.out.println("Time taken: " + secs);
		startTime = System.nanoTime();
		int count = 0;
		for(String key : keys) {
			p.hgetAll(key);
			count++;
			if(count == 10)
				break;
		}
		p.sync();
		System.out.println("Time taken: " + Util.getElapsedTime(startTime));
		System.out.println("Done");
	}
	
	private static void testMongoNullException() throws Exception {
		long predicateID = 554406;
		Mongo mongo = new MongoClient("localhost", 27017);
		DB localDB = mongo.getDB(Constants.MONGO_RDF_DB);
		DBCollection predicateSelectivityCollection = localDB.getCollection(
				Constants.MONGO_PREDICATE_SELECTIVITY);
		DBObject countDoc = predicateSelectivityCollection.findOne(
				new BasicDBObject(Constants.FIELD_HASH_VALUE, predicateID));
		System.out.println("countDoc null? " + (countDoc==null));
		int count = (Integer) countDoc.get(
				Constants.FIELD_PRED_SELECTIVITY);
		System.out.println("Count of predicate: " + count);
		
		DBObject countDoc1 = predicateSelectivityCollection.findOne(
				new BasicDBObject(Constants.FIELD_HASH_VALUE, 
						Long.toString(predicateID)));
		System.out.println("countDoc null? " + (countDoc1==null));
		mongo.close();
	}
	
	private static void testSystemProperties() {
		Properties properties = System.getProperties();
		Set<Entry<Object,Object>> entries = properties.entrySet();
		for(Entry<Object,Object> entry : entries)
			System.out.println(entry.getKey().toString() + "   " + 
					entry.getValue().toString());
	}
	
	public static void main(String[] args) throws Exception {
//		connectToMongo();
//		testDocInserts();
//		testIndexes();
//		readRDFTriples(args[0]);
//		testRandomAccess(args[0]);
//		testFindAndModify();
//		testUniqueIndex();
//		testFileMerge();
//		testSharding();
//		testUUID();
//		testRDFReader();				
//		testServerScripting();
//		testArrayInsertion();
//		testRedisScript();
//		findGraphCluster();
//		testShardedPipeline();
//		findID();
//		testMongoDupID();
//		deleteRedisHashField();
//		testRedisReadSpeed();
//		testMongoNullException();
//		testSystemProperties();
		
		String vertexRegex = "[vV]ertex.*?";
		String testStr = "vertex";
		Pattern pattern = Pattern.compile(vertexRegex);
		boolean isAMatch = pattern.matcher(testStr).matches();
		System.out.println(isAMatch);
	}
}

