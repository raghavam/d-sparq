package dsparq.query.opt;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import jsr166y.Phaser;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import dsparq.misc.Constants;
import dsparq.util.Util;

public class SampleQueryProcessor {

	private ExecutorService threadPool;
	private Mongo localMongo;
	private DBCollection starSchemaCollection;
	private Phaser synchPhaser;
	private AtomicLong nameCounter;
//	private AtomicLong personCounter;
	
	public SampleQueryProcessor() {
		threadPool = Executors.newFixedThreadPool(32);
		try {
//			mongos = new Mongo("nimbus2", 27017);
			localMongo = new Mongo("nimbus5", 10000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		DB localDB = localMongo.getDB(Constants.MONGO_RDF_DB);
		starSchemaCollection = localDB.getCollection(Constants.MONGO_STAR_SCHEMA);
		// register self - current thread
		synchPhaser = new Phaser(1);
		nameCounter = new AtomicLong(0);
//		personCounter = new AtomicLong(0);
	}
	
	public void processQuery() throws Exception {
		// processing sp2Query4/sp2Query5a
		try {
			StarQueryProcessor1 starQueryprocessor1 = new StarQueryProcessor1();
			threadPool.execute(starQueryprocessor1);
			System.out.println("Started stage1...");
			StarQueryProcessor2 starQueryProcessor2 = new StarQueryProcessor2();
			threadPool.execute(starQueryProcessor2);
			System.out.println("Started stage2...");
			synchPhaser.arriveAndAwaitAdvance();
			System.out.println("Phaser done.");
			threadPool.shutdown();
			System.out.println("Name counter: " + nameCounter.get());
//			System.out.println("Person counter: " + personCounter.get());
		}
		finally {
			localMongo.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
		GregorianCalendar start = new GregorianCalendar();
		new SampleQueryProcessor().processQuery();
		Util.getElapsedTime(start);
	}
	
	class StarQueryProcessor2 extends Thread {
		
		@Override
		public void run() {
			synchPhaser.register();
			BasicDBList conditions = new BasicDBList();
			BasicDBObject elemMatchValue = new BasicDBObject();
			elemMatchValue.put("predicate", new Long(82));
			elemMatchValue.put("object", new Long(42112838));
			BasicDBObject elemMatchOp = new BasicDBObject("$elemMatch", elemMatchValue);
			BasicDBObject clause1 = new BasicDBObject("predobj", elemMatchOp);
			BasicDBObject clause2 = new BasicDBObject("predobj.predicate", new Long(19));
			conditions.add(clause1);
			conditions.add(clause2);
			BasicDBObject andOp = new BasicDBObject("$and", conditions);
			DBCursor cursor = starSchemaCollection.find(andOp);
			List<Long> authorList = new ArrayList<Long>();
			while(cursor.hasNext()) {
				DBObject result = cursor.next();
				BasicDBList predObjList = 
					(BasicDBList) result.get(Constants.FIELD_TRIPLE_PRED_OBJ);
				for(Object predObjItem : predObjList) {
					DBObject item = (DBObject) predObjItem;
					if((Long)item.get(Constants.FIELD_TRIPLE_PREDICATE) == 19) {
						authorList.add((Long)item.get(Constants.FIELD_TRIPLE_OBJECT));
						if(authorList.size() >= Constants.CONTAINER_CAPACITY) {
							PathQueryProcessor processor = new PathQueryProcessor(authorList);
							threadPool.execute(processor);
							authorList.clear();
						}
					}
				}
			}
			synchPhaser.arrive();
		}
	}
	
	class StarQueryProcessor1 extends Thread {
				
		@Override
		public void run() {
			synchPhaser.register();
			BasicDBList conditions = new BasicDBList();
			BasicDBObject elemMatchValue = new BasicDBObject();
			elemMatchValue.put("predicate", new Long(82));
			elemMatchValue.put("object", new Long(40573065));
			BasicDBObject elemMatchOp = new BasicDBObject("$elemMatch", elemMatchValue);
			BasicDBObject clause1 = new BasicDBObject("predobj", elemMatchOp);
			BasicDBObject clause2 = new BasicDBObject("predobj.predicate", new Long(19));
			conditions.add(clause1);
			conditions.add(clause2);
			BasicDBObject andOp = new BasicDBObject("$and", conditions);
			DBCursor cursor = starSchemaCollection.find(andOp);
			List<Long> authorList = new ArrayList<Long>();
			while(cursor.hasNext()) {
				DBObject result = cursor.next();
				BasicDBList predObjList = 
					(BasicDBList) result.get(Constants.FIELD_TRIPLE_PRED_OBJ);
				for(Object predObjItem : predObjList) {
					DBObject item = (DBObject) predObjItem;
					if((Long)item.get(Constants.FIELD_TRIPLE_PREDICATE) == 19) {
						authorList.add((Long)item.get(Constants.FIELD_TRIPLE_OBJECT));
						if(authorList.size() >= Constants.CONTAINER_CAPACITY) {
							PathQueryProcessor processor = new PathQueryProcessor(authorList);
							threadPool.execute(processor);
							authorList.clear();
						}
					}
				}
			}
			synchPhaser.arrive();
		}
	}
	
	class PathQueryProcessor extends Thread {
		
		private List<Long> idList;
		
		PathQueryProcessor(List<Long> idList) {
			this.idList = new ArrayList<Long>(idList.size());
			this.idList.addAll(idList);
		}
		
		@Override
		public void run() {
			synchPhaser.register();
			BasicDBObject doc = new BasicDBObject();
			for(Long id : idList) {
				doc.put(Constants.FIELD_TRIPLE_SUBJECT, id);
				doc.put(Constants.FIELD_TRIPLE_PRED_OBJ + "." + 
						Constants.FIELD_TRIPLE_PREDICATE, new Long(83));
				DBCursor cursor = starSchemaCollection.find(doc);
				while(cursor.hasNext()) {
					cursor.next();
					nameCounter.incrementAndGet();
				}
			}			
			synchPhaser.arrive();
		}
	}
}
