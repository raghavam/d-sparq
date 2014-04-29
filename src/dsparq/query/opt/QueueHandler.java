package dsparq.query.opt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import com.hp.hpl.jena.graph.Triple;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

import dsparq.misc.Constants;
import dsparq.query.analysis.QueryPattern;
import dsparq.query.opt.ThreadedQueryExecutor.JoinProcessor;

public class QueueHandler {

	private Map<Integer, LinkedBlockingQueue<Map<String, Long>>> idQueueMap;
	private Map<Integer, PatternContext> idPatternMap;
	private MongoClient localMongo;
	private DBCollection tripleCollection;
	private ExecutorService threadPool;
	private ConcurrentLinkedQueue<Map<String, Long>> finalResult;
 	
	public QueueHandler() {
		try {
			localMongo = new MongoClient("localhost", 10000);
			localMongo.setWriteConcern(WriteConcern.NORMAL);
			DB rdfDB = localMongo.getDB(Constants.MONGO_RDF_DB);
			tripleCollection = rdfDB.getCollection(
									Constants.MONGO_STAR_SCHEMA);
			finalResult = new ConcurrentLinkedQueue<Map<String, Long>>();
			idQueueMap = new HashMap<Integer, 
						LinkedBlockingQueue<Map<String, Long>>>();
			idPatternMap = new HashMap<Integer, PatternContext>();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void addQueuePatternContext(Integer inputQueueID, 
			QueryPattern queryPattern, Integer outputQueueID) {
		PatternContext patternContext = 
				new PatternContext(queryPattern, outputQueueID);
		idQueueMap.put(inputQueueID, 
						new LinkedBlockingQueue<Map<String, Long>>(
							Constants.CONTAINER_CAPACITY));
		idPatternMap.put(inputQueueID, patternContext);  	    
	}
	
	public void add(int bgpIndex, Map<String, Long> triple) {
		
		if(triple == null)
			System.out.println("Found triple null " + bgpIndex);
		
//		if(bgpIndex == bgps.size()-1) {
//			finalResult.add(triple);
//			return;
//		}
		boolean isElementAdded = idQueueMap.get(bgpIndex).offer(triple);
		// if queue is full, add it to the list of JoinProcessor and continue
		if(!isElementAdded) {
			spawnThread(bgpIndex);
			idQueueMap.get(bgpIndex).add(triple);
		}
	}
	
	public void doneAdding(int bgpIndex) {
		spawnThread(bgpIndex);
/*		
		elementsToProcess.clear();
		idQueueMap.get(bgpIndex).drainTo(elementsToProcess);
		if(elementsToProcess.isEmpty())
			return;
		// if its the last bgp then need not spawn another thread
		if(bgpIndex == bgps.size()-1)
			return;
		JoinProcessor joinProcessor = threadedQueryExecutor.new JoinProcessor(
				elementsToProcess, tripleCollection, bgps.get(bgpIndex+1), 
				bgpIndex+1, doneSignal);
		elementsToProcess.clear();
		threadPool.execute(joinProcessor);
*/		
	}
	
	private void spawnThread(int bgpIndex) {
		List<Map<String, Long>> elementsToProcess = 
			new ArrayList<Map<String, Long>>(idQueueMap.get(bgpIndex).size());
		idQueueMap.get(bgpIndex).drainTo(elementsToProcess);
/*		
		if((bgpNumThreadsMap.get(bgpIndex) == 0) && !firstTime) {
			doneSignal.countDown();
			System.out.println("Signal countdown for " + bgpIndex + " ; " + 
					bgpNumThreadsMap.get(bgpIndex));
		}
		firstTime = false;
*/		
		if(elementsToProcess.isEmpty()) 
			return;
		
		// if its the last bgp then need not spawn another thread
//		if(bgpIndex == bgps.size()-1)
//			return;
		
//		JoinProcessor joinProcessor = threadedQueryExecutor.new JoinProcessor(
//				elementsToProcess, tripleCollection, bgps.get(bgpIndex+1), 
//				bgpIndex+1);
//		threadPool.execute(joinProcessor);
		elementsToProcess.clear();
	}
	
	public ConcurrentLinkedQueue<Map<String, Long>> getFinalResult() {
		return finalResult;
	}
}

class PatternContext {
	QueryPattern queryPattern;
	Integer outputQueueID;
	
	PatternContext(QueryPattern queryPattern, Integer outputQueueID) {
		this.queryPattern = queryPattern;
		this.outputQueueID = outputQueueID;
	}
}

