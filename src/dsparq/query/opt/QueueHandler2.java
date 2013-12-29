package dsparq.query.opt;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import dsparq.misc.Constants;
import dsparq.query.analysis.NumericalTriplePattern;
import dsparq.query.analysis.PipelinePattern;
import dsparq.query.analysis.QueryPattern;
import dsparq.query.analysis.StarPattern;

public class QueueHandler2 {

	private LinkedBlockingQueue<Long> joinIDs;
	private QueryPattern connectingPattern;
	private ExecutorService threadPool;
	
	public QueueHandler2(QueryPattern connectingPattern, 
			ExecutorService threadExecutorService) {
		this.connectingPattern = connectingPattern;
		joinIDs = new LinkedBlockingQueue<Long>(Constants.QUEUE_CAPACITY);
		this.threadPool = threadExecutorService;
	}
	
	public void addToQueue(Long joinID) {
		if(joinID == null) {
			//when done adding all elements, null is sent to this method.
			if(joinIDs.isEmpty())
				return;
			spawnThread();
		}
		boolean isElementAdded = joinIDs.offer(joinID);
		if(!isElementAdded) {
			//create a thread and let it handle connectingPattern
			spawnThread();
			joinIDs.add(joinID);
		}
	}
	
	private void spawnThread() {
		List<Long> idsToProcess = new ArrayList<Long>(joinIDs.size());
		joinIDs.drainTo(idsToProcess);
		JoinProcessor2 joinProcessor = 
				new JoinProcessor2(idsToProcess, connectingPattern);
		threadPool.execute(joinProcessor);
	}
}

class JoinProcessor2 extends PatternHandler implements Runnable {

	private List<Long> joinIDs;
	private QueryPattern queryPattern;
	
	JoinProcessor2(List<Long> joinIDs, QueryPattern queryPattern) {
		this.joinIDs = new ArrayList<Long>(joinIDs.size());
		this.joinIDs.addAll(joinIDs);
		this.queryPattern = queryPattern;
	}
	
	@Override
	public void run() {
		synchPhaser.register();
		if(queryPattern instanceof NumericalTriplePattern) {
			NumericalTriplePattern ntp = (NumericalTriplePattern) queryPattern;
			DBObject subjectDoc = createSubjectDoc(ntp);
			DBObject predObjDoc = handleNumericalTriplePattern(ntp);
			DBObject combinedDoc = createSubPredObjDoc(subjectDoc, predObjDoc);
			DBCursor cursor;
			if(combinedDoc == null)
				cursor = starSchemaCollection.find();
			else
				cursor = starSchemaCollection.find(combinedDoc);
			System.out.println("In QueueHandler2, #docs: " + cursor.count());
		}
		else if(queryPattern instanceof StarPattern) {
			StarPattern starPattern = (StarPattern) queryPattern;
			DBObject starDoc = handleStarPattern(starPattern);
			DBCursor cursor;
			if(starDoc == null)
				cursor = starSchemaCollection.find();
			else
				cursor = starSchemaCollection.find(starDoc);
			System.out.println("In QueueHandler2, StarPattern, " +
					"#docs: " + cursor.count());
			//TODO: Check if more queues need to be created
		}
		else if(queryPattern instanceof PipelinePattern) {
			PipelinePattern pipelinePattern = (PipelinePattern) queryPattern;
			DBObject starDoc = handlePipelinePattern(pipelinePattern);
			DBCursor cursor;
			if(starDoc == null)
				cursor = starSchemaCollection.find();
			else
				cursor = starSchemaCollection.find(starDoc);
			System.out.println("In QueueHandler2, PipelinePattern, " +
					"#docs: " + cursor.count());
			//TODO: Check if more queues need to be created
		}
		else {
			try {
				throw new Exception("Unknown pattern type");
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
		synchPhaser.arrive();
	}
}
