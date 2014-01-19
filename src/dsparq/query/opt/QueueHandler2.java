package dsparq.query.opt;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import dsparq.misc.Constants;
import dsparq.query.analysis.ConnectingRelation;
import dsparq.query.analysis.NumericalTriplePattern;
import dsparq.query.analysis.PipelinePattern;
import dsparq.query.analysis.QueryPattern;
import dsparq.query.analysis.RelationPattern;
import dsparq.query.analysis.StarPattern;

public class QueueHandler2 {

	private LinkedBlockingQueue<Long> joinIDs;
	private RelationPattern connectingRelationPattern;
	private ExecutorService threadPool;
	
	public QueueHandler2(RelationPattern connectingRelationPattern, 
			ExecutorService threadExecutorService) {
		this.connectingRelationPattern = connectingRelationPattern;
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
		else {
			boolean isElementAdded = joinIDs.offer(joinID);
			if(!isElementAdded) {
				//create a thread and let it handle connectingPattern
				spawnThread();
				joinIDs.add(joinID);
			}			
		}
	}
	
	private void spawnThread() {
		List<Long> idsToProcess = new ArrayList<Long>(joinIDs.size());
		joinIDs.drainTo(idsToProcess);
		JoinProcessor2 joinProcessor = 
				new JoinProcessor2(idsToProcess, connectingRelationPattern);
		threadPool.execute(joinProcessor);
	}
}

class JoinProcessor2 extends PatternHandler implements Runnable {

	private List<Long> joinIDs;
	private RelationPattern connectingRelationPattern;
	
	JoinProcessor2(List<Long> joinIDs, 
			RelationPattern connectingRelationPattern) {
		this.joinIDs = new ArrayList<Long>(joinIDs.size());
		this.joinIDs.addAll(joinIDs);
		this.connectingRelationPattern = connectingRelationPattern;
	}
	
	@Override
	public void run() {
		synchPhaser.register();
		QueryPattern queryPattern = 
				connectingRelationPattern.getConnectingPattern();
		if(queryPattern instanceof NumericalTriplePattern) {
			NumericalTriplePattern ntp = (NumericalTriplePattern) queryPattern;
			for(Long joinID : joinIDs) {
				DBObject subjectDoc; 
				DBObject predObjDoc;
				if(connectingRelationPattern.getConnectingRelation() == 
						ConnectingRelation.OBJ_SUB)
					subjectDoc = createSubjectDoc(ntp, joinID);
				else
					subjectDoc = createSubjectDoc(ntp, null);
				if(connectingRelationPattern.getConnectingRelation() == 
						ConnectingRelation.OBJ_OBJ)
					predObjDoc = handleNumericalTriplePattern(ntp, joinID);
				else
					predObjDoc = handleNumericalTriplePattern(ntp, null);
				DBObject combinedDoc = createSubPredObjDoc(subjectDoc, predObjDoc);
				DBCursor cursor;
				if(combinedDoc == null)
					cursor = starSchemaCollection.find().limit(LIMIT_RESULTS);
				else
					cursor = starSchemaCollection.find(combinedDoc).limit(LIMIT_RESULTS);
				long count = 0;
				while(cursor.hasNext()) {
					cursor.next();
					count++;
				}
				System.out.println("In QueueHandler2, NumericalTriplePattern, " +
						"#docs: " + count);
			}
		}
		else if(queryPattern instanceof StarPattern) {
			StarPattern starPattern = (StarPattern) queryPattern;
			System.out.println("#JoinIDs: " + joinIDs.size());
			for(Long joinID : joinIDs) {
				System.out.println("QHandler2, Star, joinID: " + joinID);
				DBObject starDoc = handleStarPattern(starPattern, 
						connectingRelationPattern.getConnectingRelation(), 
						connectingRelationPattern.getConnectingVariable(), joinID);
				if(starDoc != null)
					System.out.println("starDoc: " + starDoc.toString());
				DBCursor cursor;
				if(starDoc == null)
					cursor = starSchemaCollection.find().limit(LIMIT_RESULTS);
				else
					cursor = starSchemaCollection.find(starDoc).limit(LIMIT_RESULTS);
				System.out.println("Cursor... ");
				try {
					System.out.println("In QueueHandler2, StarPattern, " +
						"#docs: " + cursor.count());
				}catch(Exception e) { e.printStackTrace(); }
				System.out.println("Cursor done...");
				//TODO: Check if more queues need to be created
			}
		}
		else if(queryPattern instanceof PipelinePattern) {
			for(Long joinID : joinIDs) {
				DBObject queryDoc = handlePipelinePattern(
						connectingRelationPattern, joinID);
				DBCursor cursor;
				if(queryDoc == null)
					cursor = starSchemaCollection.find().limit(LIMIT_RESULTS);
				else
					cursor = starSchemaCollection.find(queryDoc).limit(LIMIT_RESULTS);
				System.out.println("In QueueHandler2, PipelinePattern, " +
						"#docs: " + cursor.count());
				//TODO: Check if more queues need to be created
			}
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
