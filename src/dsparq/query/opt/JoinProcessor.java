package dsparq.query.opt;

import java.util.List;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import dsparq.misc.Constants;
import dsparq.query.analysis.ConnectingRelation;
import dsparq.query.analysis.NumericalTriplePattern;
import dsparq.query.analysis.PipelinePattern;
import dsparq.query.analysis.QueryPattern;
import dsparq.query.analysis.RelationPattern;
import dsparq.query.analysis.StarPattern;

public class JoinProcessor implements Runnable {

	private List<Long> joinIDs;
	private RelationPattern relationPattern;
	private DBCollection starSchemaCollection;
	
	public JoinProcessor(List<Long> joinIDs, RelationPattern relationPattern, 
			DBCollection collection) {
		this.joinIDs = joinIDs;
		this.relationPattern = relationPattern;
		this.starSchemaCollection = collection;
	}
	
	@Override
	public void run() {
		QueryPattern queryPattern = relationPattern.getConnectingPattern();
		if(queryPattern instanceof NumericalTriplePattern) {
			NumericalTriplePattern triplePattern = 
					(NumericalTriplePattern) queryPattern;
			
			if(relationPattern.getConnectingRelation() == 
					ConnectingRelation.OBJ_SUB) 
				fetchFromDB(triplePattern, true);
			else if(relationPattern.getConnectingRelation() == 
					ConnectingRelation.OBJ_OBJ) 
				fetchFromDB(triplePattern, false);
		}
		else if(queryPattern instanceof StarPattern) {
			
		}
		else if(queryPattern instanceof PipelinePattern) {
			
		}
		else {
			try {
				throw new Exception("Unexpected Type");
			} catch(Exception e) { e.printStackTrace(); }
		}
	}
	
	private void fetchFromDB(NumericalTriplePattern triplePattern, 
			boolean isObjSub) {
		BasicDBObject queryDoc = new BasicDBObject();
		for(Long joinID : joinIDs) {
			if(isObjSub) {
				queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, joinID);
				if(triplePattern.getObject().charAt(0) != '?')
					queryDoc.put(Constants.FIELD_TRIPLE_OBJECT, 
							triplePattern.getObject());
			}
			else {
				queryDoc.put(Constants.FIELD_TRIPLE_OBJECT, joinID);
				if(triplePattern.getSubject().charAt(0) != '?')
					queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, 
							triplePattern.getSubject());
			}
			if(triplePattern.getPredicate().getEdgeLabel().
					charAt(0) != '?')
				queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, 
						triplePattern.getPredicate());			
			DBCursor cursor = starSchemaCollection.find(queryDoc);
			while(cursor.hasNext()) {
				DBObject result = cursor.next();
				//store result somewhere; print/count for now
				System.out.print("Sub: " + 
						result.get(Constants.FIELD_TRIPLE_SUBJECT));
				BasicDBList predObjList = 
						(BasicDBList) result.get(Constants.FIELD_TRIPLE_PRED_OBJ);
				for(Object predObjItem : predObjList) {
					DBObject item = (DBObject) predObjItem;
					System.out.print("  Pred: " + 
							item.get(Constants.FIELD_TRIPLE_PREDICATE));
					System.out.println("  Obj: " + 
							item.get(Constants.FIELD_TRIPLE_OBJECT));
				}
			}
		}
	}

}
