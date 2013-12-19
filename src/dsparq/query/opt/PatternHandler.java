package dsparq.query.opt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import jsr166y.Phaser;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import dsparq.misc.Constants;
import dsparq.query.analysis.NumericalTriplePattern;
import dsparq.query.analysis.PipelinePattern;
import dsparq.query.analysis.QueryPattern;
import dsparq.query.analysis.RelationPattern;
import dsparq.query.analysis.StarPattern;

public class PatternHandler {

	protected Map<String, QueueHandler2> dependentQueueMap;
	protected Mongo localMongo;
	protected DBCollection starSchemaCollection;
	protected DB localDB;
	protected ExecutorService threadPool;
	protected Phaser synchPhaser;
	protected HashMap<String, SubObj> predSubObjMap; 
	
	public PatternHandler() {
		dependentQueueMap = new HashMap<String, QueueHandler2>();
		predSubObjMap = new HashMap<String, SubObj>();
		threadPool = Executors.newCachedThreadPool();
		try {
			localMongo = new MongoClient("nimbus5", 10000);
		}catch (Exception e) {
			e.printStackTrace();
		}
		localDB = localMongo.getDB(Constants.MONGO_RDF_DB);
		starSchemaCollection = localDB.getCollection(
				Constants.MONGO_STAR_SCHEMA);
		synchPhaser = new Phaser(1);
	}
	
	/**
	 * Handles only the predicate, object part. Subject has to handled
	 * separately by the caller since subject is not part of the "predobj" array.
	 * @param numericalTriplePattern
	 * @return
	 */
	public DBObject handleNumericalTriplePattern(
			NumericalTriplePattern numericalTriplePattern) {
		String predID = numericalTriplePattern.getPredicate().getEdgeLabel();
		String objID = numericalTriplePattern.getObject();
		boolean isPredVar = true;
		boolean isObjVar = true;
		DBObject elemMatch = null;
		if(numericalTriplePattern.getPredicate().getEdgeLabel().charAt(0) != '?')
			isPredVar = false;
		if(numericalTriplePattern.getObject().charAt(0) != '?')
			isObjVar = false;
		if(!isPredVar && !isObjVar) {
			//both predicate & object are constants
			DBObject predObjDoc = new BasicDBObject();
			predObjDoc.put(Constants.FIELD_TRIPLE_PREDICATE, 
					Long.parseLong(predID));
			predObjDoc.put(Constants.FIELD_TRIPLE_OBJECT, 
					Long.parseLong(objID));
			elemMatch = new BasicDBObject("$elemMatch", 
					predObjDoc);
		}
		else if(!isPredVar) {
			//predicate is constant
			elemMatch = new BasicDBObject("$elemMatch", 
					new BasicDBObject(Constants.FIELD_TRIPLE_PREDICATE, 
					Long.parseLong(predID)));
		}
		else if(!isObjVar) {
			//object is constant
			elemMatch = new BasicDBObject("$elemMatch", 
					new BasicDBObject(Constants.FIELD_TRIPLE_OBJECT, 
					Long.parseLong(predID)));
		}
		return elemMatch;
	}
	
	public DBObject createSubjectDoc(NumericalTriplePattern ntp) {
		DBObject subjectDoc = null;
		if(ntp.getSubject().charAt(0) != '?') {
			subjectDoc = new BasicDBObject();
			subjectDoc.put(Constants.FIELD_TRIPLE_SUBJECT, 
					Long.parseLong(ntp.getSubject()));
		}
		return subjectDoc;
	}
	
	public DBObject createSubPredObjDoc(DBObject subjectDoc, 
			DBObject predObjDoc) {
		if(subjectDoc != null && predObjDoc != null) {
			BasicDBList dbList = new BasicDBList();
			dbList.add(subjectDoc);
			dbList.add(predObjDoc);
			return dbList;
		}
		else if(subjectDoc != null)
			return subjectDoc;
		else if(predObjDoc != null)
			return predObjDoc;
		else 
			return null;
	}
	
	public DBObject handleStarPattern(StarPattern starPattern) {
		//TODO: put in selectivity. Checking speed without selectivity first.
//		reorderPatterns(starPattern);
				
		List<QueryPattern> patterns = starPattern.getQueryPatterns();
		if(patterns == null) {
			System.out.println("No patterns to work with.");
			return null;
		}
		//subject is same, pred-obj differ. 
		boolean isSubjectRead = false;
		BasicDBList conditions = new BasicDBList();
		DBObject subjectDoc = null;
		for(int i=0; i<patterns.size(); i++) {
			QueryPattern pattern = patterns.get(i);
			if(pattern instanceof NumericalTriplePattern) {
				NumericalTriplePattern ntp = (NumericalTriplePattern) pattern;
				if(!isSubjectRead) {
					subjectDoc = createSubjectDoc(ntp);
					isSubjectRead = true;
				}
				DBObject elemMatch = handleNumericalTriplePattern(ntp);
				if(elemMatch != null)
					conditions.add(elemMatch);
			}
			/* NumericalTriplePattern in PipelinePattern becomes part of this
			 * StarPattern. The ConnectingPattern is pushed into a connecting
			 * queue.
			*/
			else if(pattern instanceof PipelinePattern) {	
				PipelinePattern ppattern = (PipelinePattern) pattern;
				NumericalTriplePattern ntp = ppattern.getTriple();
				SubObj subObj;
				if(ntp.getSubject().charAt(0) != '?')
					subObj = new SubObj(ntp.getSubject(), ntp.getObject());
				else
					subObj = new SubObj(ntp.getObject());
				predSubObjMap.put(ntp.getPredicate().getEdgeLabel(), 
						subObj);
				DBObject elemMatch = handleNumericalTriplePattern(
						ntp);
				if(elemMatch != null)
					conditions.add(elemMatch);
				List<RelationPattern> relPatterns = 
						ppattern.getRelationPatterns();
				for(RelationPattern relPattern : relPatterns) {
					dependentQueueMap.put(ntp.getObject(), 
							new QueueHandler2(
									relPattern.getConnectingPattern(), 
									threadPool, synchPhaser));
				}
			}
			else {
				try {
					throw new Exception("Unknown pattern type");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		 
		if(conditions.isEmpty()) {
			//subject, predicate and object are variables. So retrieve all.
			return null;
		}
		else {
			DBObject predObjDoc;
			DBObject allDoc = new BasicDBObject("$all", conditions);
			predObjDoc = new BasicDBObject(
						Constants.FIELD_TRIPLE_PRED_OBJ, allDoc);
			if(subjectDoc != null) {
				BasicDBList docList = new BasicDBList();
				docList.add(subjectDoc);
				docList.add(predObjDoc);
				return docList;
			}
			else
				return predObjDoc;
		}	
	}
	
	public DBObject handlePipelinePattern(PipelinePattern pipelinePattern) {
		NumericalTriplePattern triple = pipelinePattern.getTriple();
		List<RelationPattern> relationPatterns = 
				pipelinePattern.getRelationPatterns();
		if(relationPatterns.isEmpty()) {
			//treat it as a NumericalTriplePattern
			DBObject subjectDoc = createSubjectDoc(triple);
			DBObject predObjDoc = handleNumericalTriplePattern(triple);
			DBObject combinedDoc = createSubPredObjDoc(subjectDoc, predObjDoc);
			return combinedDoc;
		}
		else {
			SubObj subObj;
			if(triple.getSubject().charAt(0) != '?')
				subObj = new SubObj(triple.getSubject(), triple.getObject());
			else
				subObj = new SubObj(triple.getObject());
			predSubObjMap.put(triple.getObject(), subObj);
			DBObject subjectDoc = createSubjectDoc(triple);
			DBObject predObjDoc = handleNumericalTriplePattern(triple);
			DBObject combinedDoc = createSubPredObjDoc(subjectDoc, predObjDoc);
			for(RelationPattern rpattern : relationPatterns) {
				//create a queue for each rpattern
				QueueHandler2 queueHandler = new QueueHandler2(
						rpattern.getConnectingPattern(), threadPool, synchPhaser);
				dependentQueueMap.put(triple.getObject(), queueHandler);
			}
			return combinedDoc;
		}
	}
}

class SubObj {
	String subject;
	String object;
	
	SubObj() {
		subject = object = null;
	}
	SubObj(String subject, String object) {
		this.subject = subject;
		this.object = object;
	}
	SubObj(String object) {
		subject = null;
		this.object = object;
	}
}
