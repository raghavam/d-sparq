package dsparq.query.opt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import dsparq.misc.Constants;
import dsparq.misc.HostInfo;
import dsparq.misc.PropertyFileHandler;
import dsparq.query.analysis.ConnectingRelation;
import dsparq.query.analysis.NumericalTriplePattern;
import dsparq.query.analysis.PipelinePattern;
import dsparq.query.analysis.QueryPattern;
import dsparq.query.analysis.RelationPattern;
import dsparq.query.analysis.StarPattern;

public class PatternHandler {

	protected Mongo localMongo;
	protected DBCollection starSchemaCollection;
	protected DB localRdfDB;
	protected ExecutorService threadPool;
	protected Phaser synchPhaser;
	/*Key: Predicates. They are definitely constants (in sp2 modified queries).
	 *Value: Combination of subject & object. Object is useful for pipelines.
	 *		 Subject is used for further verification.
	 */
	protected HashMap<Long, SubObj> predSubObjMap; 
	//Object is the key. This is useful for pipeline patterns.
	protected Map<String, QueueHandler2> dependentQueueMap;
	private DBCollection predicateSelectivityCollection;
	protected PropertyFileHandler propertyFileHandler;
	private Mongo routerMongo;
	
	protected final int LIMIT_RESULTS = 100;			//for testing, remove later
	
	public PatternHandler() {
		dependentQueueMap = new HashMap<String, QueueHandler2>();
		predSubObjMap = new HashMap<Long, SubObj>();
		threadPool = Executors.newCachedThreadPool();
		propertyFileHandler = PropertyFileHandler.getInstance();
		HostInfo routerHostInfo = propertyFileHandler.getMongoRouterHostInfo();
		try {
			localMongo = new MongoClient("localhost", 
					propertyFileHandler.getShardPort());
			routerMongo = new MongoClient(routerHostInfo.getHost(), 
					routerHostInfo.getPort());
		}catch (Exception e) {
			e.printStackTrace();
		}
		localRdfDB = localMongo.getDB(Constants.MONGO_RDF_DB);
		DB routerRdfDB = routerMongo.getDB(Constants.MONGO_RDF_DB);
		predicateSelectivityCollection = routerRdfDB.getCollection(
				Constants.MONGO_PREDICATE_SELECTIVITY);
		starSchemaCollection = localRdfDB.getCollection(
				Constants.MONGO_STAR_SCHEMA);
		synchPhaser = new Phaser(1);
	}
	
	/**
	 * Handles only the predicate, object part. Subject has to handled
	 * separately by the caller since subject is not part of the "predobj" array.
	 * @param numericalTriplePattern
	 * @param objJoinID the join value to be used for object. null is used 
	 * 					in cases where the join should not be considered.
	 * @return
	 */
	public DBObject handleNumericalTriplePattern(
			NumericalTriplePattern numericalTriplePattern, Long objJoinID) {
		String predID = numericalTriplePattern.getPredicate().getEdgeLabel();
		String objID = (objJoinID != null) ? Long.toString(objJoinID) : 
			numericalTriplePattern.getObject();
		boolean isPredVar = true;
		boolean isObjVar = true;
		DBObject elemMatch = null;
		if(predID.charAt(0) != '?')
			isPredVar = false;
		if(objID.charAt(0) != '?')
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
					Long.parseLong(objID)));
		}
		return elemMatch;
	}
	
	public DBObject createSubjectDoc(NumericalTriplePattern ntp, Long subJoinID) {
		DBObject subjectDoc = new BasicDBObject();
		if(subJoinID != null)
			subjectDoc.put(Constants.FIELD_TRIPLE_SUBJECT, subJoinID);
		else if(ntp.getSubject().charAt(0) != '?') {
			subjectDoc.put(Constants.FIELD_TRIPLE_SUBJECT, 
					Long.parseLong(ntp.getSubject()));
		}
		else
			subjectDoc = null;
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
	
	public DBObject handleStarPattern(StarPattern starPattern, 
			ConnectingRelation connectingRelation, String connectingVariable, 
			Long joinID) {				
		List<QueryPattern> patterns = starPattern.getQueryPatterns();
		if(patterns == null) {
			System.out.println("No patterns to work with.");
			return null;
		}
		//predicate selectivity based reordering
		reorderPatterns(starPattern);
				
		//subject is same, pred-obj differ. 
		boolean isSubjectRead = false;
		BasicDBList conditions = new BasicDBList();
		DBObject subjectDoc = null;
		for(int i=0; i<patterns.size(); i++) {
			QueryPattern pattern = patterns.get(i);
			if(pattern instanceof NumericalTriplePattern) {
				NumericalTriplePattern ntp = (NumericalTriplePattern) pattern;
				if(!isSubjectRead) {
					if(connectingRelation == null)
						subjectDoc = createSubjectDoc(ntp, null);
					else if(connectingRelation == ConnectingRelation.OBJ_SUB)
						subjectDoc = createSubjectDoc(ntp, joinID);
					isSubjectRead = true;
				}
				DBObject elemMatch;
				if(connectingRelation == ConnectingRelation.OBJ_OBJ) {
					//check if the object of this pattern matches with
					//the connecting variable
					if(ntp.getObject().equals(connectingVariable)) {
						elemMatch = handleNumericalTriplePattern(ntp, joinID);
					}
					else
						elemMatch = handleNumericalTriplePattern(ntp, null);
				}
				else
					elemMatch = handleNumericalTriplePattern(ntp, null);
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
				SubObj subObj = new SubObj(ntp.getSubject(), ntp.getObject());
				predSubObjMap.put(Long.valueOf(
						ntp.getPredicate().getEdgeLabel()), subObj);
				DBObject elemMatch = handleNumericalTriplePattern(
						ntp, null);
				if(elemMatch != null)
					conditions.add(elemMatch);
				List<RelationPattern> relPatterns = 
						ppattern.getRelationPatterns();
				if(!relPatterns.isEmpty()) {
					if(relPatterns.size() > 1) {
						try {
							throw new Exception("Expecting only 1 pattern");
						}catch(Exception e) { e.printStackTrace(); }
					}
					dependentQueueMap.put(ntp.getObject(), 
							new QueueHandler2(relPatterns.get(0), threadPool));
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
	
	/**
	 * Based on selectivity of the predicates, the patterns are reordered.
	 * @param starPattern StarPattern to reorder
	 */
	private StarPattern reorderPatterns(StarPattern starPattern) {
		List<QueryPattern> patterns = starPattern.getQueryPatterns();
		List<PositionScore> posScoreList = 
				new ArrayList<PositionScore>(patterns.size());
		StarPattern reorderedStarPattern = new StarPattern();
		for(int i=0; i<patterns.size(); i++) {
			QueryPattern pattern = patterns.get(i);
			NumericalTriplePattern ntp = null;
			if(pattern instanceof NumericalTriplePattern) {
				ntp = (NumericalTriplePattern) pattern;
			}
			else if(pattern instanceof PipelinePattern) {
				PipelinePattern pipelinePattern = (PipelinePattern) pattern;
				ntp = pipelinePattern.getTriple();
			}
			//else: cannot be any other type
			
			String predID = ntp.getPredicate().getEdgeLabel();
			if(predID.charAt(0) != '?') {
				DBObject countDoc = predicateSelectivityCollection.findOne(
						new BasicDBObject(Constants.FIELD_HASH_VALUE, 
								Long.valueOf(predID)));
				int count = (Integer) countDoc.get(
						Constants.FIELD_PRED_SELECTIVITY);
				posScoreList.add(new PositionScore(i, count));
			}
			else {
				//make this the largest count predicate
				posScoreList.add(new PositionScore(i, Integer.MAX_VALUE));
			}
		}
		routerMongo.close();
		Collections.sort(posScoreList, new ScoreComparator());
		for(PositionScore pscore : posScoreList) {
			reorderedStarPattern.addQueryPattern(
					patterns.get(pscore.position));
		}
		return reorderedStarPattern;
	}
	
	public DBObject handlePipelinePattern(PipelinePattern pipelinePattern) {
		NumericalTriplePattern triple = pipelinePattern.getTriple();
		DBObject subjectDoc = createSubjectDoc(triple, null);
		DBObject predObjDoc = handleNumericalTriplePattern(triple, null);
		DBObject combinedDoc = createSubPredObjDoc(subjectDoc, predObjDoc);
		List<RelationPattern> relationPatterns = 
				pipelinePattern.getRelationPatterns();
		if(relationPatterns.isEmpty()) {
			//treat it as a NumericalTriplePattern
			return combinedDoc;
		}
		else {
			handleRelationPatterns(triple, relationPatterns);
			return combinedDoc;
		}
	}
	
	public DBObject handlePipelinePattern(RelationPattern relationPattern, 
			Long joinID) {
		PipelinePattern pipelinePattern = (PipelinePattern) 
				relationPattern.getConnectingPattern();
		NumericalTriplePattern triple = pipelinePattern.getTriple();
		//use joinID as either subject or object depending on the relation
		//in connecting pattern
		DBObject subjectDoc;
		DBObject predObjDoc;
		ConnectingRelation connectingRelation = 
				relationPattern.getConnectingRelation();
		if(connectingRelation == ConnectingRelation.OBJ_SUB) {
			subjectDoc = createSubjectDoc(triple, joinID);
			predObjDoc = handleNumericalTriplePattern(triple, null);
		}
		else {
			//join on object
			subjectDoc = createSubjectDoc(triple, null);
			predObjDoc = handleNumericalTriplePattern(triple, joinID);
		}
		DBObject combinedDoc = createSubPredObjDoc(subjectDoc, predObjDoc);
		List<RelationPattern> relationPatterns = 
				pipelinePattern.getRelationPatterns();
		if(relationPatterns.isEmpty()) {
			//treat it as a NumericalTriplePattern
			return combinedDoc;
		}
		else {
			handleRelationPatterns(triple, relationPatterns);
			return combinedDoc;
		}
	}
	
	private void handleRelationPatterns(NumericalTriplePattern triple, 
			List<RelationPattern> relationPatterns) {
		SubObj subObj = new SubObj(triple.getSubject(), triple.getObject());
		predSubObjMap.put(Long.valueOf(
				triple.getPredicate().getEdgeLabel()), subObj);
		for(RelationPattern rpattern : relationPatterns) {
			//create a queue for each rpattern
			QueueHandler2 queueHandler = new QueueHandler2(
					rpattern, threadPool);
			//same for ConnectingRelation.OBJ_SUB and 
			//ConnectingRelation.OBJ_OBJ
			dependentQueueMap.put(triple.getObject(), queueHandler);
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

class PositionScore {
	int position;
	int score;
	
	PositionScore(int position, int score) {
		this.position = position;
		this.score = score;
	}
}

class ScoreComparator implements Comparator<PositionScore> {

	@Override
	public int compare(PositionScore ps1, PositionScore ps2) {
		return ps1.score - ps2.score;
	}
	
}
