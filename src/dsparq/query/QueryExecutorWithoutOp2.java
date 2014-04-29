package dsparq.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpAsQuery;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.util.FmtUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import dsparq.misc.Constants;
import dsparq.misc.HostInfo;
import dsparq.misc.PropertyFileHandler;
import dsparq.util.Util;

public class QueryExecutorWithoutOp2 {
	
	private Mongo mongoS;
	private Mongo shardMongo;
	private DBCollection tripleCollection; 
	private DBCollection idValCollection;
	private DBCollection eidValCollection;
	
	public QueryExecutorWithoutOp2() {
		try {
			PropertyFileHandler propertyFileHandler = PropertyFileHandler.getInstance();
			HostInfo mongosHostInfo = propertyFileHandler.getMongoRouterHostInfo();
			mongoS = new Mongo(mongosHostInfo.getHost(), mongosHostInfo.getPort());
			shardMongo = new Mongo(Constants.MONGO_LOCAL_HOST, 10000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		DB ldb = shardMongo.getDB(Constants.MONGO_RDF_DB);
		DB sdb = mongoS.getDB(Constants.MONGO_RDF_DB);
		tripleCollection = ldb.getCollection(Constants.MONGO_PARTITIONED_VERTEX_COLLECTION);
		idValCollection = sdb.getCollection(Constants.MONGO_IDVAL_COLLECTION);
		eidValCollection = sdb.getCollection(Constants.MONGO_EIDVAL_COLLECTION);
	}

	public void processQuery2(String query) throws Exception {
		Query queryObj = QueryFactory.create(query);
		QueryVisitor queryVisitor = new QueryVisitor(queryObj);
		Op op = Algebra.compile(queryObj);
		op.visit(queryVisitor);
		List<Triple> basicGraphPatterns = queryVisitor.getBasicGraphPatterns();		
		
		// fetch data for first one
		Table<Long, String, Long> answerTable = fetchDataFromDB(
												basicGraphPatterns.get(0));
		
		if(basicGraphPatterns.size() > 1) {
			// compare the subject/object with the previous BGPs and
			// fetch data according to the joins
			Table<Long,String,Long> newAnswerTable = HashBasedTable.create();
			for(int i=1; i<basicGraphPatterns.size(); i++) {
				Triple bgp = basicGraphPatterns.get(i);
				Node subject = bgp.getSubject();
				Node predicate = bgp.getPredicate();
				Node object = bgp.getObject();
				boolean isObjVar = object.isVariable();
				String objStr = FmtUtils.stringForNode(object);
				String subStr = FmtUtils.stringForNode(subject);
				
				if(subject.isVariable()) {
					// check if this sub is present in answerTable column
					if(answerTable.containsColumn(subStr)) {
						Map<Long,Long> rowVals = answerTable.column(
													subStr);
						Collection<Long> values = rowVals.values();
						BasicDBObject queryDoc = new BasicDBObject();
						for(Long val : values) {
							// fetch data from DB with this as key
							queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, val);
							if(!predicate.isVariable())
								queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, 
									getIDFromStrVal(FmtUtils.stringForNode(
											predicate), true));
							if(!isObjVar) {
								queryDoc.put(Constants.FIELD_TRIPLE_OBJECT, 
										getIDFromStrVal(objStr, false));
								DBObject result = tripleCollection.findOne(queryDoc);
								if(result != null) {
									// add this row to newAnswerTable
									Map<String,Long> colVals = answerTable.row(val);
									Set<Entry<String,Long>> entries = colVals.entrySet();
									for(Entry<String,Long> entry : entries)
										newAnswerTable.put(val, entry.getKey(), entry.getValue());
								}	
							}
							else {
								int resultSize = tripleCollection.find(queryDoc).count();
								if(resultSize > 1)
									throw new Exception("Cannot handle if > 1");
								else if(resultSize == 1) {
									DBObject resultDoc = tripleCollection.findOne(queryDoc);
									Map<String,Long> colVals = answerTable.row(val);
									Set<Entry<String,Long>> entries = colVals.entrySet();
									for(Entry<String,Long> entry : entries)
										newAnswerTable.put(val, entry.getKey(), entry.getValue());
									newAnswerTable.put(val, objStr, 
											(Long)resultDoc.get(Constants.FIELD_TRIPLE_OBJECT));
								}
							}	
							queryDoc.clear();
						}
					}
				}
				
				if(object.isVariable()) {
					if(subject.isVariable()) {
						// check in the new answers table and filter further
						// assign results to new(new answer table)? -- remove this row
						// from newAnswerTable
						if(answerTable.containsColumn(objStr)) {
							Map<Long,Long> rowVals = answerTable.column(
									objStr);
							Collection<Long> values = rowVals.values();
							Map<Long,Long> newRowVals = newAnswerTable.column(objStr);
							Collection<Long> newValues = newRowVals.values();
							// retain only the common values between new & old row vals
							Set<Long> oldvals = new HashSet(values);
							Set<Long> newvals = new HashSet(newValues);
							oldvals.retainAll(newvals);
							// column() method is a view over underlying table, so if 
							// this view is modified, underlying table would be modified as well.
//							for(Long val : oldvals)
//								if(!newRowVals.containsValue(val))
//									newRowVals.remove(arg0)
						}
						// if this var is not present in answerTable then nothing
						// need to be done.
					}
					else {
						// fetch data from DB with this as key
						joinOnObject(objStr,answerTable,subStr,predicate,newAnswerTable);
					}
				}
				answerTable.clear();
				answerTable.putAll(newAnswerTable);
				newAnswerTable.clear();
			}
		}
		
		// print answer
/*		
		// convert the IDs to Strings
		System.out.println("Query answer\n");
		for(String var : answerTable.getBgpVars())
			System.out.print(var + "  ");
		System.out.println("\n");
		// TODO: setting isPredicate to false for query3 (for most queries 
		//		 this should be true)
		boolean isPredicate = false;
		for(Map<String, Long> varIDMap : answerTable.getTripleRows()) {
			Set<Entry<String, Long>> entries = varIDMap.entrySet();
			for(Entry<String, Long> entry : entries)
				System.out.println(entry.getKey() + " : " + 
						getStrValFromID(entry.getValue(), isPredicate));
			System.out.println();
		}
*/		
		mongoS.close();
		shardMongo.close();
	}
	
	private void joinOnObject(String objLabel, Table<Long,String,Long> 
				answerTable, String subStr, Node predicate, 
				Table<Long,String,Long> newAnswerTable) throws Exception {
		if(answerTable.containsColumn(objLabel)) {
			Map<Long,Long> rowVals = answerTable.column(objLabel);
			Collection<Long> values = rowVals.values();
			BasicDBObject queryDoc = new BasicDBObject();
			for(Long val : values) {
				// fetch data from DB with this as key
				queryDoc.put(Constants.FIELD_TRIPLE_OBJECT, val);
				queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, 
						getIDFromStrVal(subStr, false));
				if(!predicate.isVariable())
					queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, 
						getIDFromStrVal(FmtUtils.stringForNode(
								predicate), true));
				int resultSize = tripleCollection.find(queryDoc).count();
				if(resultSize > 1)
					throw new Exception("Cannot handle if > 1");
				else if(resultSize == 1) {
					DBObject result = tripleCollection.findOne(queryDoc);
					if(result != null) {
						// add this row to newAnswerTable
						Map<String,Long> colVals = answerTable.row(val);
						Set<Entry<String,Long>> entries = colVals.entrySet();
						for(Entry<String,Long> entry : entries)
							newAnswerTable.put(val, entry.getKey(), entry.getValue());
					}
				}				
				queryDoc.clear();
			}
		}
	}
	
	private TriplesHolder checkAndPerformJoins(TriplesHolder prevBGPResults, 
			TriplesHolder currResultBGP, List<String> queryVars) {
		TriplesHolder filteredTripleTable = new TripleTable();
		Set<String> commonVars = prevBGPResults.getBgpVars();
		commonVars.retainAll(currResultBGP.getBgpVars());
		
		for(Map<String, Long> varIDMap1 : prevBGPResults.getTripleRows())
			for(Map<String, Long> varIDMap2 : currResultBGP.getTripleRows()) {				
				int i = 0;
				for(String var : commonVars) {
					if(varIDMap1.get(var).longValue() == 
						varIDMap2.get(var).longValue()) {
						i++;
						continue;
					}
					else {
						// all common variables should match(be equal) for the join.
						break;
					}
				}
				if(i == commonVars.size()) {
					// all common variables matched. So add this record to the
					// answer table, along with the required variables
					
					// get the values of whatever select clause vars are present 
					// in these two tables.
					Map<String, Long> resultIDMap = new HashMap<String, Long>();
					Set<String> resultBGPVars = new HashSet<String>();
					// assumption: if the common vars have to be checked against
					//			   the next BGP then they should be in the select
					//			   clause.
					for(String v : queryVars) {
						if(prevBGPResults.getBgpVars().contains(v)) {
							resultIDMap.put(v, varIDMap1.get(v));
							resultBGPVars.add(v);
						}
						else if(currResultBGP.getBgpVars().contains(v)) {
							resultIDMap.put(v, varIDMap2.get(v));
							resultBGPVars.add(v);
						}
					}
					filteredTripleTable.addTriple(resultIDMap);
					filteredTripleTable.setBgpVars(resultBGPVars);
				}					
			}
		
		return filteredTripleTable;
	}
	
	private Table<Long,String,Long> fetchDataFromDB(Triple bgp) throws Exception {
		Node subject = bgp.getSubject();
		Node predicate = bgp.getPredicate();
		Node object = bgp.getObject();
		
		Table<Long,String,Long> tripleTable = HashBasedTable.create();		
		BasicDBObject queryDoc = new BasicDBObject();
		boolean isSubVar = true;
		boolean isPredicate = false;
		if(!subject.isVariable()) { 
			queryDoc.put(Constants.FIELD_TRIPLE_SUBJECT, 
					getIDFromStrVal(FmtUtils.stringForNode(subject), isPredicate));
			isSubVar = false;
		}
		
		boolean isPredVar = true;
		if(!predicate.isVariable()) {
			isPredicate = true;
			queryDoc.put(Constants.FIELD_TRIPLE_PREDICATE, 
					getIDFromStrVal(FmtUtils.stringForNode(predicate), isPredicate));
			isPredVar = false;
		}
		
		boolean isObjVar = true;
		if(!object.isVariable()) {
			isPredicate = false;
			queryDoc.put(Constants.FIELD_TRIPLE_OBJECT, 
					getIDFromStrVal(FmtUtils.stringForNode(object), isPredicate));
			isObjVar = false;
		}
		
		DBCursor resultCursor;
		String subStr = subject.toString();
		String predStr = predicate.toString();
		String objStr = object.toString();
		
		if(isSubVar && isPredVar && isObjVar) {
			// this means all the 3 are variables in the form ?s ?p ?o
			// so fetch all the data
			resultCursor = tripleCollection.find();
			
			long rowCount = 1;
			while(resultCursor.hasNext()) {
				DBObject tripleRow = resultCursor.next();
				tripleTable.put(rowCount, subStr, 
						(Long)tripleRow.get(Constants.FIELD_TRIPLE_SUBJECT));
				tripleTable.put(rowCount, predStr, 
						(Long)tripleRow.get(Constants.FIELD_TRIPLE_PREDICATE));
				tripleTable.put(rowCount, objStr, 
						(Long)tripleRow.get(Constants.FIELD_TRIPLE_OBJECT));
				rowCount++;
			}
		}
		else {
			
			// assumption: predicates as variable is not considered here. 
			// This information is used to perform joins. So it is not 
			// considered and if predicates are in the select clause variables 
			// (to be displayed) then it is ignored.
			
			BasicDBObject fields = new BasicDBObject();
			long rowCount = 1;
			if(isSubVar) {
				fields.put(Constants.FIELD_TRIPLE_SUBJECT, 1);
				if(isObjVar) {
					fields.put(Constants.FIELD_TRIPLE_OBJECT, 1);
					resultCursor = tripleCollection.find(queryDoc, fields);
					while(resultCursor.hasNext()) {
						DBObject tripleRow = resultCursor.next();
						tripleTable.put(rowCount, subStr, 
								(Long)tripleRow.get(Constants.FIELD_TRIPLE_SUBJECT));
						tripleTable.put(rowCount, objStr, 
								(Long)tripleRow.get(Constants.FIELD_TRIPLE_OBJECT));
						rowCount++;
					}
				}
				else {
					resultCursor = tripleCollection.find(queryDoc, fields);
					while(resultCursor.hasNext()) {
						DBObject tripleRow = resultCursor.next();
						tripleTable.put(rowCount, subStr, 
								(Long)tripleRow.get(Constants.FIELD_TRIPLE_SUBJECT));
						rowCount++;
					}
				}
			}
			else {
				if(isObjVar) {
					fields.put(Constants.FIELD_TRIPLE_OBJECT, 1);
					resultCursor = tripleCollection.find(queryDoc, fields);
					while(resultCursor.hasNext()) {
						DBObject tripleRow = resultCursor.next();
						tripleTable.put(rowCount, objStr, 
								(Long)tripleRow.get(Constants.FIELD_TRIPLE_OBJECT));
						rowCount++;
					}
				}
			}
		}
		return tripleTable;
	}
	
	private long getIDFromStrVal(String value, boolean isPredicate) throws Exception {
		// get hash digest of this value and then query DB
		String digestValue = Util.generateMessageDigest(value);
		DBObject resultID = null;
		if(isPredicate) 
			resultID = eidValCollection.findOne(new BasicDBObject(
					Constants.FIELD_HASH_VALUE, digestValue));
		else 
			resultID = idValCollection.findOne(new BasicDBObject(
								Constants.FIELD_HASH_VALUE, digestValue));
		if(resultID == null)
			throw new Exception("ID not found for: " + value + 
					" and its digest value: " + digestValue);
		return (Long)resultID.get(Constants.FIELD_ID);
	}
	
	private String getStrValFromID(long id, boolean isPredicate) throws Exception {
		DBObject resultDoc = null;
		if(isPredicate)
			resultDoc = eidValCollection.findOne(new BasicDBObject(
					Constants.FIELD_ID, id));
		else
			resultDoc = idValCollection.findOne(new BasicDBObject(
								Constants.FIELD_ID, id));
		if(resultDoc == null)
			throw new Exception("Str value not found for: " + id);
		return (String)resultDoc.get(Constants.FIELD_STR_VALUE);
	}
	
	public static void main(String[] args) throws Exception {
		// query-1 in LUBM
		String testQuery1 = "SELECT ?x WHERE { " +
			"?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
				"<http://lubm.example.org#GraduateStudent> . " + 
			"?x <http://lubm.example.org#takesCourse> " +
				"<http://www.Department0.University0.edu/GraduateCourse0> " +
			"} ";
		
		
		// query-2 in LUBM
		String testQuery2 = "SELECT ?x ?y ?z WHERE { " +
	"?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
	"	<http://lubm.example.org#GraduateStudent> . " + 
	"?y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
	"	<http://lubm.example.org#University> . " +
	"?z <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
	"	<http://lubm.example.org#Department> . " + 
	"?x <http://lubm.example.org#memberOf> ?z . " + 
	"?z <http://lubm.example.org#subOrganizationOf> ?y . " + 
	"?x <http://lubm.example.org#undergraduateDegreeFrom> ?y " +
	"} ";
		
		// query-3 in LUBM 
		// TODO: add more BGPs to this, for testing
		String testQuery3 = "SELECT ?x WHERE { " +
			"?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
				"<http://lubm.example.org#Publication> . " + 
			"?x <http://lubm.example.org#publicationAuthor> " +
				"<http://www.Department0.University0.edu/AssistantProfessor0> " +
			"} ";
		
		// query-4 in LUBM -- this requires some inferencing. "Professor" is
		// not found in the triples
		String testQuery4 = "SELECT ?x ?y1 ?y2 ?y3 WHERE { " +
			"?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
				"<http://lubm.example.org#Professor> . " + 
			"?x <http://lubm.example.org#worksFor> " +
				"<http://www.Department0.University0.edu> . " + 
			"?x <http://lubm.example.org#name> ?y1 . " + 
			"?x <http://lubm.example.org#emailAddress> ?y2 . " +
			"?x <http://lubm.example.org#telephone> ?y3 " +
			"} ";
		
		new QueryExecutorWithoutOp2().processQuery2(testQuery3);
	}
}

/*
class QueryVisitor extends OpAsQuery.Converter {
	
	private List<Triple> basicGraphPatterns;
	private List<String> queryVars;

	public QueryVisitor(Query query) {
		super(query);
		basicGraphPatterns = new ArrayList<Triple>();
		queryVars = new ArrayList<String>();
	}
	
	public List<Triple> getBasicGraphPatterns() {
		return basicGraphPatterns;
	}
	
	public List<String> getQueryVariables() {
		return queryVars;
	}

	@Override
	public void visit(OpBGP bgp) {
		BasicPattern bp = bgp.getPattern();
		Iterator<Triple> triples = bp.iterator();
		while(triples.hasNext()) {
			Triple t = triples.next();
			basicGraphPatterns.add(t);
		}
	}

	@Override
	public void visit(OpFilter filter) {
		System.out.println("In OpFilter");
		filter.getSubOp().visit(this);
	}

	@Override
	public void visit(OpProject project) {		
		for(Var v : project.getVars())
			queryVars.add(v.toString());
		project.getSubOp().visit(this);

	}	
}
*/