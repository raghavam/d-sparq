package dsparq.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jgrapht.alg.DijkstraShortestPath;
import org.jgrapht.graph.Multigraph;

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
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprList;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.ElementVisitorBase;

import dsparq.misc.Constants;

public class QuerySplitDecisionMaker {

	protected Multigraph<String, String> queryGraph;
	private String core = null;
	protected List<Triple> basicGraphPatterns;
	
	private boolean isPWOC(String query, int hopGuarantee) {
		
		basicGraphPatterns = parseQuery(query);
		queryGraph = constructQueryGraph(basicGraphPatterns);
		
		// for each vertex v, n = findDOFE(v);
		// list.add(n); core = smallest(list);
		
		List<VertexDistance> vdList = new ArrayList<VertexDistance>();
		Set<String> vertices = queryGraph.vertexSet();
		for(String vertex : vertices) {
			int d = findDOFE(vertex, vertices);
			VertexDistance vd = new VertexDistance(vertex, d);
			vdList.add(vd);
		}
		Collections.sort(vdList, new DistanceComparator());
		int coreDistance = vdList.get(0).distance;
		core = vdList.get(0).vertex;
		
		return (coreDistance <= hopGuarantee);
	}
	
	private Multigraph<String, String> constructQueryGraph(
			List<Triple> basicGraphPatterns) {
		Multigraph<String, String> graph = 
			new Multigraph<String, String>(String.class);
		for(Triple bgp : basicGraphPatterns) {
			String subject = bgp.getSubject().toString(); 
			String object = bgp.getObject().toString();
			graph.addVertex(subject);
			graph.addVertex(object);
			graph.addEdge(subject, object, bgp.getPredicate().toString(true));
		}	
		return graph;
	}
	
	/**
	 * Distance of the farthest edge (DoFE) of a vertex is the shortest
	 * path among the longest paths.
	 * 
	 * @param vertex
	 * @return
	 */
	protected int findDOFE(String vertex, Set<String> vertices) {
		List<Integer> shortestPaths = new ArrayList<Integer>();
		for(String adjVertex : vertices) {
			if(vertex.equals(adjVertex))
				continue;
			List<String> pathEdges = DijkstraShortestPath.findPathBetween(queryGraph, 
								vertex, adjVertex);
			if(pathEdges == null)
				System.out.println(vertex + " - " + adjVertex + " null path");
			else {
//				System.out.println(vertex + " - " + adjVertex + " path len: " + pathEdges.size());
				shortestPaths.add(pathEdges.size());
			}
		}
		Collections.sort(shortestPaths);
		int longestPathDistance = shortestPaths.get(shortestPaths.size()-1);
		return longestPathDistance;
	}
	
	public List<String> splitQuery(String query, int hopGuarantee) {
		List<String> splitQueries = new ArrayList<String>();
		MyOpVisitor opVisitor;
		
		boolean isPWOC = isPWOC(query, hopGuarantee);
		
		if(isPWOC) {
			// no need to split
			splitQueries.add(query);
		}
		else {
			// split the queries based on the core vertex
			Query queryObj = QueryFactory.create(query);
			opVisitor = new MyOpVisitor(queryObj, core);
			Op op = Algebra.compile(queryObj);
			op.visit(opVisitor);
			
			String opWithCore = opVisitor.getOpWithCore();
			// check if these 2 queries needs to be split further
			splitQueries.addAll(splitQuery(opWithCore, hopGuarantee));
			String opWithoutCore = opVisitor.getOpWithoutCore();
			splitQueries.addAll(splitQuery(opWithoutCore, hopGuarantee));
		}
		
		return splitQueries;
	}
	
	private List<Triple> parseQuery(String queryStr) {
		Query query = QueryFactory.create(queryStr);
		Element element = query.getQueryPattern();
		MyElementVisitor visitor = new MyElementVisitor();
		element.visit(visitor);
		return visitor.getBasicGraphPatterns();
	}
	
	public static void main(String[] args) {
		String testQuery1 = "select ?s ?o ?dob ?n where { "+ 
				   	   "?s <http://dbpedia.org/property/influenced> ?o ." +
				   	   "?o <http://dbpedia.org/property/dateOfBirth> ?dob ." +
				   	   "?o <http://dbpedia.org/property/name> ?n ." + 
				   	   "} ";
		
		String testQuery2 = "select ?player ?club ?region where { " +
							"?player <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
								"<http://dbpedia.org/resource/footballer> ." +
							"?player <http://dbpedia.org/property/playsFor> ?club ." +
							"?player <http://dbpedia.org/property/born> ?region ." +
							"?club <http://dbpedia.org/property/region> ?region ." +
							"?region <http://dbpedia.org/property/population> ?pop ." +
							"FILTER (?pop > 2000000) } ";
	
		List<String> queries = new QuerySplitDecisionMaker().splitQuery(testQuery2, 1);
		System.out.println("Split queries\n");
		for(String q : queries)
			System.out.println(q + "\n");
	}

}

class MyElementVisitor extends ElementVisitorBase {
	
	private List<Triple> basicGraphPatterns;
	
	MyElementVisitor() {
		basicGraphPatterns = new ArrayList<Triple>();
	}
	
	public List<Triple> getBasicGraphPatterns() {
		return basicGraphPatterns;
	}
	
	@Override
	public void visit(ElementPathBlock pathBlock) {	
		Iterator<TriplePath> paths = pathBlock.patternElts();
		while(paths.hasNext())
			basicGraphPatterns.add(paths.next().asTriple());	
	}

	@Override
	public void visit(ElementGroup elementGroup) {
		List<Element> elements = elementGroup.getElements();
		for(Element e : elements)
			e.visit(this);
	}	
}

class VertexDistance {
	String vertex;
	int distance;
	
	VertexDistance(String vertex, int distance) {
		this.vertex = vertex;
		this.distance = distance;
	}
}

class DistanceComparator implements Comparator<VertexDistance> {

	@Override
	public int compare(VertexDistance vd1, VertexDistance vd2) {
		return (vd1.distance - vd2.distance);
	}
	
}

class MyOpVisitor extends OpAsQuery.Converter {
	
	private String core;
	private Set<String> coreLabels;
	private Set<String> coreLessLabels;
	private Op opWithCore;
	private Op opWithoutCore;

	public String getOpWithCore() {
		Query query = OpAsQuery.asQuery(opWithCore);
		return query.serialize();
	}

	public String getOpWithoutCore() {
		Query query = OpAsQuery.asQuery(opWithoutCore);
		return query.serialize();
	}

	public MyOpVisitor(Query query, String core) {
		super(query);
		this.core = core;
		coreLabels = new HashSet<String>();
		coreLessLabels = new HashSet<String>();
	}

	@Override
	public void visit(OpBGP bgp) {

		BasicPattern bp = bgp.getPattern();
		BasicPattern coreBasicPattern = new BasicPattern();
		BasicPattern coreLessBasicPattern = new BasicPattern();
		Iterator<Triple> triples = bp.iterator();
		while(triples.hasNext()) {
			Triple t = triples.next();
			String subject = t.getSubject().toString();
			String predicate = t.getPredicate().toString();
			String object = t.getObject().toString(true);
			
			if(subject.equals(core) || object.equals(core)) {
				coreBasicPattern.add(t);
				coreLabels.add(subject);
				coreLabels.add(predicate);
				coreLabels.add(object);
			}
			else {
				coreLessBasicPattern.add(t);
				coreLessLabels.add(subject);
				coreLessLabels.add(predicate);
				coreLessLabels.add(object);
			}
		}
		if(!coreBasicPattern.isEmpty())
			opWithCore = new OpBGP(coreBasicPattern);
		if(!coreLessBasicPattern.isEmpty())
			opWithoutCore = new OpBGP(coreLessBasicPattern);
	}

	@Override
	public void visit(OpFilter filter) {
		
		filter.getSubOp().visit(this);
		
		ExprList exprs = filter.getExprs();
		Iterator<Expr> exprIt = exprs.iterator();
		ExprList coreExprs = new ExprList();
		ExprList coreLessExprs = new ExprList();
		
		while(exprIt.hasNext()) {
			Expr expr = exprIt.next();
			Set<Var> vars = expr.getVarsMentioned();
			for(Var v : vars) {
				if(coreLabels.contains(v.toString()))
					coreExprs.add(expr);
				if(coreLessLabels.contains(v.toString()))
					coreLessExprs.add(expr);
				else {
					try {
						throw new Exception("Unknown label: " + v.toString());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
		if(!coreExprs.isEmpty())
			opWithCore = OpFilter.filter(coreExprs, opWithCore);
		if(!coreLessExprs.isEmpty())
			opWithoutCore = OpFilter.filter(coreLessExprs, opWithoutCore);
	}

	@Override
	public void visit(OpProject project) {
		
		project.getSubOp().visit(this);
		
		List<Var> selectVars = project.getVars();
		List<Var> coreVars = new ArrayList<Var>();
		List<Var> coreLessVars = new ArrayList<Var>();

		for(Var v : selectVars) {
			if(coreLabels.contains(v.toString()))
				coreVars.add(v);
			if(coreLessLabels.contains(v.toString()))
				coreLessVars.add(v);
		}
		
		try {
			if(!coreVars.isEmpty())
				opWithCore = new OpProject(opWithCore, coreVars);
			else 
				throw new Exception("No projection vars in core query");
			if(!coreLessVars.isEmpty())
				opWithoutCore = new OpProject(opWithoutCore, coreLessVars);
			else
				throw new Exception("No projection vars in core less query");
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}	
}
