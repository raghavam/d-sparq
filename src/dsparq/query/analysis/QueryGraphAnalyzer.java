package dsparq.query.analysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.graph.ClassBasedEdgeFactory;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.util.NodeFactory;


/**
 * This class builds a graph out of the query patterns
 * and finds out the various patterns in them such as
 * star and pipeline.
 * 
 * @author Raghava
 *
 */
public class QueryGraphAnalyzer { 
	
	public List<QueryPattern> getAnalyzedPatterns(
			List<Triple> basicGraphPatterns) throws Exception {
		SimpleDirectedGraph<String, RelationshipEdge> queryGraph = null;
//				constructQueryGraph(basicGraphPatterns);
//				constructDummyGraph();
		
		List<QueryPattern> queryPatterns = new ArrayList<QueryPattern>();
		Map<String, QueryPattern> referenceMap = 
				new HashMap<String, QueryPattern>();
		//find out source vertices
		Set<String> vertices = queryGraph.vertexSet();
		List<String> sourceVertices = new ArrayList<String>();
		int inDegree;
		for(String v : vertices) {
			inDegree = queryGraph.inDegreeOf(v);
			if(inDegree == 0) 
				sourceVertices.add(v);
		}
		
		//do a breadth-first starting at sourceVertices
		BreadthFirstIterator<String, RelationshipEdge> bfIterator;
		for(String startVertex : sourceVertices) {
			System.out.println("source vertex: " + startVertex);
			QueryPattern queryPattern = null;
			bfIterator = new BreadthFirstIterator<String, RelationshipEdge>(
							queryGraph, startVertex);
			while(bfIterator.hasNext()) {
				String v = bfIterator.next();
				Set<RelationshipEdge> outGoingEdges = queryGraph.outgoingEdgesOf(v);
				QueryPattern patternAtVertex = referenceMap.get(v);
				if(outGoingEdges.size() == 1) {
					RelationshipEdge oedge = 
							outGoingEdges.iterator().next();
					String nv = oedge.getEdgeTarget().getLabel();
					NumericalTriplePattern triple = null;
//							new NumericalTriplePattern(v, 
//									oedge, nv);
					if(queryGraph.outDegreeOf(nv) >= 1) {
						PipelinePattern pipeline = new PipelinePattern();
						pipeline.setTriple(triple);
//						pipeline.setConnectingRelation(
//								ConnectingRelation.OBJ_SUB);
						referenceMap.put(nv, pipeline);
//						if(patternAtVertex != null) 
//							patternAtVertex.checkTypeAndAddPattern(pipeline);
//						else
//							queryPattern = pipeline;
					}
					else {
//						if(patternAtVertex != null) 							
//							patternAtVertex.checkTypeAndAddPattern(triple);
//						else 
//							queryPattern = triple;
					}
				}
				else if(outGoingEdges.size() > 1) {
					StarPattern starPattern = new StarPattern();
					for(RelationshipEdge oedge : outGoingEdges) {
						String oedgeTarget = oedge.getEdgeTarget().toString();
						NumericalTriplePattern triple = null;
//								new NumericalTriplePattern(v, 
//										oedge, oedgeTarget);
						if(queryGraph.outDegreeOf(oedgeTarget) == 0)
							starPattern.addQueryPattern(triple);
						else {
							PipelinePattern pipeline = new PipelinePattern();
							pipeline.setTriple(triple);
//							pipeline.setConnectingRelation(
//									ConnectingRelation.OBJ_SUB);
							referenceMap.put(oedgeTarget, pipeline);
							starPattern.addQueryPattern(pipeline);
						}
					}
//					if(patternAtVertex != null)
//						patternAtVertex.checkTypeAndAddPattern(starPattern);
//					else
//						queryPattern = starPattern;
				}
				referenceMap.remove(v);
			}
			queryPatterns.add(queryPattern);
			System.out.println("Query Pattern: " + queryPattern);
			System.out.println();
		}		
		
		return queryPatterns;
	}
	
	private SimpleDirectedGraph<Long, Long> constructQueryGraph(
			List<Triple> basicGraphPatterns) {
		SimpleDirectedGraph<Long, Long> queryGraph = 
				new SimpleDirectedGraph<Long, Long>(Long.class);
		for(Triple triple : basicGraphPatterns) {
			Long sourceVertex = Long.parseLong(
					triple.getSubject().toString());
			Long targetVertex = Long.parseLong(
					triple.getObject().toString());
			Long edgeLabel = Long.parseLong(
					triple.getPredicate().toString());
			queryGraph.addVertex(sourceVertex);
			queryGraph.addVertex(targetVertex);
			queryGraph.addEdge(sourceVertex, targetVertex, edgeLabel);
		}		
		return queryGraph;
	}
	
	private SimpleDirectedGraph<RDFVertex, RelationshipEdge> constructDummyGraph() {
		SimpleDirectedGraph<RDFVertex, RelationshipEdge> queryGraph = 
				new SimpleDirectedGraph<RDFVertex, RelationshipEdge>(
						new ClassBasedEdgeFactory<RDFVertex, 
								RelationshipEdge>(RelationshipEdge.class));
		String s1 = "s1";
		RDFVertex vs1 = new RDFVertex();
		vs1.setLabel(s1);
		String s2 = "s2";
		RDFVertex vs2 = new RDFVertex();
		vs2.setLabel(s2);
		queryGraph.addVertex(vs1);
		queryGraph.addVertex(vs2);
		String s3 = "s3";
		RDFVertex vs3 = new RDFVertex();
		vs3.setLabel(s3);
		String o1 = "o1";
		RDFVertex vo1 = new RDFVertex();
		vo1.setLabel(o1);
		String o2 = "o2";
		RDFVertex vo2 = new RDFVertex();
		vo2.setLabel(o2);
		String o3 = "o3";
		RDFVertex vo3 = new RDFVertex();
		vo3.setLabel(o3);
		queryGraph.addVertex(vs3);
		queryGraph.addVertex(vo1);
		queryGraph.addVertex(vo2);
		queryGraph.addVertex(vo3);
		
		queryGraph.addEdge(vs2, vs3, new RelationshipEdge(vs2, vs3, "p2"));
		queryGraph.addEdge(vs3, vo3, new RelationshipEdge(vs3, vo3, "p1"));
		queryGraph.addEdge(vs2, vo2, new RelationshipEdge(vs2, vo2, "p4"));
		queryGraph.addEdge(vs2, vo1, new RelationshipEdge(vs2, vo1, "p3"));
		queryGraph.addEdge(vo1, vs1, new RelationshipEdge(vo1, vs1, "p3"));
/*		
		String s11 = "s11";
		String s21 = "s21";
		queryGraph.addVertex(s11);
		queryGraph.addVertex(s21);
		String s31 = "s31";
		String o11 = "o11";
		String o21 = "o21";
		String o31 = "o31";
		queryGraph.addVertex(s31);
		queryGraph.addVertex(o11);
		queryGraph.addVertex(o21);
		queryGraph.addVertex(o31);
*/		
//		queryGraph.addEdge(s21, s31, new RelationshipEdge(s21, s31, "p1"));
//		queryGraph.addEdge(s31, o31, new RelationshipEdge(s31, o31, "p1"));
//		queryGraph.addEdge(s21, o21, new RelationshipEdge(s21, o21, "p4"));
//		queryGraph.addEdge(s21, o11, new RelationshipEdge(s21, o11, "p3"));
//		queryGraph.addEdge(o11, s11, new RelationshipEdge(o11, s11, "p1"));
		
		return queryGraph;
	}
	
	public static void main(String[] args) throws Exception {		
//		List<QueryPattern> queryPatterns = 
//				new QueryGraphAnalyzer().getAnalyzedPatterns(null);
//		System.out.println(queryPatterns.toString());
		SimpleDirectedGraph<RDFVertex, RelationshipEdge> queryGraph = 
				new QueryGraphAnalyzer().constructDummyGraph();
		System.out.println(queryGraph.toString());
		Set<RDFVertex> vertices = queryGraph.vertexSet();
		RDFVertex v = vertices.iterator().next();
		System.out.println("Vertex: " + v);
		Set<RelationshipEdge> edges = queryGraph.edgesOf(v);
		for(RelationshipEdge e : edges)
			System.out.println("Edge: " + e);
	}
}
