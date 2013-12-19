package dsparq.query.analysis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.graph.SimpleGraph;

/**
 * Finds biconnected components and articulation points in
 * a graph. But the biconnected components are star-patterns. 
 * 
 * @author Raghava
 *
 */
public class BiConFinder {

//	private Stack<Pair> stack;
	private Map<String, Integer> num;
	private Map<String, Integer> low;
	private SimpleGraph<RDFVertex, DefaultEdge> undirectedQueryGraph;
	private SimpleDirectedGraph<RDFVertex, RelationshipEdge> directedQueryGraph;
	private Set<String> articulationPoints;
	private Set<String> notArticulationPoints;
	private int i = 0;
	private Map<String, QueryPattern> vertexPatternMap;
	
	public BiConFinder() {
		articulationPoints = new HashSet<String>();
		notArticulationPoints = new HashSet<String>();
		num = new HashMap<String, Integer>();
		low = new HashMap<String, Integer>();
		vertexPatternMap = new HashMap<String, QueryPattern>();
	}
	
	public QueryPattern getQueryPatterns(
			SimpleGraph<RDFVertex, DefaultEdge> undirectedGraph,
			SimpleDirectedGraph<RDFVertex, RelationshipEdge> directedGraph) 
			throws Exception {
		undirectedQueryGraph = undirectedGraph;
		directedQueryGraph = directedGraph;
		Set<RDFVertex> vertices = undirectedQueryGraph.vertexSet();
		QueryPattern queryPattern = null;
			
		for(RDFVertex v : vertices)
			num.put(v.getLabel(), new Integer(0));
		for(RDFVertex v : vertices)
			if(num.get(v.getLabel()) == 0) 
				queryPattern = bicon(v, null);
		return queryPattern;
	}
	
	private QueryPattern bicon(RDFVertex v, RDFVertex u) throws Exception {
		i = i + 1;
		num.put(v.getLabel(), i);
		low.put(v.getLabel(), i);
		boolean foundArticulationPoint = false;
		QueryPattern pattern = null; 
		Set<DefaultEdge> adjacentEdges = undirectedQueryGraph.edgesOf(v);
		for(DefaultEdge adjEdge : adjacentEdges) {
			RDFVertex t = undirectedQueryGraph.getEdgeTarget(adjEdge);
			RDFVertex s = undirectedQueryGraph.getEdgeSource(adjEdge);
			RDFVertex w = t.equals(v) ? s : t;
			if(num.get(w.getLabel()) == 0) {
				bicon(w, v);
				int min = Math.min(low.get(v.getLabel()), 
						low.get(w.getLabel()));
				low.put(v.getLabel(), min);
				if(low.get(w.getLabel()) >= num.get(v.getLabel())) {
					if(directedQueryGraph.outDegreeOf(v) >= 2 && 
							directedQueryGraph.inDegreeOf(v) == 0) {
						if(!notArticulationPoints.contains(v.getLabel())) {
							notArticulationPoints.add(v.getLabel());
							foundArticulationPoint = true;
//							System.out.println("Skipping Articultion point: " + v);
						}
					}
					else {
						if(!articulationPoints.contains(v.getLabel())) {
							articulationPoints.add(v.getLabel());
							foundArticulationPoint = true;
//							System.out.println("Articultion point: " + 
//									v.getLabel());
						}
					}			
					if(foundArticulationPoint) {
						Set<RelationshipEdge> outGoingEdges = 
							directedQueryGraph.outgoingEdgesOf(v);
						if(outGoingEdges.size() == 1) {
							RelationshipEdge e = outGoingEdges.iterator().next();
							NumericalTriplePattern triple = 
								new NumericalTriplePattern(
										e.getEdgeSource(), e, 
										e.getEdgeTarget());							
							if(directedQueryGraph.outDegreeOf(
									e.getEdgeTarget()) > 0) {
								pattern = new PipelinePattern();
								((PipelinePattern)pattern).setTriple(triple);
								QueryPattern connectingPattern = 
									vertexPatternMap.remove(
											e.getEdgeTarget().getLabel());
								if(connectingPattern != null) {
									((PipelinePattern)pattern).
										addRelationTriple(
												ConnectingRelation.OBJ_SUB, 
												connectingPattern);
								}
							}
							else
								pattern = triple;						
						}
						else if(outGoingEdges.size() >= 2) {
							pattern = new StarPattern();
							for(RelationshipEdge e : outGoingEdges) {
								//check if this is a NumericalTriplePattern or 
								//a PipelinePattern
								if(directedQueryGraph.outDegreeOf(
										e.getEdgeTarget()) > 0) {
									//Pipeline pattern
									PipelinePattern ppattern = new PipelinePattern();
									NumericalTriplePattern triplePattern = 
											new NumericalTriplePattern(
													e.getEdgeSource(), e, 
													e.getEdgeTarget());
									ppattern.setTriple(triplePattern);
									QueryPattern connectingPattern = 
										vertexPatternMap.remove(
												e.getEdgeTarget().getLabel());
									if(connectingPattern != null)
										ppattern.addRelationTriple(
												ConnectingRelation.OBJ_SUB, 
												connectingPattern);
									((StarPattern)pattern).addQueryPattern(
											ppattern);
								}
								else {
									//NumericalTriple pattern
									((StarPattern)pattern).addQueryPattern(
											new NumericalTriplePattern(
													e.getEdgeSource(), e, 
													e.getEdgeTarget()));
								}
							}
						}
//						else
//							throw new Exception("OutDegree of " + v + " is " + 
//									outGoingEdges.size());
						checkIncomingEdges(v, pattern);						
						vertexPatternMap.put(v.getLabel(), pattern);
					}
					foundArticulationPoint = false;
				}
			}
			else if((num.get(w.getLabel()) < num.get(v.getLabel())) 
					&& (!w.equals(u))) {
				int min = Math.min(low.get(v.getLabel()), 
						num.get(w.getLabel()));
				low.put(v.getLabel(), min);
			}
		}
		return pattern;
	}
	
	private void checkIncomingEdges(RDFVertex vertex, 
			QueryPattern vertexQueryPattern) throws Exception {
		Set<RelationshipEdge> incomingEdges = 
				directedQueryGraph.incomingEdgesOf(vertex);
		if(incomingEdges.isEmpty())
			return;
		RelationshipEdge e = incomingEdges.iterator().next();
			QueryPattern pattern = vertexPatternMap.get(
					e.getEdgeSource().getLabel());
			if(pattern != null) {
				//found pattern, move rest of the patterns from other incoming 
				// edges into this one.
				if(pattern instanceof PipelinePattern) {
					if(vertexQueryPattern != null)
						((PipelinePattern)pattern).addRelationTriple(
							ConnectingRelation.OBJ_SUB, vertexQueryPattern);
					for(RelationshipEdge edge : incomingEdges) {
						if(edge.getEdgeSource().equals(e.getEdgeSource()))
							continue;
						QueryPattern pattern2 = vertexPatternMap.get(
											edge.getEdgeSource().getLabel());
						if(pattern2 != null)
							((PipelinePattern)pattern).addRelationTriple(
									ConnectingRelation.OBJ_OBJ, pattern2);
					}
				}
				else if(pattern instanceof StarPattern) {
					if(vertexQueryPattern != null)
						((StarPattern)pattern).searchAndAdd(vertex.getLabel(), 
							ConnectingRelation.OBJ_SUB, vertexQueryPattern);
					for(RelationshipEdge edge : incomingEdges) {
						if(edge.getEdgeSource().equals(e.getEdgeSource()))
							continue;
						QueryPattern pattern2 = vertexPatternMap.get(
											edge.getEdgeSource().getLabel());
						if(pattern2 != null)
							((StarPattern)pattern).searchAndAdd(vertex.getLabel(),
									ConnectingRelation.OBJ_OBJ, pattern2);
					}
				}
				else
					throw new Exception("Unexpected pattern type");
			}
	}
	
	
	public static void main(String[] args) throws Exception {
		new BiConFinder().getQueryPatterns(null, null);
	}
}