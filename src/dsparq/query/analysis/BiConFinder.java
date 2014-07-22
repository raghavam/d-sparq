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
//	private Map<String, Integer> num;
	private Map<String, Integer> low;
	private SimpleGraph<RDFVertex, DefaultEdge> undirectedQueryGraph;
	private SimpleDirectedGraph<RDFVertex, RelationshipEdge> directedQueryGraph;
	private Set<String> articulationPoints;
	private Set<String> notArticulationPoints;
	private int i = 0;
	private Map<String, QueryPattern> vertexPatternMap;
	
	private Map<String, Color> vertexColorMap;
	private Map<String, String> vertexPredMap;
	private Map<String, Integer> d;
	
	public BiConFinder() {
		articulationPoints = new HashSet<String>();
		notArticulationPoints = new HashSet<String>();
//		num = new HashMap<String, Integer>();
		low = new HashMap<String, Integer>();
		vertexPatternMap = new HashMap<String, QueryPattern>();
		
		vertexColorMap = new HashMap<String, Color>();
		vertexPredMap = new HashMap<String, String>();
		d = new HashMap<String, Integer>();
	}
	
	public QueryPattern getQueryPatterns(
			SimpleGraph<RDFVertex, DefaultEdge> undirectedGraph,
			SimpleDirectedGraph<RDFVertex, RelationshipEdge> directedGraph) 
			throws Exception {
		undirectedQueryGraph = undirectedGraph;
		directedQueryGraph = directedGraph;
		Set<RDFVertex> vertices = undirectedQueryGraph.vertexSet();
		QueryPattern queryPattern = null;
		clearVars();
		
		RDFVertex root = null;
		for(RDFVertex v : vertices) {
//			num.put(v.getLabel(), new Integer(0));
			vertexColorMap.put(v.getLabel(), Color.WHITE);
			if(directedGraph.inDegreeOf(v) == 0)
				root = v;
		}
//		for(RDFVertex v : vertices)
//			if(num.get(v.getLabel()) == 0) 
//				queryPattern = bicon(v, null);
		queryPattern = findArticulationPoints(root);		
		return queryPattern;
	}
	
	private void clearVars() {
		low.clear();
		d.clear();
		vertexColorMap.clear();
		vertexPredMap.clear();
		vertexPatternMap.clear();
		articulationPoints.clear();
		notArticulationPoints.clear();
		i = 0;
	}
	
	private QueryPattern findArticulationPoints(RDFVertex u) throws Exception {
		vertexColorMap.put(u.getLabel(), Color.GRAY);
		i = i + 1;
		low.put(u.getLabel(), i);
		d.put(u.getLabel(), i);
		boolean foundArticulationPoint = false;
		QueryPattern pattern = null;
		Set<DefaultEdge> adjacentEdges = undirectedQueryGraph.edgesOf(u);
		for(DefaultEdge adjEdge : adjacentEdges) {
			RDFVertex t = undirectedQueryGraph.getEdgeTarget(adjEdge);
			RDFVertex s = undirectedQueryGraph.getEdgeSource(adjEdge);
			RDFVertex v = t.equals(u) ? s : t;
			if(vertexColorMap.get(v.getLabel()) == Color.WHITE) {
				vertexPredMap.put(v.getLabel(), u.getLabel());
				findArticulationPoints(v);
				int min = Math.min(low.get(u.getLabel()), 
						low.get(v.getLabel()));
				low.put(u.getLabel(), min);
				if(low.get(v.getLabel()) >= d.get(u.getLabel())) {
					//u is the articulation point
					if(directedQueryGraph.outDegreeOf(u) >= 2 && 
							directedQueryGraph.inDegreeOf(u) == 0) {
						if(!notArticulationPoints.contains(u.getLabel())) {
							notArticulationPoints.add(u.getLabel());
							foundArticulationPoint = true;
//							System.out.println("Skipping Articultion point: " + u);
						}
					}
					else {
						if(!articulationPoints.contains(u.getLabel())) {
							articulationPoints.add(u.getLabel());
							foundArticulationPoint = true;
//							System.out.println("Articultion point: " + 
//									u.getLabel());
						}
					}			
					if(foundArticulationPoint) {
						Set<RelationshipEdge> outGoingEdges = 
							directedQueryGraph.outgoingEdgesOf(u);
						if(outGoingEdges.size() == 1) {
							RelationshipEdge e = outGoingEdges.iterator().next();
							NumericalTriplePattern triple = 
								new NumericalTriplePattern(
										e.getEdgeSource(), e, 
										e.getEdgeTarget());	
							pattern = new PipelinePattern();
							((PipelinePattern)pattern).setTriple(triple);
							if(directedQueryGraph.outDegreeOf(
									e.getEdgeTarget()) > 0) {
								QueryPattern connectingPattern = 
									vertexPatternMap.remove(
											e.getEdgeTarget().getLabel());
								if(connectingPattern != null) {
									((PipelinePattern)pattern).
										addRelationTriple(
												ConnectingRelation.OBJ_SUB, 
												connectingPattern, 
												e.getEdgeTarget().getLabel());
								}
							}
//							else
//								pattern = triple;						
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
												connectingPattern, 
												e.getEdgeTarget().getLabel());
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
						checkIncomingEdges(u, pattern);						
						vertexPatternMap.put(u.getLabel(), pattern);
					}
					foundArticulationPoint = false;
				}
			}
			else if(!v.getLabel().equals(vertexPredMap.get(u.getLabel()))) {
				int min = Math.min(low.get(u.getLabel()), d.get(v.getLabel()));
				low.put(u.getLabel(), min);
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
							ConnectingRelation.OBJ_SUB, vertexQueryPattern, 
							e.getEdgeSource().getLabel());
					for(RelationshipEdge edge : incomingEdges) {
						if(edge.getEdgeSource().equals(e.getEdgeSource()))
							continue;
						QueryPattern pattern2 = vertexPatternMap.get(
											edge.getEdgeSource().getLabel());
						if(pattern2 != null)
							((PipelinePattern)pattern).addRelationTriple(
									ConnectingRelation.OBJ_OBJ, pattern2, 
									edge.getEdgeSource().getLabel());
					}
				}
				else if(pattern instanceof StarPattern) {
					if(vertexQueryPattern != null) {
						((StarPattern)pattern).searchAndAdd(vertex.getLabel(), 
							ConnectingRelation.OBJ_SUB, vertexQueryPattern, 
							vertex.getLabel());
					}
					for(RelationshipEdge edge : incomingEdges) {
						if(edge.getEdgeSource().equals(e.getEdgeSource()))
							continue;
						QueryPattern pattern2 = vertexPatternMap.get(
											edge.getEdgeSource().getLabel());
						if(pattern2 != null) {
							((StarPattern)pattern).searchAndAdd(vertex.getLabel(),
									ConnectingRelation.OBJ_OBJ, pattern2, 
									vertex.getLabel());
						}
					}
				}
				else
					throw new Exception("Unexpected pattern type");
			}
	}
	
	
	public static void main(String[] args) throws Exception {
		new BiConFinder().getQueryPatterns(null, null);
	}
	
	enum Color { WHITE, GRAY }
}


