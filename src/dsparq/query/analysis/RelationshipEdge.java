package dsparq.query.analysis;

import org.jgrapht.graph.DefaultEdge;


public class RelationshipEdge extends DefaultEdge {
	private static final long serialVersionUID = 1L;
	private RDFVertex v1;
    private RDFVertex v2;
    private String label;

    public RelationshipEdge(RDFVertex v1, RDFVertex v2, String label) {
        this.v1 = v1;
        this.v2 = v2;
        this.label = label;
    }

    public RDFVertex getEdgeSource() {
        return v1;
    }

    public RDFVertex getEdgeTarget() {
        return v2;
    }
    
    public String getEdgeLabel() {
    	return label;
    }

    @Override
    public String toString() {
        return label;
    }
}
