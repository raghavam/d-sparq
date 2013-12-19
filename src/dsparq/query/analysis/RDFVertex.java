package dsparq.query.analysis;

/**
 * Represents a vertex in the query graph. Holds either a subject
 * or an object. If 'isRealLabel' is true then the label is not renamed
 * and 'label' contains the actual label. If it is false the label has
 * been renamed and the actual label is in 'originalLabel'.
 * 
 * @author Raghava
 *
 */
public class RDFVertex {

	private String label;
	private boolean isRealLabel;
	private String originalLabel;
	
	public RDFVertex() {
		isRealLabel = true;
		originalLabel = null;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public boolean isRealLabel() {
		return isRealLabel;
	}

	public void setRealLabel(boolean isRealLabel) {
		this.isRealLabel = isRealLabel;
	}

	public String getOriginalLabel() {
		return originalLabel;
	}

	public void setOriginalLabel(String originalLabel) {
		this.originalLabel = originalLabel;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof RDFVertex))
			return false;
		else
			return label.equals(((RDFVertex)obj).getLabel());
	}
	
	@Override
	public int hashCode() {
		return label.hashCode();
	}
	
	@Override
	public String toString() {
		return (isRealLabel ? label : originalLabel);
	}
}
