package dsparq.partitioning;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;


/**
 * The purpose of this class is to format triples 
 * in NT format to a format that is suitable for 
 * Jiewen's code which is subject|predicate|object|bool
 * 
 * bool - indicates whether object is literal(true) or not(false)
 * 
 * @author Raghava Mutharaju
 *
 */
public class TripleFormatter extends Configured implements Tool {

	// rdf:type = <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
	
	@Override
	public int run(String[] args) throws Exception {
		return 0;
	}
	
	public static void main(String[] args) {

	}

}
