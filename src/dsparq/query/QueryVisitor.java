package dsparq.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.OpAsQuery;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;

public class QueryVisitor extends OpAsQuery.Converter {
	
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