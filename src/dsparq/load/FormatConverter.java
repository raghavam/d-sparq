package dsparq.load;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.openjena.riot.RiotReader;
import org.openjena.riot.lang.LangNTriples;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import dsparq.misc.Constants;

/**
 * This class is used for the conversion of RDF triples
 * from RDF/XML format to N-Triple format.
 * @author Raghava
 *
 */
public class FormatConverter {

	public void convertToNTriples(String dirPath, String outputDir) 
		throws Exception {
		// retrieve all *.owl files
		File owlFolder = new File(dirPath);
		File[] allFiles = owlFolder.listFiles();
		List<File> owlFiles = new ArrayList<File>();
		for(File file : allFiles) {
			if(file.getName().endsWith(".owl"))
				owlFiles.add(file);
		}
		System.out.println("There are " + owlFiles.size() + " owl files");
		Model model = ModelFactory.createDefaultModel();
		BufferedReader reader = null;
		PrintWriter writer = null;
		long totalTriples = 0;
		for(File ofile : owlFiles) {
			String[] fnameExt = ofile.getName().split("\\.");
			File ntfile = new File(outputDir + "/" + fnameExt[0] + ".nt");
//			System.out.println(ntfile.getCanonicalPath());
			reader = new BufferedReader(new FileReader(ofile));
			writer = new PrintWriter(new BufferedWriter(new FileWriter(ntfile)));
			model.read(reader, Constants.BASE_URI);
			model.write(writer, "N-TRIPLE");
			totalTriples += model.size();
			model.removeAll();
		}
		System.out.println("Total no of triples: " + totalTriples);
		model.close();
		if(reader != null)
			reader.close();
		if(writer != null)
			writer.close();
		
		// with univ 1, there are 102737 triples
	}
	
	public void listTriples(String path) throws Exception {
		FileInputStream in = new FileInputStream(path);
		LangNTriples ntriples = RiotReader.createParserNTriples(in, null);
		while(ntriples.hasNext()) {
			Triple t = ntriples.next();
			System.out.println(t.getSubject().toString());
			System.out.println(t.getPredicate().toString());
			System.out.println(t.getObject().toString());
			System.out.println();
		}
		in.close();
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			throw new Exception("Provide directory containing owl files " +
					"and output directory");
		}
		new FormatConverter().convertToNTriples(args[0], args[1]);
//		new FormatConverter().listTriples(args[0]);
	}

}
