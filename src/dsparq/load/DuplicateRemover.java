package dsparq.load;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import dsparq.misc.Constants;

public class DuplicateRemover {
	
	//TODO: replace this by a disk-backed set
	private Set<String> tripleComponents = new HashSet<String>();

	public void removeDuplicates(String dirPath, String outputDir) 
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
		long fileCount = 0;
		double multiplier = 1;
		for(File ofile : owlFiles) {
			String[] fnameExt = ofile.getName().split("\\.");
			File ntfile = new File(outputDir + "/" + fnameExt[0] + ".nt");
//			System.out.println(ntfile.getCanonicalPath());
			reader = new BufferedReader(new FileReader(ofile));
			writer = new PrintWriter(new BufferedWriter(new FileWriter(ntfile)));
			model.read(reader, Constants.BASE_URI);
			insertStmtsIntoSet(model.listStatements());
			model.write(writer, "N-TRIPLE");
			totalTriples += model.size();
			model.removeAll();
			fileCount++;
			if(fileCount >= (owlFiles.size()*0.1*multiplier)) {
				System.out.println("Reached " + fileCount);
				multiplier++;
			}
		}
		System.out.println("Total no of triples: " + totalTriples);
		System.out.println("Size of set: " + tripleComponents.size());
		model.close();
		if(reader != null)
			reader.close();
		if(writer != null)
			writer.close();
		
		// write contents of the set to a file
		PrintWriter tripleComponentWriter = new PrintWriter(
					new BufferedWriter(new FileWriter("unique-triple-comps")));
		for(String component : tripleComponents)
			tripleComponentWriter.println(component);
		tripleComponentWriter.close();
	}
	
	public void removeDuplicates2(String dirPath, String outputDir) 
			throws Exception {
		// retrieve all *.owl files
		File owlFolder = new File(dirPath);
		File[] allFiles = owlFolder.listFiles();
		List<File> owlFiles = new ArrayList<File>();
		for(File file : allFiles) {
			if(file.getName().endsWith(".owl"))
				owlFiles.add(file);
		}
		int totalFiles = owlFiles.size();
		System.out.println("There are " + totalFiles + " owl files");
		Model model = ModelFactory.createDefaultModel();
		BufferedReader reader = null;
		PrintWriter writer = null;
		long totalTriples = 0;
		long fileCount = 0;
		double multiplier = 1;
		for(int i=0; i<totalFiles/2; i++) {
			File ofile = owlFiles.get(i);
			String[] fnameExt = ofile.getName().split("\\.");
			File ntfile = new File(outputDir + "/" + fnameExt[0] + ".nt");
//			System.out.println(ntfile.getCanonicalPath());
			reader = new BufferedReader(new FileReader(ofile));
			writer = new PrintWriter(new BufferedWriter(new FileWriter(ntfile)));
			model.read(reader, Constants.BASE_URI);
			insertStmtsIntoDB(model.listStatements());
			model.write(writer, "N-TRIPLE");
			totalTriples += model.size();
			model.removeAll();
			fileCount++;
			if(fileCount >= (owlFiles.size()*0.1*multiplier)) {
				System.out.println("Reached " + fileCount);
				multiplier++;
			}
		}
		System.out.println("Total no of triples: " + totalTriples);
		System.out.println("Size of set: " + tripleComponents.size());
		model.close();
		if(reader != null)
			reader.close();
		if(writer != null)
			writer.close();
	}
	
	private void insertStmtsIntoSet(StmtIterator stmtIterator) {
		while(stmtIterator.hasNext()) {
			Triple triple = stmtIterator.next().asTriple();
			tripleComponents.add(triple.getSubject().toString());
			tripleComponents.add(triple.getPredicate().toString());
			tripleComponents.add(triple.getObject().toString());
		}
	}
	
	private void insertStmtsIntoDB(StmtIterator stmtIterator) {
		while(stmtIterator.hasNext()) {
			Triple triple = stmtIterator.next().asTriple();
			tripleComponents.add(triple.getSubject().toString());
			tripleComponents.add(triple.getPredicate().toString());
			tripleComponents.add(triple.getObject().toString());
		}
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			throw new Exception("Provide directory containing owl files " +
					"and output directory");
		}
		new DuplicateRemover().removeDuplicates(args[0], args[1]);
	}

}
