package dsparq.load;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Random;

import dsparq.misc.Constants;
import dsparq.util.Util;

/**
 * This class generates dummy triples in the form of numerical IDs.
 * @author Raghava
 *
 */
public class DummyTripleGenerator {
	
	public void generateDummyTriples(long numTriples) {
		long startTime = System.nanoTime();
		long numRandomTriples = (long) (numTriples * 0.8);
		long numStarTriples = numTriples - numRandomTriples;
		
		PrintWriter writer1 = null;
		PrintWriter writer2 = null;
		try {
			writer1 = new PrintWriter(new BufferedWriter(
				new FileWriter("random-triples")));
			generateRandomTriples(numRandomTriples, writer1);
			
			writer2 = new PrintWriter(new BufferedWriter(
					new FileWriter("star-triples")));
			generateStarSchemaTriples(numStarTriples, writer2);
		} catch (Exception e) {
			
		} finally {
			if (writer1 != null)
				writer1.close();
			if (writer2 != null)
				writer2.close();
		}		
		double secs = Util.getElapsedTime(startTime);
		System.out.println("Time taken (seconds): " + secs);
	}
	
	private void generateRandomTriples(long numRandomTriples, 
			PrintWriter writer) {
		Random random = new Random();
		for (int i = 1; i <= numRandomTriples; i++) {
			long subject = random.nextLong();
			long predicate = random.nextLong();
			long object = random.nextLong();
			StringBuilder tripleKV = new StringBuilder();
			tripleKV.append(subject).
				append(Constants.TRIPLE_TERM_DELIMITER).
				append(predicate).
				append(Constants.TRIPLE_TERM_DELIMITER).
				append(object);
			writer.println(tripleKV.toString());
		}
		System.out.println("Generated random triples and written to file");
	}
	
	private void generateStarSchemaTriples(long numStarTriples, 
			PrintWriter writer) {
		long numGroups = (long) (numStarTriples * 0.1);
		Random random = new Random();
		for (int i = 1; i <= numGroups; i++) {
			long subject = random.nextLong();
			for (int j = 1; j <= 10; j++) {
				long predicate = random.nextLong();
				long object = random.nextLong();
				StringBuilder tripleKV = new StringBuilder();
				tripleKV.append(subject).
					append(Constants.TRIPLE_TERM_DELIMITER).
					append(predicate).
					append(Constants.TRIPLE_TERM_DELIMITER).
					append(object);
				writer.println(tripleKV.toString());
			}
		}
		System.out.println("Generated star schema triples and written to file");
	}
	
	public static void main(String[] args) {
		if(args.length != 1) {
			System.out.println("Provide the number of triples to be generated");
			System.exit(-1);
		}
		new DummyTripleGenerator().generateDummyTriples(
				Long.parseLong(args[0]));
	}
}
