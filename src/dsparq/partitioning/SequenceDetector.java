package dsparq.partitioning;

import java.io.BufferedReader;
import java.io.FileReader;

public class SequenceDetector {

	public void detectSequence(String path) throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader(path));
		String line;
		long currVal = 0;
		long nextVal;
		while((line = reader.readLine()) != null) {
			nextVal = Long.parseLong(line.trim());
			if(nextVal != (currVal + 1)) {
				System.out.println("Sequence broken after " + currVal);
				currVal = nextVal-1;
			}
			currVal++;
		}
		reader.close();
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			throw new Exception("provide vertex file for sequence checking");
		}
		new SequenceDetector().detectSequence(args[0]);
	}

}
