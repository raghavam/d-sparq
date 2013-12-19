package dsparq.partitioning.format;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;


/**
 * This class combines two files. File containing
 * only vertex IDs and output of METIS which contains
 * only partition IDs.
 * 
 * Hadoop job requires combination of the two files.
 * 
 * @author Raghava
 *
 */
public class VertexPartitionFormatter {

	public void generateVertexPartitionIDs(String vpath, String ppath) 
		throws Exception {
		
		File inputData1, inputData2;
		FileReader fileReader1, fileReader2;
		BufferedReader vertexIDReader = null, partitionIDReader = null;
		PrintWriter writer = null;
		try {			
			inputData1 = new File(vpath);
			fileReader1 = new FileReader(inputData1);
			vertexIDReader = new BufferedReader(fileReader1);	
			
			inputData2 = new File(ppath);
			fileReader2 = new FileReader(inputData2);
			partitionIDReader = new BufferedReader(fileReader2);	
			
			writer = new PrintWriter(new BufferedWriter(
					new FileWriter("vertex-partitions")));
			
			String vertexID;
			String partitionID;
			long count = 0;
			while((vertexID = vertexIDReader.readLine()) != null) {
				partitionID = partitionIDReader.readLine().trim();
				vertexID = vertexID.trim();
				writer.println(vertexID + " " + partitionID);
				count++;
				if(count%10000 == 0)
					System.out.println("Reached " + count);
			}
			if(partitionIDReader.readLine() != null)
				throw new Exception("partitionID file length should " +
						"be same as vertexID file length");
			System.out.println("output written to vertex-partitions file");
		}
		finally {
			if(writer != null)
				writer.close();
			if(vertexIDReader != null)
				vertexIDReader.close();
			if(partitionIDReader != null)
				partitionIDReader.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			throw new Exception("provide vertexID file and " +
					"partitionID file in that order");
		}
		new VertexPartitionFormatter().generateVertexPartitionIDs(args[0], args[1]);
	}

}
