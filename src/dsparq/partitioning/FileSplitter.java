package dsparq.partitioning;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Scanner;

import dsparq.misc.PropertyFileHandler;

/**
 * This class is used to split a file into sub-files
 * based on the number of partitions available.
 * 
 * @author Raghava
 *
 */
public class FileSplitter {
	
	public void splitFile(String path, long totalLines) throws Exception {
		
		BufferedInputStream inputStream = new BufferedInputStream(
							new FileInputStream(new File(path)));
		Scanner scanner = new Scanner(inputStream, "UTF-8");
		PropertyFileHandler propertyFileHandler = 
								PropertyFileHandler.getInstance();
		// also includes the master node
		int partitions = propertyFileHandler.getShardCount() + 1;
		long linesInSplit = totalLines/partitions;
		long lineCount = 1;
		
		for(int i=1; i <= partitions-1; i++) {
			PrintWriter writer
			   = new PrintWriter(new BufferedWriter(new FileWriter(
					   "file-split-" + i)));
			System.out.println("Writing into file-" + i);
			while(lineCount <= linesInSplit) {
				writer.println(scanner.nextLine());
				lineCount++;
			}
			writer.close();
			lineCount = 1;
		}
		// put the remaining lines in the last split
		PrintWriter writer
		   				= new PrintWriter(new BufferedWriter(new FileWriter(
		   								"file-split-" + partitions)));
		while(scanner.hasNext())
			writer.println(scanner.nextLine());
		System.out.println("Done");
		writer.close();
		inputStream.close();
		scanner.close();
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Use cat <file> | wc -l to count the number of lines");
		if(args.length != 2) {
			String msg = "Provide path to the file to be split and total " +
					"number of lines in that file";
			throw new Exception(msg);
		}
		new FileSplitter().splitFile(args[0], Long.parseLong(args[1]));
	}
}
