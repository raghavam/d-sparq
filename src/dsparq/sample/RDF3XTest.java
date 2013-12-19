package dsparq.sample;

import java.io.File;
import java.sql.DriverManager;
import java.util.GregorianCalendar;
import java.util.Scanner;


import de.mpii.rdf3x.Connection;
import de.mpii.rdf3x.Statement;
import dsparq.util.Util;

public class RDF3XTest {

	public void runQuery(String db, String query) throws Exception {
		java.sql.Connection c = DriverManager.getConnection("rdf3x://"+db);
		java.sql.Statement s = c.createStatement();
	    java.sql.ResultSet r = s.executeQuery(query);

	    java.sql.ResultSetMetaData rm=r.getMetaData();
/*	      
	      for (int index=0;index<rm.getColumnCount();index++) {
	         if (index>0) System.out.print(' ');
	         System.out.print(rm.getColumnLabel(index+1));
	      }
	      System.out.println();
*/
	    long resultCount = 0;
	    if (r.first()) 
	    	do {
//	    		for (int index=0;index<rm.getColumnCount();index++) {
//	    			  if (index>0) 
//	    				  System.out.print(' ');
//	    			  System.out.print(r.getString(index+1));
//	    		}
	    		resultCount++;
//	    		System.out.println();
	    	} while (r.next());
	    else
	    	System.out.println("This is not first...");
	    System.out.println("Results: " + resultCount);
	    c.close();
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length!=2) {
	         System.out.println("provide db and query");
	         return;
	    }
		GregorianCalendar start = new GregorianCalendar();
		File queryFile = new File(args[1]);
		Scanner scanner = new Scanner(queryFile);
		StringBuilder query = new StringBuilder();
		while(scanner.hasNext()) 
			query.append(scanner.nextLine());
		scanner.close();
		new RDF3XTest().runQuery(args[0], query.toString());
		double secs = Util.getElapsedTime(start);
		System.out.println("Total Secs: " + secs);
	}

}
