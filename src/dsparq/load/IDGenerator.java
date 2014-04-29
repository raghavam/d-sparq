package dsparq.load;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import org.openjena.riot.RiotReader;
import org.openjena.riot.lang.LangNTriples;

import com.hp.hpl.jena.graph.Triple;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import dsparq.misc.Constants;
import dsparq.misc.HostInfo;
import dsparq.misc.PropertyFileHandler;
import dsparq.util.Util;

/**
 * Generates numeric ID and adds it to DB. Numerical IDs are
 * required for Metis (vertex IDs).
 * 
 * @author Raghava
 */
public class IDGenerator {

	public void generateIDs(String dirPath) throws Exception {
		
	}
	
	
	public static void main(String[] args) throws Exception {
		new IDGenerator().generateIDs(args[0]);
	}
}
