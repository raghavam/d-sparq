package dsparq.util;

import java.security.MessageDigest;
import java.util.GregorianCalendar;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import dsparq.misc.Constants;

public class Util {

	/**
	 * Takes a message and uses the given hash mechanism to convert 
	 * the message to a digest.
	 * @param md made as a parameter so that repeated calls to this method can be given only one instance.
	 * @param message string to be hashed
	 * @return message digest
	 * @throws Exception
	 */
	public static String generateMessageDigest(String message) 
	throws Exception {
		MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
		byte[] sb = message.getBytes("UTF-8");
		byte[] result = messageDigest.digest(sb);
		
		StringBuilder hexStr = new StringBuilder();
		  for (int i=0; i < result.length; i++) {
		    hexStr.append(Integer.toString( ( result[i] & 0xff ) + 0x100, 16).substring(1));
		}
		return hexStr.toString();
	}	
	
	public static double getElapsedTime(long startTimeNano) {
		long endTimeNano = System.nanoTime();		
		double diffSecs = (endTimeNano - startTimeNano)/(double)1000000000;
		return diffSecs;
	}
	
	public static long getIDFromStrVal(DBCollection idValCollection, 
			DBCollection eidValCollection, 
			String value, boolean isPredicate) throws Exception {
		// get hash digest of this value and then query DB
		String digestValue = Util.generateMessageDigest(value);
		DBObject resultID = null;
		if(isPredicate) {
			if(value.equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") ||
					value.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") ||
					value.equals("rdf:type"))
				return new Long(2);
			resultID = eidValCollection.findOne(new BasicDBObject(
					Constants.FIELD_HASH_VALUE, digestValue));
		}
		else 
			resultID = idValCollection.findOne(new BasicDBObject(
								Constants.FIELD_HASH_VALUE, digestValue));
		if(resultID == null)
			throw new Exception("ID not found for: " + value + 
					" and its digest value: " + digestValue);
		return (Long)resultID.get(Constants.FIELD_ID);
	}
}
