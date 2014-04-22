package dsparq.misc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * This class handles all the read requests for
 * the ShardInfo.properties file. 
 * 
 * @author Raghava
 */
public class PropertyFileHandler {
	private final static PropertyFileHandler propertyFileHandler = new PropertyFileHandler();
	private Properties shardInfoProperties = null;
	private final String PROPERTY_FILE = "ShardInfo.properties";
	
	private PropertyFileHandler() {
		// does not allow instantiation of this class
		try {
			shardInfoProperties = new Properties();
//			InputStream stream = getClass().getResourceAsStream("/" + PROPERTY_FILE);
			shardInfoProperties.load(new FileInputStream(PROPERTY_FILE));
		}
		catch(FileNotFoundException e) {
			e.printStackTrace();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public static PropertyFileHandler getInstance() {
		return propertyFileHandler;
	}
	
	public Object clone() throws CloneNotSupportedException {
		throw new CloneNotSupportedException("Cannot clone an instance of this class");
	}
	
	public List<HostInfo> getAllShardsInfo() {
		List<HostInfo> hostList = new ArrayList<HostInfo>();
		String shardCountStr = shardInfoProperties.getProperty("shard.count");
		int shardCount = Integer.parseInt(shardCountStr);
		for(int i=1; i<=shardCount; i++) {			
			String[] hostPort = shardInfoProperties.getProperty("shard" + i).trim().split(":");
			HostInfo hostInfo = new HostInfo(hostPort[0], Integer.parseInt(hostPort[1]));
			hostList.add(hostInfo);
		}		
		return hostList;
	}
	
	//using only one mongos now
	@Deprecated
	public List<HostInfo> getAllMongoRouters() {
		List<HostInfo> hostList = new ArrayList<HostInfo>();
		String shardCountStr = shardInfoProperties.getProperty("mongos.count");
		int shardCount = Integer.parseInt(shardCountStr);
		for(int i=1; i<=shardCount; i++) {			
			String[] hostPort = shardInfoProperties.getProperty("mongos" + i).trim().split(":");
			HostInfo hostInfo = new HostInfo(hostPort[0], Integer.parseInt(hostPort[1]));
			hostList.add(hostInfo);
		}		
		return hostList;
	}
	
	public String getMongoRouter() {
		return shardInfoProperties.getProperty("mongo.router");
	}
	
	public int getMongosCount() {
		return Integer.parseInt(shardInfoProperties.getProperty("mongos.count"));
	}
	
	public int getShardCount() {
		return Integer.parseInt(shardInfoProperties.getProperty("shard.count"));
	}
	
	public HostInfo getMongoSHostInfo() {
		String[] mongosHostPort = shardInfoProperties.getProperty("mongos").
										trim().split(":");
		HostInfo hostInfo = new HostInfo(mongosHostPort[0], 
				Integer.parseInt(mongosHostPort[1]));	
		return hostInfo;
	}
	
	public String getRedisHosts() {
		return shardInfoProperties.getProperty("redis.hosts");
	}
}


