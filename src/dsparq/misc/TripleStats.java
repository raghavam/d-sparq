package dsparq.misc;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.util.Hashing;

import dsparq.util.Util;

public class TripleStats {

	public void getTypeCount(String path) throws Exception {
		FileInputStream in = new FileInputStream(path);
		NxParser nxParser = new NxParser(in);
		long tripleCount = 0;
		// nimbus2:6479,nimbus3:6479,nimbus4:6479,nimbus5:6479,nimbus6:6479
		List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
		shards.add(new JedisShardInfo("nimbus2", 6479));
		shards.add(new JedisShardInfo("nimbus3", 6479));
		shards.add(new JedisShardInfo("nimbus4", 6479));
		shards.add(new JedisShardInfo("nimbus5", 6479));
		shards.add(new JedisShardInfo("nimbus6", 6479));
		ShardedJedis jedis = new ShardedJedis(shards, Hashing.MURMUR_HASH);
		
		while(nxParser.hasNext()) {
			Node[] nodes = nxParser.next();
			String subDigestValue = Util.generateMessageDigest(nodes[0].toString());
			String predDigestValue = Util.generateMessageDigest(nodes[1].toString());
			String objDigestValue = Util.generateMessageDigest(nodes[2].toString());
			String subID = jedis.hget(subDigestValue, "id");
			if(subID == null) {
				jedis.disconnect();
				throw new Exception("Null value for subject: " + nodes[0].toString());
			}
			Jedis predShard = jedis.getShard(predDigestValue);
			predShard.select(1);
			String predID = predShard.hget(predDigestValue, "id");
			predShard.select(0);
			if(predID == null) {
				jedis.disconnect();
				throw new Exception("Null value for predicate: " + nodes[1].toString());
			}
			String objID = jedis.hget(objDigestValue, "id");
			if(objID == null) {
				jedis.disconnect();
				throw new Exception("Null value for object: " + nodes[2].toString());
			}
			
			tripleCount++;
			if(tripleCount == 100000)
				break;
		}
		in.close();
		jedis.disconnect();
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length != 1) {
			System.out.println("Specify the path to the triples file.");
			System.exit(-1);
		}
		new TripleStats().getTypeCount(args[0]);
	}
}
