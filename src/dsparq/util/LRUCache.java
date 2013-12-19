package dsparq.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An LRU Cache based on the description in the following URL.
 * URL: http://www.codewalk.com/2012/04/least-recently-used-lru-cache-implementation-java.html
 *
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {
	private static final long serialVersionUID = 1L;
	private int capacity;
	
	public LRUCache(int capacity) {
		super(capacity+1, 1.0f, true);
		this.capacity = capacity;
	}
	
	@Override
	protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
		return size() > capacity;
	}
}
