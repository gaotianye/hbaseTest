package cn.gaotianye.redis;

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtils {
	JedisPool jedisPool = null;
	
	public RedisUtils(){
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxIdle(10);
		poolConfig.setMaxTotal(100);
		poolConfig.setMaxWaitMillis(10000);
		poolConfig.setTestOnBorrow(true);
		jedisPool = new JedisPool(poolConfig, "123.56.19.135", 6379);
	}
	
	public List<String> lrange(String key,int start,int end){
		Jedis resource = jedisPool.getResource();
		
		List<String> list = resource.lrange(key, start, end);
		resource.close();
		return list;
		
	}
	
	public void add(String key, String url) {
		Jedis resource = jedisPool.getResource();
		Long sadd = resource.sadd("set_"+key, url);
		if(sadd==1){
			resource.lpush("list_"+key, url);
		}
		resource.close();
	}
	
	public String poll(String key) {
		Jedis resource = jedisPool.getResource();
		String result = resource.rpop(key);
		resource.close();
		return result;
	}
	
	public static void main(String[] args) {
		RedisUtils utils = new RedisUtils();
		utils.add("sss", "gaotianye");
		System.out.println("ok");
	}
}
