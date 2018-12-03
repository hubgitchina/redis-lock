package cn.com.ut.biz.redis;

import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import cn.com.ut.core.common.util.CommonUtil;

/**
 * Redis分布式锁工具类
 * 
 * @author wangpeng1
 * @since 2017年11月13日
 */
public class RedisLock {

	private static final Logger logger = LoggerFactory.getLogger(RedisLock.class);

	private RedisTemplate<String, Serializable> redisTemplate;

	private static final int DEFAULT_ACQUIRY_RESOLUTION_MILLIS = 100;

	/**
	 * 锁标识key
	 */
	private String lockKey;

	/**
	 * 锁隐藏值标识key
	 */
	private String lockKeyHidden;

	/**
	 * 锁超时时间，防止线程在入锁以后，无限的执行等待
	 */
	private int expireMsecs = 60 * 1000;

	/**
	 * 锁等待时间，防止线程饥饿
	 */
	private int timeoutMsecs = 10 * 1000;

	/**
	 * 线程睡眠毫秒数起始范围
	 */
	private int sleepStartMs = 100;

	/**
	 * 线程睡眠毫秒数结束范围
	 */
	private int sleepEndMs = 200;

	private volatile boolean locked = false;

	public String getLockKey() {

		return lockKey;
	}

	public String getLockKeyHidden() {

		return lockKeyHidden;
	}

	public RedisLock(RedisTemplate<String, Serializable> redisTemplate, String lockKey) {
		this.redisTemplate = redisTemplate;
		this.lockKey = lockKey + "_lock";
		this.lockKeyHidden = lockKey + "_lock_hidden";
	}

	public RedisLock(RedisTemplate<String, Serializable> redisTemplate, String lockKey,
			int timeoutMsecs) {
		this(redisTemplate, lockKey);
		this.timeoutMsecs = timeoutMsecs;
	}

	public RedisLock(RedisTemplate<String, Serializable> redisTemplate, String lockKey,
			int timeoutMsecs, int expireMsecs) {
		this(redisTemplate, lockKey, timeoutMsecs);
		this.expireMsecs = expireMsecs;
	}

	public RedisLock(RedisTemplate<String, Serializable> redisTemplate, String lockKey,
			int timeoutMsecs, int expireMsecs, int sleepStartMs, int sleepEndMs) {
		this(redisTemplate, lockKey, timeoutMsecs, expireMsecs);
		this.sleepStartMs = sleepStartMs;
		this.sleepEndMs = sleepEndMs;
	}

	private String get(final String key) {

		Object obj = null;
		try {
			obj = redisTemplate.execute(new RedisCallback<Object>() {
				@Override
				public Object doInRedis(RedisConnection connection) throws DataAccessException {

					StringRedisSerializer serializer = new StringRedisSerializer();
					byte[] data = connection.get(serializer.serialize(key));
					connection.close();
					if (data == null) {
						return null;
					}
					return serializer.deserialize(data);
				}
			});
		} catch (Exception e) {
			logger.error("获取Redis锁时间戳失败，key值为 : {}", key);
		}
		return obj != null ? obj.toString() : null;
	}

	private boolean setNX(final String key, final String value) {

		Object obj = null;
		try {
			obj = redisTemplate.execute(new RedisCallback<Object>() {
				@Override
				public Object doInRedis(RedisConnection connection) throws DataAccessException {

					StringRedisSerializer serializer = new StringRedisSerializer();
					Boolean success = connection.setNX(serializer.serialize(key),
							serializer.serialize(value));
					connection.close();
					return success;
				}
			});
		} catch (Exception e) {
			logger.error("Redis加锁失败，key值为 : {}", key);
		}
		return obj != null ? (Boolean) obj : false;
	}

	private String getSet(final String key, final String value) {

		Object obj = null;
		try {
			obj = redisTemplate.execute(new RedisCallback<Object>() {
				@Override
				public Object doInRedis(RedisConnection connection) throws DataAccessException {

					StringRedisSerializer serializer = new StringRedisSerializer();
					byte[] ret = connection.getSet(serializer.serialize(key),
							serializer.serialize(value));
					connection.close();
					return serializer.deserialize(ret);
				}
			});
		} catch (Exception e) {
			logger.error("Redis加锁失败，key值为 : {}", key);
		}
		return obj != null ? (String) obj : null;
	}

	/**
	 * 为了防止各服务器时间可能不一致，此处从Redis服务器获取时间
	 * 
	 * @return
	 */
	public long getTimeFromRedis() {

		return redisTemplate.execute(new RedisCallback<Long>() {
			@Override
			public Long doInRedis(RedisConnection connection) throws DataAccessException {

				return connection.time();
			}
		});
	}

	/**
	 * 获得 lock. 实现思路: 主要是使用了redis 的setnx命令,缓存了锁. reids缓存的key是锁的key,所有的共享,
	 * value是锁的到期时间(注意:这里把过期时间放在value了,没有时间上设置其超时时间) 执行过程:
	 * 1.通过setnx尝试设置某个key的值,成功(当前没有这个锁)则返回,成功获得锁
	 * 2.锁已经存在则获取锁的到期时间,和当前时间比较,超时的话,则设置新的值
	 */
	public synchronized boolean lock() throws InterruptedException {

		int timeout = timeoutMsecs;
		while (timeout >= 0) {

			String mac = PlatformUtils.MACAddress(); // 当前MAC
			int jvmPid = PlatformUtils.JVMPid(); // 当前JVM
			long threadId = Thread.currentThread().getId(); // 当前线程ID

			long expires = getTimeFromRedis() + expireMsecs + 1;
			String expiresStr = String.valueOf(expires); // 锁到期时间

			StringBuffer sbKeyValue = new StringBuffer();
			sbKeyValue.append(expiresStr).append(",").append(mac).append(",").append(jvmPid)
					.append(",").append(threadId);

			String keyValue = sbKeyValue.toString();
			logger.info("待加锁-" + keyValue);
			if (this.setNX(lockKey, keyValue)) {
				logger.info("加锁成功-" + keyValue);
				// 加锁成功同时给隐藏Key赋值，便于后续被其它线程覆盖时，好做比较还原
				this.getSet(lockKeyHidden, keyValue);
				// 获取锁成功
				locked = true;
				return true;
			}

			String currentValueStr = this.get(lockKey); // redis里的存储的数据，格式为"时间戳+MAC+JVMPid+ThreadId"
			// 如果这时刚好前一个锁被释放，则立刻调用加锁
			if (CommonUtil.isEmpty(currentValueStr)) {
				if (this.setNX(lockKey, keyValue)) {
					logger.info("加锁成功，锁刚被删除-" + keyValue);
					// 加锁成功同时给隐藏Key赋值，便于后续被其它线程覆盖时，好做比较还原
					this.getSet(lockKeyHidden, keyValue);
					// 获取锁成功
					locked = true;
					return true;
				}
			}
			// 判断是否为空，不为空的情况下，如果被其他线程设置了值，则第二个条件判断是过不去的
			if (CommonUtil.isNotEmpty(currentValueStr)
					&& Long.parseLong(currentValueStr.split(",")[0]) < getTimeFromRedis()) {
				// 进入这里表示锁已超时
				// 获取上一个锁到期时间，并设置现在的锁到期时间
				String oldValueStr = this.getSet(lockKey, keyValue);
				// 再次判断获取的旧值，如果为空，则表明刚刚有线程更快的抢到锁并解锁释放了资源
				if (CommonUtil.isEmpty(oldValueStr)) {
					String currentValueTemp = this.get(lockKey);
					if (currentValueTemp.equals(keyValue)) {
						logger.info("抢锁成功，锁刚被删除-" + keyValue);
						// 加锁成功同时给隐藏Key赋值，便于后续被其它线程覆盖时，好做比较还原
						this.getSet(lockKeyHidden, keyValue);
						locked = true;
						return true;
					}
				}
				// 只有一个线程才能获取上一个线上的设置时间，因为jedis.getSet是同步的，原子性的
				if (CommonUtil.isNotEmpty(oldValueStr) && oldValueStr.equals(currentValueStr)) {
					// 防止误删（覆盖，因为key是相同的）了他人的锁——这里达不到效果，这里值会被覆盖，但是因为什么相差了很少的时间，所以可以接受
					logger.info("抢锁成功-" + keyValue);
					// [分布式的情况下]:如过这个时候，多个线程恰好都到了这里，但是只有一个线程的设置值和当前值相同，他才有权利获取锁
					// 加锁成功同时给隐藏Key赋值，便于后续被其它线程覆盖时，好做比较还原
					this.getSet(lockKeyHidden, keyValue);
					// 获取锁成功
					locked = true;
					return true;
				}
				// 判断当存在某个线程紧跟另一个线程之后getSet值之后，出现的前后值不一致的情况
				if (CommonUtil.isNotEmpty(oldValueStr) && !oldValueStr.equals(currentValueStr)) {
					String setValueHidden = this.get(lockKeyHidden);
					logger.info("抢锁失败，还原已覆盖其它线程原始值-" + setValueHidden);
					// 对已获取锁，又遭到其它线程覆盖值的情况，做还原，将值重新设置为锁持有者的值
					this.getSet(lockKey, setValueHidden);
				}
			}
			timeout -= DEFAULT_ACQUIRY_RESOLUTION_MILLIS;

			int sleepTime = ThreadLocalRandom.current().nextInt(sleepStartMs, sleepEndMs);
			logger.info(keyValue + " - 休眠 " + sleepTime + " 毫秒");

			/*
			 * 延迟100 至200毫秒, 这里使用随机时间会好一点,可以防止饥饿进程的出现
			 * 当同时到达多个进程,只会有一个进程获得锁,其他的都用同样的频率进行尝试,后面有来了一些进行,也以同样的频率申请锁,
			 * 这将可能导致前面来的锁得不到满足. 使用随机的等待时间可以一定程度上保证公平性
			 */
			Thread.sleep(sleepTime);

		}
		return false;
	}

	/**
	 * 解锁方法，只有当处在上锁状态，时间戳已过期，或解锁的就是锁的持有者，才允许解锁
	 * 判断是否为锁拥有者，通过MAC，JVMPid，线程ID都相同，则表示当前解锁的就是锁的持有者，允许解锁
	 * 之所以判断是否是锁拥有者，就是为了防止如超时，此刻其它请求进来获取了锁，而此刻不判断直接解锁，会导致误删其它持有者的锁
	 * 此处还是存在一种情况，即当一个线程快速获取到锁的同时，其它线程通过getSet方法比对，修改了前一个线程持有锁的值，时间戳上虽然午餐极小，但是解锁的时候，会匹配不上持有者
	 */
	public synchronized void unlock() {

		if (locked) {
			String currentValueStr = this.get(lockKey);
			logger.info("解锁-" + currentValueStr);

			if (currentValueStr != null) {
				// 判断时间戳是否已过期，过期则直接解锁
				if (Long.parseLong(currentValueStr.split(",")[0]) < getTimeFromRedis()) {
					redisTemplate.delete(lockKey);
					redisTemplate.delete(lockKeyHidden);
					locked = false;
					logger.info("过期-解锁成功-" + currentValueStr);
				} else {
					boolean isSameMac = (currentValueStr.split(",")[1]
							.equals(PlatformUtils.MACAddress()));
					boolean isSameJvm = (Integer
							.parseInt(currentValueStr.split(",")[2]) == PlatformUtils.JVMPid());
					boolean isSameThread = (Long.parseLong(currentValueStr.split(",")[3]) == Thread
							.currentThread().getId());
					if (isSameMac && isSameJvm && isSameThread) {
						redisTemplate.delete(lockKey);
						redisTemplate.delete(lockKeyHidden);
						locked = false;
						logger.info("主动-解锁成功-" + currentValueStr);
					}
				}
			}
		}
	}

}
