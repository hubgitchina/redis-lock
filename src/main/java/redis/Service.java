package cn.com.ut.biz.redis;

import java.io.Serializable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class Service {
	@Autowired
	private RedisTemplate<String, Serializable> redisTemplate;

	int n = 500;

	public void kill() {

		RedisLock lock = new RedisLock(redisTemplate, "test", 10000, 20000, 0, 100);
		try {
			if (lock.lock()) {
				System.out.println(Thread.currentThread().getId() + "获得了锁");
				System.out.println(--n);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			System.out.println("准备解锁");
			lock.unlock();
		}
	}
}
