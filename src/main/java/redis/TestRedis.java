package cn.com.ut.biz.redis;

public class TestRedis {
	public static void main(String[] args) {

		Service service = new Service();
		for (int i = 0; i < 100; i++) {
			ThreadA threadA = new ThreadA(service);
			threadA.start();
		}
	}
}
