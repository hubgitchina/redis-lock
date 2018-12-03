package cn.com.ut.biz.recipe.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/redislock")
public class RedisLockController {

	@Autowired
	private Service service;

	@ServiceComponent(session = false)
	@GetMapping("/redis")
	public void test() {

		for (int i = 0; i < 100; i++) {
			ThreadA threadA = new ThreadA(service);
			threadA.start();
		}
	}
	
}
