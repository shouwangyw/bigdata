package com.yw.recommend.service.impl;

import com.yw.recommend.service.DemoService;

public class DemoServiceImpl implements DemoService {

	@Override
	public void sayName(String name) {
		 System.out.println("say name: " + name);
	}
}
