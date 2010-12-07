package com.googlecode.protobuf.pro.duplex.example.spring;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class PingSpringServerRunner {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringConfig.class);

		Thread.sleep(10000);
		
		System.exit(0);
	}

}
