package com.googlecode.protobuf.pro.duplex.example.spring;

import org.springframework.context.annotation.Bean;

import com.googlecode.protobuf.pro.duplex.example.DefaultPingPongServiceImpl;


public class SpringConfig
{
	private static String PROTOSERVERHOST = "localhost";
	private static int PROTOSERVERPORT = 8090;
	
	//Implementation of the Service Interface
	@Bean(name="pingPongServiceImpl")
	public DefaultPingPongServiceImpl pingPongServiceImpl()
	{
		return new DefaultPingPongServiceImpl();
	}
	
	//Will start the server
    @Bean(name="pingSpringServer")
    public PingSpringServer pingSpringServer()
    {
    	return new PingSpringServer(PROTOSERVERHOST, PROTOSERVERPORT);
    }
}