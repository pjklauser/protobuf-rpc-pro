/**
 *   Copyright 2010-2013 Peter Klauser
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
*/
package com.googlecode.protobuf.pro.duplex.example.spring;

import org.springframework.context.annotation.Bean;

import com.googlecode.protobuf.pro.duplex.example.PingPongServiceFactory;


public class SpringConfig
{
	private static String PROTOSERVERHOST = "localhost";
	private static int PROTOSERVERPORT = 8090;
	
	//Implementation of the Service Interface
	@Bean(name="pingPongServiceImpl")
	public PingPongServiceFactory.NonBlockingPingServer pingPongServiceImpl()
	{
		return new PingPongServiceFactory.NonBlockingPingServer();
	}
	
	//Will start the server
    @Bean(name="pingSpringServer")
    public PingSpringServer pingSpringServer()
    {
    	return new PingSpringServer(PROTOSERVERHOST, PROTOSERVERPORT);
    }
}