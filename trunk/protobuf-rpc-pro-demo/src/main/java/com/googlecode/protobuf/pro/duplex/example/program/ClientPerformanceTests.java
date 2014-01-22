/**
 *   Copyright 2010-2014 Peter Klauser
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
package com.googlecode.protobuf.pro.duplex.example.program;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.example.execution.SimpleBlockingPingClient;
import com.googlecode.protobuf.pro.duplex.example.wire.ClientExecutor;
import com.googlecode.protobuf.pro.duplex.example.wire.DemoDescriptor;
import com.googlecode.protobuf.pro.duplex.example.wire.ExecutableClient;
import com.googlecode.protobuf.pro.duplex.example.wire.ExecutableProgram;
import com.googlecode.protobuf.pro.duplex.server.RpcClientRegistry;

public class ClientPerformanceTests implements ExecutableProgram {

	private static Logger log = LoggerFactory.getLogger(ClientPerformanceTests.class);

	public ClientPerformanceTests() {
	}
	
	@Override
	public void execute(RpcClientRegistry registry) throws Throwable {
		List<RpcClientChannel> channels = registry.getAllClients();
		if ( channels.size() <= 0) {
			throw new Exception("No channels.");
		}
		for( RpcClientChannel channel : channels ) {
			doTests(channel);
		}
	}

	protected void doTests(RpcClientChannel channel) throws Throwable {
    	ExecutableClient c = null;
    	ClientExecutor exec = new ClientExecutor();
    	DemoDescriptor config = null;
    	log.warn("Start Perf Test");
    	// server blocking ping with reverse pong - 100 calls, no timeout, no processing time
    	config = new DemoDescriptor(10000, 100000, new DemoDescriptor.CallDescriptor(0,0,true,false), new DemoDescriptor.CallDescriptor(0,0,true,false));
    	c = new SimpleBlockingPingClient(config);
    	exec.execute(c,channel);
    	
    	// server non blocking ping - no timeout, no processing time
    	config = new DemoDescriptor(10000, 100000, new DemoDescriptor.CallDescriptor(0,0,true,false));
    	c = new SimpleBlockingPingClient(config);
    	exec.execute(c,channel);
    	
    	ExecutableClient[] clients=new ExecutableClient[10];
    	for( int i = 0; i < clients.length;i++) {
    		clients[i] = new SimpleBlockingPingClient(config);
    	}
    	exec.execute(clients,channel);

    	log.warn("End Perf Test");
	}
}
