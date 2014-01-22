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
import com.googlecode.protobuf.pro.duplex.example.execution.CancellingNonBlockingPingClient;
import com.googlecode.protobuf.pro.duplex.example.execution.SimpleBlockingPingClient;
import com.googlecode.protobuf.pro.duplex.example.execution.SimpleBlockingPongClient;
import com.googlecode.protobuf.pro.duplex.example.wire.ClientExecutor;
import com.googlecode.protobuf.pro.duplex.example.wire.DemoDescriptor;
import com.googlecode.protobuf.pro.duplex.example.wire.ExecutableClient;
import com.googlecode.protobuf.pro.duplex.example.wire.ExecutableProgram;
import com.googlecode.protobuf.pro.duplex.server.RpcClientRegistry;

public class AllClientTests implements ExecutableProgram {

	private static Logger log = LoggerFactory.getLogger(AllClientTests.class);

	public AllClientTests() {
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
    	
    	// server blocking ping with reverse pong - 100 calls, no timeout, no processing time
    	config = new DemoDescriptor(100, 1, new DemoDescriptor.CallDescriptor(0,0,true,false), new DemoDescriptor.CallDescriptor(0,0,true,false));
    	c = new SimpleBlockingPingClient(config);
    	exec.execute(c,channel);
    	
    	// server non blocking ping with reverse blocking pong - 100 calls, no timeout, no processing time
    	config = new DemoDescriptor(100, 1, new DemoDescriptor.CallDescriptor(0,0,false,false), new DemoDescriptor.CallDescriptor(0,0,true,false));
    	c = new SimpleBlockingPingClient(config);
    	exec.execute(c,channel);
    	
    	// server non blocking ping with reverse non blocking pong - 100 calls, no timeout, no processing time
    	config = new DemoDescriptor(100, 1, new DemoDescriptor.CallDescriptor(0,0,false,false), new DemoDescriptor.CallDescriptor(0,0,false,false));
    	c = new SimpleBlockingPingClient(config);
    	exec.execute(c,channel);
    	
    	// pong call
    	c = new SimpleBlockingPongClient(new DemoDescriptor(10, 100, null, new DemoDescriptor.CallDescriptor(100,0,false,false)));
    	exec.execute(c, channel);
    	
    	// cancelled ping
    	c = new CancellingNonBlockingPingClient(new DemoDescriptor(10, 100, new DemoDescriptor.CallDescriptor(100,0,false,false)));
    	exec.execute(c,channel);

    	// simple ping - no timeout, no processing time, no percentComplete
    	c = new SimpleBlockingPingClient(new DemoDescriptor(1, 100, new DemoDescriptor.CallDescriptor(0,0,false,false)));
    	exec.execute(c,channel);
    	
    	// simple ping - 10s timeout, 1s processing time, no percentComplete
    	c = new SimpleBlockingPingClient(new DemoDescriptor(1, 100, new DemoDescriptor.CallDescriptor(1000,10000,false,false)));
    	exec.execute(c,channel);
    	
    	// simple ping - 10s timeout, 5s processing time, percentComplete
    	c = new SimpleBlockingPingClient(new DemoDescriptor(1, 100, new DemoDescriptor.CallDescriptor(5000,10000,false,true)));
    	exec.execute(c,channel);
    	
    	// server non blocking ping - no timeout, no processing time
    	config = new DemoDescriptor(1000, 100, new DemoDescriptor.CallDescriptor(0,0,false,false));
    	c = new SimpleBlockingPingClient(config);
    	exec.execute(c,channel);
    	
    	// server non blocking ping - no timeout, no processing time
    	config = new DemoDescriptor(20, 100, new DemoDescriptor.CallDescriptor(3000,0,true,true));
    	c = new SimpleBlockingPingClient(config);
    	exec.execute(c,channel);
    	
    	ExecutableClient[] clients=new ExecutableClient[10];
    	for( int i = 0; i < clients.length;i++) {
    		clients[i] = new SimpleBlockingPingClient(config);
    	}
    	exec.execute(clients,channel);

    	// server blocking ping with reverse pong - 20 calls, no timeout, no processing time, but pong times out after 0.5s
    	config = new DemoDescriptor(20, 1, new DemoDescriptor.CallDescriptor(0,0,true,false), new DemoDescriptor.CallDescriptor(1000,500,true,false));
    	c = new SimpleBlockingPingClient(config);
    	exec.execute(c,channel);
    	
    	// server nonblocking ping with reverse nonblocking pong - 20 calls, no timeout, no processing time, but pong times out after 0.5s
    	config = new DemoDescriptor(20, 1, new DemoDescriptor.CallDescriptor(0,0,false,false), new DemoDescriptor.CallDescriptor(1000,500,false,false));
    	c = new SimpleBlockingPingClient(config);
    	exec.execute(c,channel);
    	
	}
}
