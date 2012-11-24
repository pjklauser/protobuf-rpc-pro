package com.googlecode.protobuf.pro.duplex.example.program;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

	private static Log log = LogFactory.getLog(AllClientTests.class);

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
	}
}
