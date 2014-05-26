package com.googlecode.protobuf.pro.duplex;

import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.RpcServiceRegistry.ServiceDescriptor;
import com.googlecode.protobuf.pro.duplex.test.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.test.PingPong.PingPongService;
import com.googlecode.protobuf.pro.duplex.test.PingPong.Pong;

public class RpcServiceRegistryTest {

	PingPongService.BlockingInterface bi = new PingPongService.BlockingInterface() {
		
		@Override
		public Pong ping(RpcController controller, Ping request)
				throws ServiceException {
			
			return null;
		}
		
		@Override
		public Pong fail(RpcController controller, Ping request)
				throws ServiceException {
			
			return null;
		}
	};

	PingPongService.Interface i = new PingPongService.Interface() {
		
		@Override
		public void ping(RpcController controller, Ping request,
				RpcCallback<Pong> done) {
		}
		
		@Override
		public void fail(RpcController controller, Ping request,
				RpcCallback<Pong> done) {
		}
	};
	
	
	@Test
	public void testRegisterUnregister() {
		RpcServiceRegistry r = new RpcServiceRegistry();

        BlockingService bPingService = PingPongService.newReflectiveBlockingService(bi);
        r.registerService(true, bPingService);

        Map<String, ServiceDescriptor> result = r.getServices();
        assertNotNull(result);
        assertEquals(1, result.size());
        /*
        Service nbPingService = PingPongService.newReflectiveService(i);
        r.registerService(true, nbPingService);
		*/
        
        r.removeService(bPingService);
        result = r.getServices();
        assertNotNull(result);
        assertEquals(0, result.size());
	}

	
}
