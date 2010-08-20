/**
 *   Copyright 2010 Peter Klauser
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
package com.googlecode.protobuf.pro.duplex.example;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PingService;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PongService;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Pong;

/**
 * PingService
 * 
 * @author Peter Klauser
 *
 */
public class DefaultPingPongServiceImpl implements PingService.Interface, PongService.Interface {
	
	public DefaultPingPongServiceImpl() {
	}
	
	@Override
	public void ping(RpcController controller, Ping request,
			RpcCallback<Pong> done) {
		doProcessing(controller, request.getNumber());
		
		Pong response = Pong.newBuilder().setNumber(request.getNumber()).setPongData(request.getPingData()).build();
		done.run(response);
		
	}

	@Override
	public void pong(RpcController controller, Pong request,
			RpcCallback<Ping> done) {
		doProcessing(controller, request.getNumber());
		
		Ping response = Ping.newBuilder().setNumber(request.getNumber()).setPingData(request.getPongData()).build();
		done.run(response);
	}

	void doProcessing( RpcController controller, int processingTime ) {
		long processUntil = System.currentTimeMillis() + processingTime;
		do {
				long sleepDuration = processUntil - System.currentTimeMillis();
				if ( sleepDuration > 0 ) {
					try {
						Thread.sleep(sleepDuration);
					} catch ( InterruptedException e ) {
						// RpcCancel interrupts Thread.sleep and NIO waits.
						
						Thread.currentThread().interrupt(); // propagate just in case we have later IO operations
						// this will be cleared after the RPC call returns.
						if ( controller.isCanceled() ) {
							// just get out
							return;
						}
					}
				}
			
		} while( System.currentTimeMillis() < processUntil );
	}

}
