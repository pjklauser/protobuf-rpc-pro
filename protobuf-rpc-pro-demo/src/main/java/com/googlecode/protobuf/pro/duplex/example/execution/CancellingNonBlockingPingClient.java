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
package com.googlecode.protobuf.pro.duplex.example.execution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.pro.duplex.ClientRpcController;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.example.wire.DemoDescriptor;
import com.googlecode.protobuf.pro.duplex.example.wire.ExecutableClient;
import com.googlecode.protobuf.pro.duplex.example.wire.PercentCompleteCallback;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.BlockingPingService;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.NonBlockingPingService;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.PercentComplete;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.Pong;

public class CancellingNonBlockingPingClient implements ExecutableClient {

	private static Logger log = LoggerFactory.getLogger(CancellingNonBlockingPingClient.class);

	private DemoDescriptor config;
	private Throwable error;
	
	public CancellingNonBlockingPingClient( DemoDescriptor config ) {
		this.config = config;
	}
	
	@Override
	public void execute(RpcClientChannel channel){
		try {
			long startTS = System.currentTimeMillis();
			long endTS = 0;

			BlockingPingService.Interface blockingService = BlockingPingService.newStub(channel);
			NonBlockingPingService.Interface nonBlockingService = NonBlockingPingService.newStub(channel);
			for (int i = 0; i < config.getNumCalls(); i++) {
				if (i % 1000 == 1) {
					System.out.println(i);
				}
				final ClientRpcController controller = channel.newRpcController();
				controller.setTimeoutMs(config.getPingCall().getTimeoutMs());
				
				// we set a Oob response callback even if we dont request percentComplete messages
				// to be able to test if we receive any when we didn't ask.
				PercentCompleteCallback pcc = new PercentCompleteCallback(controller);
				controller.setOobResponseCallback( PercentComplete.getDefaultInstance(), pcc);
				
				Ping.Builder pingBuilder = Ping.newBuilder();
				pingBuilder.setSequenceNo(i);
				pingBuilder.setPingDurationMs(config.getPingCall().getDurationMs());
				pingBuilder.setPingPayload(config.getNewPayload());
				pingBuilder.setPingPercentComplete(config.getPingCall().isDoPercentCompleteNotification());
				if (config.getPongCall() != null) {
					pingBuilder.setPongRequired(true);
					pingBuilder.setPongBlocking(config.getPongCall().isCallBlockingImpl());
					pingBuilder.setPongDurationMs(config.getPongCall().getDurationMs());
					pingBuilder.setPongTimeoutMs(config.getPongCall().getTimeoutMs());
					pingBuilder.setPongPercentComplete(config.getPongCall().isDoPercentCompleteNotification());
				}
				Ping ping = pingBuilder.build();
				
				// we expect to cancel the call at 50% of the processing duration of the ping.
				// which preceeds any pong call from the server side.
				RpcCallback<PingPong.Pong> callback = new RpcCallback<PingPong.Pong>() {
					
					@Override
					public void run(Pong pong) {
						log.info("We got a " + pong);
						if ( pong == null ) {
							controller.storeCallLocalVariable("failure", Boolean.TRUE);
						} else {
							controller.storeCallLocalVariable("failure", Boolean.FALSE);
							controller.storeCallLocalVariable("pong", pong);
						}
					}
				};
				
				long callStartTs = System.currentTimeMillis();
				if ( config.getPingCall().isCallBlockingImpl()) {
					blockingService.ping(controller, ping, callback);
				} else {
					nonBlockingService.ping(controller, ping, callback);
				}
				Thread.sleep(config.getPingCall().getDurationMs());
				controller.startCancel();

				while( controller.getCallLocalVariable("failure") == null ) {
					Thread.sleep(100);
					if ( System.currentTimeMillis() - callStartTs > config.getPingCall().getDurationMs() ) {
						throw new Exception("Cancellation didn't complete call.");
					}
				}
				if ( controller.getCallLocalVariable("failure") != Boolean.TRUE) {
					throw new Exception("Expected cancellation failure.");
				} else {
					if ( !controller.failed() ) {
						throw new Exception("Expected controller failure.");
					}
					log.info("The call failed. " + controller.errorText());
					if ( !"Cancel".equals(controller.errorText())) {
						throw new Exception("errorText: \"Cancel\" expected, got " + controller.errorText());
					}
				}
 			}
			endTS = System.currentTimeMillis();
			log.info("BlockingCalls " + config.getNumCalls() + " in " + (endTS - startTS)
					/ 1000 + "s");
		} catch ( Throwable t) {
			error = t;
		}
	}

	@Override
	public Throwable getError() {
		return error;
	}

}
