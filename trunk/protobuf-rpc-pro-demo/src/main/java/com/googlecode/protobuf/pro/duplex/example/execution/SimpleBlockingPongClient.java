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

import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.ClientRpcController;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.example.wire.DemoDescriptor;
import com.googlecode.protobuf.pro.duplex.example.wire.ExecutableClient;
import com.googlecode.protobuf.pro.duplex.example.wire.PercentCompleteCallback;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.BlockingPongService;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.NonBlockingPongService;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.PercentComplete;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.Pong;

public class SimpleBlockingPongClient implements ExecutableClient {

	private static Logger log = LoggerFactory.getLogger(SimpleBlockingPongClient.class);

	private DemoDescriptor config;
	private Throwable error;
	
	public SimpleBlockingPongClient( DemoDescriptor config ) {
		this.config = config;
	}
	
	@Override
	public void execute(RpcClientChannel channel) {
		try {
			long startTS = 0;
			long endTS = 0;

			startTS = System.currentTimeMillis();

			BlockingPongService.BlockingInterface blockingService = BlockingPongService.newBlockingStub(channel);
			NonBlockingPongService.BlockingInterface nonBlockingService = NonBlockingPongService.newBlockingStub(channel);
			for (int i = 0; i < config.getNumCalls(); i++) {
				if (i % 1000 == 1) {
					System.out.println(i);
				}
				final ClientRpcController controller = channel.newRpcController();
				controller.setTimeoutMs(config.getPongCall().getTimeoutMs());
				
				// we set a Oob response callback even if we don't request percentComplete messages
				// to be able to test if we receive any when we didn't ask.
				PercentCompleteCallback pcc = new PercentCompleteCallback(controller);
				controller.setOobResponseCallback( PercentComplete.getDefaultInstance(), pcc);
				
				Pong.Builder pongBuilder = Pong.newBuilder();
				pongBuilder.setSequenceNo(i);
				pongBuilder.setPongDurationMs(config.getPongCall().getDurationMs());
				pongBuilder.setPongData(config.getNewPayload());
				pongBuilder.setPongPercentComplete(config.getPongCall().isDoPercentCompleteNotification());
				Pong pong = pongBuilder.build();
				Ping ping = null;
				try {
					if ( config.getPongCall().isCallBlockingImpl()){
						ping = blockingService.pong(controller, pong);
					} else {
						ping = nonBlockingService.pong(controller, pong);
					}
					if (ping.getPingPayload().size() != config.getPayloadSize()) {
						throw new ServiceException("Reply payload mismatch.");
					}
					if (pong.getSequenceNo() != ping.getSequenceNo()) {
						throw new ServiceException("Reply sequenceNo mismatch.");
					}
					if (pong.getPongPercentComplete()) {
						PercentComplete pc = pcc.getPercentComplete();
						if ( pc == null ) {
							throw new ServiceException("Missing % completion.");
						}
						if( pc.getSequenceNo() != pong.getSequenceNo()) {
							throw new ServiceException("% completion sequence number mismatch.");
						}
						if( pc.getPercentageComplete() != 100.0f ) {
							throw new ServiceException("% completion only " + pc.getPercentageComplete() + " for sequence number " + pc.getSequenceNo() + ".");
						}
					} else {
						if ( pcc.getPercentComplete() != null ) {
							throw new ServiceException("% completion not expected.");
						}
					}
				} catch ( ServiceException e ) {
					if ( "Timeout".equals(e.getMessage())) {
						// actual roundTripTime >= roundTripTime, we don't know by how much, but we add a 1s safety factor
						int roundTripTime = config.getPongCall().getDurationMs();
						roundTripTime += 1000;
						if ( roundTripTime >= config.getPongCall().getTimeoutMs() ) {
							// timeout is ok
						} else {
							throw e;
						}
					} else {
						throw e;
					}
				}
			}
			endTS = System.currentTimeMillis();
			log.info("BlockingCalls " + config.getNumCalls() + " in " + (endTS - startTS)
					/ 1000 + "s");
		} catch ( Throwable t ) {
			this.error = t;
		}
	}

	@Override
	public Throwable getError() {
		return this.error;
	}

}
