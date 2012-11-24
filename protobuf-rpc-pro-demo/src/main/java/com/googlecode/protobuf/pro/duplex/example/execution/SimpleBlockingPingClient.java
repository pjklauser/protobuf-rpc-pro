package com.googlecode.protobuf.pro.duplex.example.execution;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.ClientRpcController;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.example.wire.DemoDescriptor;
import com.googlecode.protobuf.pro.duplex.example.wire.ExecutableClient;
import com.googlecode.protobuf.pro.duplex.example.wire.PercentCompleteCallback;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.BlockingPingService;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.NonBlockingPingService;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.PercentComplete;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.Pong;

public class SimpleBlockingPingClient implements ExecutableClient {

	private static Log log = LogFactory.getLog(SimpleBlockingPingClient.class);

	private DemoDescriptor config;
	private Throwable error;
	
	public SimpleBlockingPingClient( DemoDescriptor config ) {
		this.config = config;
	}
	
	@Override
	public void execute(RpcClientChannel channel) {
		try {
			long startTS = 0;
			long endTS = 0;

			startTS = System.currentTimeMillis();

			BlockingPingService.BlockingInterface blockingService = BlockingPingService.newBlockingStub(channel);
			NonBlockingPingService.BlockingInterface nonBlockingService = NonBlockingPingService.newBlockingStub(channel);
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
				Pong pong = null;
				try {
					if ( config.getPingCall().isCallBlockingImpl()){
						pong = blockingService.ping(controller, ping);
					} else {
						pong = nonBlockingService.ping(controller, ping);
					}
					if (pong.getPongData().size() != config.getPayloadSize()) {
						throw new ServiceException("Reply payload mismatch.");
					}
					if (pong.getSequenceNo() != ping.getSequenceNo()) {
						throw new ServiceException("Reply sequenceNo mismatch.");
					}
					if (ping.getPingPercentComplete()) {
						PercentComplete pc = pcc.getPercentComplete();
						if ( pc == null ) {
							throw new ServiceException("Missing % completion.");
						}
						if( pc.getSequenceNo() != ping.getSequenceNo()) {
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
						int roundTripTime = config.getPingCall().getDurationMs();
						if ( config.getPongCall() != null ) {
							roundTripTime += config.getPongCall().getDurationMs();
							roundTripTime += 1000;
						}
						roundTripTime += 1000;
						if ( roundTripTime >= config.getPingCall().getTimeoutMs() ) {
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
