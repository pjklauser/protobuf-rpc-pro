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
package com.googlecode.protobuf.pro.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.Channel;

import com.google.protobuf.Message;
import com.googlecode.protobuf.pro.stream.logging.StreamLogger;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol.Chunk;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol.ChunkTypeCode;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol.CloseNotification;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol.Parameter;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol.PullRequest;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol.PushRequest;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol.WirePayload;

/**
 * @author Peter Klauser
 *
 */
public class StreamingClient<E extends Message, F extends Message> {

	private static Log log = LogFactory.getLog(StreamingClient.class);
	
	private final Map<Integer, TransferState<E,F>> pendingTransferMap = new ConcurrentHashMap<Integer, TransferState<E,F>>();
	
	private final Channel channel;
	
	private final PeerInfo clientInfo;
	private final PeerInfo serverInfo;
	private final int chunkSize;
	
	private StreamLogger streamLogger;
	private long lastUsedTS = System.currentTimeMillis();
	
	private AtomicInteger correlationId = new AtomicInteger(1);

	public StreamingClient( Channel channel, PeerInfo clientInfo, PeerInfo serverInfo, int chunkSize ) {
		this.channel = channel;
		this.clientInfo = clientInfo;
		this.serverInfo = serverInfo;
		this.chunkSize = chunkSize;
	}
	
	public TransferIn pull( E pullMessage ) {
		TransferIn transferIn = new TransferIn(getNextCorrelationId(), channel);
		TransferState<E,F> state = new TransferState<E,F>(System.currentTimeMillis(), pullMessage, null, transferIn, null);
		registerPendingRequest(transferIn.getCorrelationId(), state);
		
		PullRequest pullRequest =PullRequest.newBuilder().setCorrelationId(transferIn.getCorrelationId())
		.setRequestProto(pullMessage.toByteString()).build();  
	
		WirePayload payload = WirePayload.newBuilder().setPull(pullRequest).build();
		
		if ( log.isDebugEnabled() ) {
			log.debug("Sending ["+pullRequest.getCorrelationId()+"]PullRequest.");
		}
		channel.write(payload).awaitUninterruptibly();
	
		return transferIn;
	}

	public TransferOut push( F pushMessage ) {
		TransferOut transferOut = new TransferOut(getNextCorrelationId(), chunkSize, channel);
		TransferState<E,F> state = new TransferState<E,F>(System.currentTimeMillis(), null, pushMessage, null, transferOut);
		registerPendingRequest(transferOut.getCorrelationId(), state);
		
		PushRequest pushRequest =PushRequest.newBuilder().setCorrelationId(transferOut.getCorrelationId())
			.setRequestProto(pushMessage.toByteString()).build();  
		
		WirePayload payload = WirePayload.newBuilder().setPush(pushRequest).build();
		
		if ( log.isDebugEnabled() ) {
			log.debug("Sending ["+pushRequest.getCorrelationId()+"]PushRequest.");
		}
		channel.write(payload).awaitUninterruptibly();
		
		return transferOut;
	}
	
	public PeerInfo getPeerInfo() {
		return this.serverInfo;
	}
	
	@Override
	public String toString() {
		return "StreamClient->" + getPeerInfo();
	}
	
	/**
	 * A client Pushing data to a server can receive a closeNotification
	 * if the server side closes the transfer before the client finishes
	 * sending all data.
	 * 
	 * @param closeNotification
	 */
	public void closeNotification(CloseNotification closeNotification) {
		if ( log.isDebugEnabled() ) {
			log.debug("Received ["+closeNotification.getCorrelationId()+"]CloseNotification. Reason=" + closeNotification.getReason());
		}
		TransferState<E,F> state = removePendingTransfer(closeNotification.getCorrelationId());
		if ( state != null ) {
			TransferOut transferOut = state.getPushStream();
			if ( transferOut == null ) {
				throw new IllegalStateException("TransferState missing transferOut");
			}
			transferOut.handleClosure();
		} else {
			if ( log.isDebugEnabled() ) {
				log.debug("No PendingTransferState found for correlationId " + closeNotification.getCorrelationId());
			}
		}
	}
	
	/**
	 * The client has received a chunk pulled back from the server.
	 * 
	 * If the chunk is the last, then the transfer ends.
	 * 
	 * @param chunk
	 */
	public void pullChunk(Chunk chunk) {
		if ( log.isDebugEnabled() ) {
			log.debug("Received ["+chunk.getCorrelationId()+":"+chunk.getSeqNo()+"]PullChunk. " + chunk.getChunkType());
		}
		TransferState<E,F> state = lookupPendingTransfer(chunk.getCorrelationId());
		if ( state != null ) {
			TransferIn transferIn = state.getPullStream();
			if ( transferIn == null ) {
				throw new IllegalStateException("TransferState missing transferIn");
			}
			if ( chunk.getParameterCount() > 0 ) {
				for( Parameter parameter : chunk.getParameterList() ) {
					transferIn.provideParameter(parameter.getName(), parameter.getValue());
				}
			}
			transferIn.handleData(chunk.getPayload());
			if ( ChunkTypeCode.END == chunk.getChunkType() ) {
				removePendingTransfer(chunk.getCorrelationId());
				transferIn.handleClosure();
			}
		} else {
			// this can happen only under race conditions with close
			if ( log.isDebugEnabled() ) {
				log.debug("No PendingTransferState found for correlationId " + chunk.getCorrelationId());
			}
		}
	}
	
	public void handleClosure() {
		List<Integer> pendingTransferIds = new ArrayList<Integer>();
		pendingTransferIds.addAll(pendingTransferMap.keySet());
		do {
			for( Integer correlationId : pendingTransferIds ) {
				TransferState<E,F> state = removePendingTransfer(correlationId);
				if ( state != null ) {
					TransferIn transferIn = state.getPullStream();
					if ( transferIn != null ) {
						transferIn.handleClosure();
					}
					TransferOut transferOut = state.getPushStream();
					if ( transferOut != null ) {
						transferOut.handleClosure();
					}
				}
			}
		} while( pendingTransferMap.size() > 0 );
	}
	
	protected void doLog( TransferState<E,F> state, Message request, String errorMessage, Map<String,String> parameters ) {
		if ( streamLogger != null ) {
			streamLogger.logTransfer( clientInfo, serverInfo, request, errorMessage, state.getCorrelationId(), parameters, state.getStartTimestamp(), System.currentTimeMillis());
		}
	}
	
	private int getNextCorrelationId() {
		return correlationId.getAndIncrement();
	}
	
	private void registerPendingRequest(int seqId, TransferState<E,F> state) {
		updateLastUsed();
		if (pendingTransferMap.containsKey(seqId)) {
			throw new IllegalArgumentException("State already registered");
		}
		pendingTransferMap.put(seqId, state);
	}

	private TransferState<E,F> removePendingTransfer(int seqId) {
		updateLastUsed();
		return pendingTransferMap.remove(seqId);
	}

	private TransferState<E,F> lookupPendingTransfer(int seqId) {
		updateLastUsed();
		return pendingTransferMap.get(seqId);
	}

	private void updateLastUsed() {
		lastUsedTS = System.currentTimeMillis();
	}
	
	/**
	 * @return the clientInfo
	 */
	public PeerInfo getClientInfo() {
		return clientInfo;
	}

	/**
	 * @return the serverInfo
	 */
	public PeerInfo getServerInfo() {
		return serverInfo;
	}

	/**
	 * @return the channel
	 */
	public Channel getChannel() {
		return channel;
	}

	/**
	 * @return the streamLogger
	 */
	public StreamLogger getStreamLogger() {
		return streamLogger;
	}

	/**
	 * @param streamLogger the streamLogger to set
	 */
	public void setStreamLogger(StreamLogger streamLogger) {
		this.streamLogger = streamLogger;
	}

	/**
	 * @return the lastUsedTS
	 */
	public long getLastUsedTS() {
		return lastUsedTS;
	}

}
