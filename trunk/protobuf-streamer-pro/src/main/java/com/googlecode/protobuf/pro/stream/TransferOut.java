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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.Channel;

import com.google.protobuf.ByteString;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol.Chunk;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol.ChunkTypeCode;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol.Parameter;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol.WirePayload;

/**
 * @author Peter Klauser
 *
 */
public class TransferOut implements WritableByteChannel {

	private static Log log = LogFactory.getLog(TransferOut.class);

	private final int correlationId;
	private final AtomicInteger seqNo = new AtomicInteger(1);
	
	private boolean open = true;
	
	private final Channel channel;
	
	private int chunkPosition = 0;
	private final byte[] chunkBytes;
	private long totalBytesWritten = 0;
	
	private Map<String,String> parameters = new HashMap<String,String>();
	private Set<String> writtenParameterNames = new HashSet<String>();
	
	public TransferOut( int correlationId, int maxChunkSize, Channel channel ) {
		this.correlationId = correlationId;
		this.chunkBytes = new byte[maxChunkSize];
		this.channel = channel;
	}
	
	/* (non-Javadoc)
	 * @see java.nio.channels.Channel#isOpen()
	 */
	@Override
	public boolean isOpen() {
		return open;
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.Channel#close()
	 */
	@Override
	public synchronized void close() throws IOException {
		if ( !open ) {
			return;
		}
		open = false;
		
		Chunk.Builder chunkBuilder = Chunk.newBuilder().setChunkType(ChunkTypeCode.END).setCorrelationId(correlationId);
		chunkBuilder.setSeqNo(seqNo.getAndIncrement());
		chunkBuilder.setPayload(ByteString.copyFrom(chunkBytes, 0, chunkPosition));
		addParameters(chunkBuilder);
		Chunk chunk = chunkBuilder.build();
		
		WirePayload payload = WirePayload.newBuilder().setChunk(chunk).build();
		chunkPosition = 0;

		if ( log.isDebugEnabled() ) {
			log.debug("Sending ["+chunk.getCorrelationId()+":"+chunk.getSeqNo()+"]Chunk. " + chunk.getChunkType() + " size=" + totalBytesWritten);
		}
		
		channel.write(payload).awaitUninterruptibly();
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.WritableByteChannel#write(java.nio.ByteBuffer)
	 */
	@Override
	public synchronized int write(ByteBuffer src) throws IOException {
		if ( !open ) {
			throw new ClosedChannelException();
		}
		
		int writtenBytes = 0;
		int n = 0;
		while ( (n = src.remaining() ) > 0 ) {
			
			int sizeLeftInChunk = chunkBytes.length - chunkPosition;
			if ( n > sizeLeftInChunk ) {
				src.get(chunkBytes, chunkPosition, sizeLeftInChunk);

				// flush chunk to IO-Layer
				ChunkTypeCode type = totalBytesWritten == 0 ? ChunkTypeCode.START : ChunkTypeCode.MIDDLE;
				
				Chunk.Builder chunkBuilder = Chunk.newBuilder().setChunkType(type).setCorrelationId(correlationId);
				chunkBuilder.setSeqNo(seqNo.getAndIncrement());
				chunkBuilder.setPayload(ByteString.copyFrom(chunkBytes));
				addParameters(chunkBuilder);
				Chunk chunk = chunkBuilder.build();

				WirePayload payload = WirePayload.newBuilder().setChunk(chunk).build();

				writtenBytes += sizeLeftInChunk;
				chunkPosition = 0;

				if ( log.isDebugEnabled() ) {
					log.debug("Sending ["+chunk.getCorrelationId()+":"+chunk.getSeqNo()+"]Chunk. " + chunk.getChunkType());
				}
				
				channel.write(payload).awaitUninterruptibly();
			} else {
				// the chunk is not full yet, copy in the available bytes for later transfer
				src.get(chunkBytes, chunkPosition, n);

				writtenBytes += n;
				chunkPosition += n;
			}
		}
		totalBytesWritten += writtenBytes;
		return writtenBytes;
	}

	public synchronized void addParameter( String parameterName, String parameterValue ) {
		parameters.put(parameterName, parameterValue);
		writtenParameterNames.remove(parameterName);
	}
	
	/**
	 * A TransferOut becomes unusable if the underlying IO layer closes or
	 * the remote side sends a closeNotification.
	 */
	void handleClosure() {
		open = false;
	}
	
	/**
	 * @return the correlationId
	 */
	public int getCorrelationId() {
		return correlationId;
	}

	private void addParameters( Chunk.Builder chunk ) {
		//add parameters which have changed since last transferred or new
		for( String parameterName : parameters.keySet() ) {
			if ( !writtenParameterNames.contains( parameterName )) {
				Parameter p = Parameter.newBuilder().setName(parameterName).setValue(parameters.get(parameterName)).build();
				chunk.addParameter(p);
				
				writtenParameterNames.add(parameterName);
			}
		}
	}
	
}
