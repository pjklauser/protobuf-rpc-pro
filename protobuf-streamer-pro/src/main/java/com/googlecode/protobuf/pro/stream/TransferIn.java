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
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.Channel;

import com.google.protobuf.ByteString;
import com.googlecode.protobuf.pro.stream.server.PushHandler;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol.CloseNotification;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol.WirePayload;

/**
 * @author Peter Klauser
 *
 */
public class TransferIn implements ReadableByteChannel {

	private static Log log = LogFactory.getLog(TransferIn.class);

	private boolean open = true;
	
	private final int correlationId;
	
	private final Channel channel;
	
	private Map<String,String> parameters = new HashMap<String,String>();
	
	private SynchronousQueue<ByteBuffer> handoffQueue = new SynchronousQueue<ByteBuffer>();
	private ByteBuffer currentChunkData = null;
	private long totalBytesRead = 0;
	
	private static final ByteBuffer CLOSE = ByteBuffer.allocate(1);
	
	public TransferIn( int correlationId, Channel channel ) {
		this.correlationId = correlationId;
		this.channel = channel;
	}
	
	/**
	 * @return the correlationId
	 */
	public int getCorrelationId() {
		return correlationId;
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
	public void close() throws IOException {
		if ( !open ) {
			return;
		}
		open = false;
		
		//we need to send a closeNotification to the remote server
		//so it can stop transfer!
		CloseNotification closeNotification = CloseNotification.newBuilder().setCorrelationId(correlationId).build();
		WirePayload payload = WirePayload.newBuilder().setClose(closeNotification).build();

		if ( log.isDebugEnabled() ) {
			log.debug("Sending ["+closeNotification.getCorrelationId()+"]CloseNotification.");
		}
		
		channel.write(payload).awaitUninterruptibly();
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.ReadableByteChannel#read(java.nio.ByteBuffer)
	 */
	@Override
	public synchronized int read(ByteBuffer dst) throws IOException {
		if ( !isOpen() ) {
			return -1;
		}
		int transferrableBytes = dst.remaining();
		
		while( isOpen() ) {
			while( isOpen() && currentChunkData == null ) {
				try {
					currentChunkData = handoffQueue.poll(60, TimeUnit.SECONDS);
					if ( currentChunkData == CLOSE ) {
						currentChunkData = null;
						open = false;
						// isOpen will be false - so we fall through 
						if ( log.isDebugEnabled() ) {
							log.debug("Closed after receiving " + totalBytesRead + " bytes.");
						}
					}
				} catch ( InterruptedException e ) {
					Thread.currentThread().interrupt();
					if ( !isOpen() ) {
						ClosedByInterruptException cie = new ClosedByInterruptException();
						cie.setStackTrace(e.getStackTrace());
						throw cie;
					} else {
						InterruptedIOException iio = new InterruptedIOException();
						iio.setStackTrace(e.getStackTrace());
						throw iio;
					}
				}
			}
			
			if ( isOpen() && currentChunkData != null ) {
				int chunkBytes = currentChunkData.remaining(); 
				if ( chunkBytes > transferrableBytes ) {
					// transfer transferable bytes and the chunk is not finished
					for( int i = 0; i < transferrableBytes;i++) {
						dst.put(currentChunkData.get());
					}
					totalBytesRead += transferrableBytes;
					return transferrableBytes;
				} else {
					// transfer the entire chunk and it is finished
					dst.put(currentChunkData);
					currentChunkData = null;
					totalBytesRead += chunkBytes;
					return chunkBytes;
				}
			}
		}
		return isOpen() ? 0 : -1;
	}

	void provideParameter( String parameterName, String parameterValue ) {
		parameters.put(parameterName, parameterValue);
	}
	
	/**
	 * HandleData will block until another Thread reads the chunk
	 * data in {@link #read(ByteBuffer)} for up to 5 minutes. The
	 * thread calling this is the IO thread, so we don't fill up
	 * memory if the thread reading and processing the pull data
	 * is slow.
	 * 
	 * If the reader closes the channel we don't handle and more
	 * data in-bound.
	 * 
	 * @param chunkData
	 */
	void handleData( ByteString chunkData ) {
		if (!isOpen() ) {
			if ( log.isDebugEnabled() ) {
				log.debug("handleData after close.");
			}
			return;
		}
		try {
			ByteBuffer roBytes = chunkData.asReadOnlyByteBuffer();

			if ( !handoffQueue.offer(roBytes,600, TimeUnit.SECONDS) ) {
				log.warn("Failed to handleData after 5min.");
			}
		} catch ( InterruptedException e ) {
			Thread.currentThread().interrupt();
		}
	}
	
	/**
	 * Alternative server side push handling with {@link PushHandler}
	 * 
	 * @param chunkData
	 */
	void setData( ByteString chunkData ) {
		currentChunkData = chunkData.asReadOnlyByteBuffer();
	}
	
	void handleClosure() {
		try {
			// receiving the CLOSE message will cause the reading thread
			// to close the transfer.
			if ( !handoffQueue.offer(CLOSE, 600, TimeUnit.SECONDS) ) {
				// if after 5 minutes, no reader has taken the message, force close.
				setClosed();
			}
		} catch ( InterruptedException e ) {
			Thread.currentThread().interrupt();
		}
	}

	void setClosed() {
		open = false;
	}
	
	/**
	 * @return the currentChunkData
	 */
	public ByteBuffer getCurrentChunkData() {
		return currentChunkData;
	}

	/**
	 * @return the parameters
	 */
	public Map<String, String> getParameters() {
		return parameters;
	}
}
