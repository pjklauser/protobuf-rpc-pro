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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.Channel;

import com.google.protobuf.ByteString;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol.CloseNotification;
import com.googlecode.protobuf.pro.stream.wire.StreamProtocol.WirePayload;

/**
 * @author Peter Klauser
 *
 */
public class PushIn implements java.nio.channels.Channel {

	private static Log log = LogFactory.getLog(PushIn.class);

	private boolean open = true;
	
	private final int correlationId;
	
	private final Channel channel;
	
	private Map<String,String> parameters = new HashMap<String,String>();
	
	private ByteBuffer currentChunkData = null;
	private long totalBytesDelivered = 0;
	
	public PushIn( int correlationId, Channel channel ) {
		this.correlationId = correlationId;
		this.channel = channel;
	}
	
	/**
	 * @return the correlationId
	 */
	public int getCorrelationId() {
		return correlationId;
	}

	@Override
	public boolean isOpen() {
		return open;
	}

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

	void provideParameter( String parameterName, String parameterValue ) {
		parameters.put(parameterName, parameterValue);
	}
	
	/**
	 * Data provided in a push from the IO-layer.
	 * 
	 * @param chunkData
	 */
	void setData( ByteString chunkData ) {
		totalBytesDelivered += chunkData.size();
		
		currentChunkData = chunkData.asReadOnlyByteBuffer();
	}
	
	/**
	 * Close the PushIn and return if the caller actually closed it.
	 * 
	 * @return true if caller closed this or false if already closed.
	 */
	synchronized boolean setClosed() {
		boolean returnValue = open;
		open = false;
		return returnValue;
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

	/**
	 * @return the totalBytesDelivered
	 */
	public long getTotalBytesDelivered() {
		return totalBytesDelivered;
	}
}
