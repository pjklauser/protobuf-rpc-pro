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
package com.googlecode.protobuf.pro.duplex.example.wire;

import com.google.protobuf.ByteString;

public class DemoDescriptor {

	private int numCalls;
	private int payloadSize;
	private CallDescriptor pingCall;
	private CallDescriptor pongCall;	// if not set, no Pong

	public DemoDescriptor( int numCalls, int payloadSize, CallDescriptor ping ) {
		this( numCalls, payloadSize, ping, null);
	}
	
	public DemoDescriptor( int numCalls, int payloadSize, CallDescriptor ping, CallDescriptor pong ) {
		this.numCalls = numCalls;
		this.payloadSize = payloadSize;
		this.pingCall = ping;
		this.pongCall = pong;
	}
	
	public ByteString getNewPayload() {
		byte[] payload = new byte[payloadSize];
		for( int i = 0; i < payload.length; i++) {
			payload[i] = (byte)(i % 10);
		}
		return ByteString.copyFrom(payload);		
	}
	
	public static class CallDescriptor {
		private int durationMs;
		private int timeoutMs;
		private boolean callBlockingImpl;
		private boolean doPercentCompleteNotification; // duration / 10 => updates @ 10%
		
		public CallDescriptor( int durationMs, int timeoutMs, boolean callBlockingImpl, boolean doPercentCompleteNotification) {
			this.durationMs = durationMs;
			this.timeoutMs = timeoutMs;
			this.callBlockingImpl = callBlockingImpl;
			this.doPercentCompleteNotification = doPercentCompleteNotification;			
		}
		
		/**
		 * @return the durationMs
		 */
		public int getDurationMs() {
			return durationMs;
		}
		/**
		 * @param durationMs the durationMs to set
		 */
		public void setDurationMs(int durationMs) {
			this.durationMs = durationMs;
		}
		/**
		 * @return the timeoutMs
		 */
		public int getTimeoutMs() {
			return timeoutMs;
		}
		/**
		 * @param timeoutMs the timeoutMs to set
		 */
		public void setTimeoutMs(int timeoutMs) {
			this.timeoutMs = timeoutMs;
		}
		/**
		 * @return the callBlockingImpl
		 */
		public boolean isCallBlockingImpl() {
			return callBlockingImpl;
		}
		/**
		 * @param callBlockingImpl the callBlockingImpl to set
		 */
		public void setCallBlockingImpl(boolean callBlockingImpl) {
			this.callBlockingImpl = callBlockingImpl;
		}
		/**
		 * @return the doPercentCompleteNotification
		 */
		public boolean isDoPercentCompleteNotification() {
			return doPercentCompleteNotification;
		}
		/**
		 * @param doPercentCompleteNotification the doPercentCompleteNotification to set
		 */
		public void setDoPercentCompleteNotification(
				boolean doPercentCompleteNotification) {
			this.doPercentCompleteNotification = doPercentCompleteNotification;
		}
	}


	/**
	 * @return the pingCall
	 */
	public CallDescriptor getPingCall() {
		return pingCall;
	}


	/**
	 * @param pingCall the pingCall to set
	 */
	public void setPingCall(CallDescriptor pingCall) {
		this.pingCall = pingCall;
	}


	/**
	 * @return the pongCall
	 */
	public CallDescriptor getPongCall() {
		return pongCall;
	}


	/**
	 * @param pongCall the pongCall to set
	 */
	public void setPongCall(CallDescriptor pongCall) {
		this.pongCall = pongCall;
	}


	/**
	 * @return the payloadSize
	 */
	public int getPayloadSize() {
		return payloadSize;
	}


	/**
	 * @param payloadSize the payloadSize to set
	 */
	public void setPayloadSize(int payloadSize) {
		this.payloadSize = payloadSize;
	}


	/**
	 * @return the numCalls
	 */
	public int getNumCalls() {
		return numCalls;
	}


	/**
	 * @param numCalls the numCalls to set
	 */
	public void setNumCalls(int numCalls) {
		this.numCalls = numCalls;
	}
	
}
