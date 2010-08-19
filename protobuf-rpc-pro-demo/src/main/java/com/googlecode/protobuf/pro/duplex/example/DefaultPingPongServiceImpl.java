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

import java.util.LinkedList;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Ping;
import com.googlecode.protobuf.pro.duplex.example.PingPong.PingPongService;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Pong;

/**
 * PingPongService
 * 
 * The ping method will return as much data as requested the Ping's pongDataLength
 * field. This can be used to simulate large or small server responses.
 * 
 * The ping method will process ( using CPU or sleep ) for the time indicated in
 * the Ping's processingTime field. This can be used to simulate dynamic situations
 * where the server can "lag" and requests can come in faster than can be processed.
 * 
 * During the CPU server processing, objects are produced on the heap, to
 * produce garbage which needs collection. The processing will produce up to a maximum
 * configured number of objects ( @see #maxScrapPerCall )
 * 
 * @author Peter Klauser
 *
 */
public class DefaultPingPongServiceImpl extends PingPongService {
	
	private boolean sleepForProcessing = true;
	private int maxScrapPerCall = 1000000; // 1M Integers
	
	
	public DefaultPingPongServiceImpl() {
	}
	
	@Override
	public void ping(RpcController controller, Ping request,
			RpcCallback<Pong> done) {
		
		byte[] requestData = request.getPingData().toByteArray();
		
		doProcessing(controller, request.getProcessingTime(),requestData);
		
		byte[] responseData = new byte[request.getPongDataLength()];
		Pong response = Pong.newBuilder().setPongData(ByteString.copyFrom(responseData)).build();
		done.run(response);
		
	}

	@Override
	public void fail(RpcController controller, Ping request,
			RpcCallback<Pong> done) {

		byte[] requestData = request.getPingData().toByteArray();
		
		doProcessing(controller, request.getProcessingTime(),requestData);
		
		controller.setFailed("Failed.");
		done.run(null);
	}
	
	int doProcessing( RpcController controller, int processingTime, byte[] requestData ) {
		// Processing aid
		int[] processingData = new int[1024];
		for( int i = 0; i < processingData.length; i++ ) {
			processingData[i] = requestData[i%requestData.length];
		}
		LinkedList<Integer> processingScrap = new LinkedList<Integer>();
		processingScrap.add(101);
		long processUntil = System.currentTimeMillis() + processingTime;
		do {
			if ( sleepForProcessing ) {
				long sleepDuration = processUntil - System.currentTimeMillis();
				if ( sleepDuration > 0 ) {
					try {
						Thread.sleep(sleepDuration);
					} catch ( InterruptedException e ) {
						// RpcCancel interupts Thread.sleep and NIO waits.
						
						Thread.currentThread().interrupt(); // propagate just in case we have later IO operations
						// this will be cleared after the RPC call returns.
						if ( controller.isCanceled() ) {
							// just get out
							return 1;
						}
					}
				}
			} else {
				// load the CPU
				for( int i = 1; i < processingData.length; i++) {
					processingData[i] = processingData[i] + processingData[i-1];
				}
				if ( processingScrap.size() < getMaxScrapPerCall() ) {
					processingScrap.add(processingData[processingData.length-1]);
				}
			}
			
		} while( System.currentTimeMillis() < processUntil );
		return processingScrap.getLast();
	}

	/**
	 * @return the sleepForProcessing
	 */
	public boolean isSleepForProcessing() {
		return sleepForProcessing;
	}

	/**
	 * @param sleepForProcessing the sleepForProcessing to set
	 */
	public void setSleepForProcessing(boolean sleepForProcessing) {
		this.sleepForProcessing = sleepForProcessing;
	}

	/**
	 * @return the maxScrapPerCall
	 */
	public int getMaxScrapPerCall() {
		return maxScrapPerCall;
	}

	/**
	 * @param maxScrapPerCall the maxScrapPerCall to set
	 */
	public void setMaxScrapPerCall(int maxScrapPerCall) {
		this.maxScrapPerCall = maxScrapPerCall;
	}
}
