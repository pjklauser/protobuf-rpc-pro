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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.protobuf.pro.duplex.RpcClientChannel;

public class ClientExecutor {

	private static Logger log = LoggerFactory.getLogger(ClientExecutor.class);

	public ClientExecutor() {
	}

	public void execute(ExecutableClient client, RpcClientChannel channel) throws Throwable {
		execute(new ExecutableClient[]{client}, channel);
	}
	
	public void execute(ExecutableClient[] clients, RpcClientChannel channel) throws Throwable {
		PingClientThread[] threads = new PingClientThread[clients.length];
		for( int i = 0; i < threads.length; i++ ) {
			PingClientThread c = new PingClientThread(clients[i], channel);
			c.start();
			threads[i] = c;
		}
		
		for( int i = 0; i < threads.length; i++ ) {
			threads[i].join();
		}
		
		for( int i = 0; i < threads.length; i++ ) {
			if (threads[i].getError() !=null) {
				throw threads[i].getError();
			}
		}
	}


	private static class PingClientThread extends Thread {

		ExecutableClient client;
		RpcClientChannel channel;
		
		public PingClientThread( ExecutableClient client, RpcClientChannel channel ) {
			this.client = client;
			this.channel = channel;
		}
		
		@Override
		public void run() {
			client.execute(channel);
		}
		
		public Throwable getError() {
			return this.client.getError();
		}
	}
}
