package com.googlecode.protobuf.pro.duplex.example.wire;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.googlecode.protobuf.pro.duplex.RpcClientChannel;

public class ClientExecutor {

	private static Log log = LogFactory.getLog(ClientExecutor.class);

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
