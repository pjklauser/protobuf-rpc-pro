package com.googlecode.protobuf.pro.duplex.example;

import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.pro.duplex.example.PingPong.Pong;

public abstract class PongRpcCallback implements RpcCallback<Pong> {

	private int pos;
	
	public PongRpcCallback(int pos) {
		this.pos = pos;
	}
	
	/* (non-Javadoc)
	 * @see com.google.protobuf.RpcCallback#run(java.lang.Object)
	 */
	@Override
	public abstract void run(Pong responseMessage);

	public int getPos() {
		return pos;
	}
}
