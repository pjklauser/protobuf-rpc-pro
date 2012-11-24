package com.googlecode.protobuf.pro.duplex.example.wire;

import com.googlecode.protobuf.pro.duplex.RpcClientChannel;

public interface ExecutableClient {

	public void execute(RpcClientChannel channel);
	
	public Throwable getError();
	
}
