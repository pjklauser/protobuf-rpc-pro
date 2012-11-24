package com.googlecode.protobuf.pro.duplex.example.wire;

import com.googlecode.protobuf.pro.duplex.server.RpcClientRegistry;

public interface ExecutableProgram {

	public void execute(RpcClientRegistry registry) throws Throwable;
	
}
