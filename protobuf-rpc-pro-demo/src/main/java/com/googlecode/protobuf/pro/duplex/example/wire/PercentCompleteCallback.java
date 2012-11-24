package com.googlecode.protobuf.pro.duplex.example.wire;

import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.pro.duplex.LocalCallVariableHolder;
import com.googlecode.protobuf.pro.duplex.example.wire.PingPong.PercentComplete;

public class PercentCompleteCallback implements RpcCallback<PercentComplete> {

	public static final String KEY = "%";
	
	private LocalCallVariableHolder variableHolder;
	
	public PercentCompleteCallback(LocalCallVariableHolder variableHolder) {
		this.variableHolder = variableHolder;
	}
	
	/* (non-Javadoc)
	 * @see com.google.protobuf.RpcCallback#run(java.lang.Object)
	 */
	@Override
	public void run(PercentComplete responseMessage) {
		variableHolder.storeCallLocalVariable(KEY,responseMessage);
	}

	public PercentComplete getPercentComplete() {
		return (PercentComplete)variableHolder.getCallLocalVariable(KEY);
	}
}
