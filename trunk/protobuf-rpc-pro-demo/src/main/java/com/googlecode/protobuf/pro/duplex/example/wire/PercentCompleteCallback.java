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
