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
package com.googlecode.protobuf.pro.stream.logging;

import java.util.Map;

import com.google.protobuf.Message;
import com.googlecode.protobuf.pro.stream.PeerInfo;

/**
 * @author Peter Klauser
 * 
 */
public class CategoryPerMessageTypeLogger implements StreamLogger {

	private boolean logMessageProto = true;

	/* (non-Javadoc)
	 * @see com.googlecode.protobuf.pro.stream.logging.StreamLogger#logTransfer(com.googlecode.protobuf.pro.stream.PeerInfo, com.googlecode.protobuf.pro.stream.PeerInfo, com.google.protobuf.Message, java.lang.String, int, java.util.Map, long, long)
	 */
	@Override
	public void logTransfer(PeerInfo client, PeerInfo server, Message request,
			String errorMessage, int correlationId,
			Map<String, String> parameters, long startTS, long endTS) {

		int duration = (int)(endTS - startTS);
		/*
		RpcCall.Builder rpcCall = RpcCall.newBuilder()
				.setCorId(correlationId)
				.setDuration(duration)
				.setClient(client.toString())
				.setServer(server.toString())
				.setSignature(signature);
		if (errorMessage != null) {
			rpcCall.setError(errorMessage);
		}
		if (reqInfo != null) {
			rpcCall.setRequest(reqInfo);
		}
		if (resInfo != null) {
			rpcCall.setResponse(resInfo);
		}

		String summaryCategoryName = signature + ".info";
		Log log = LogFactory.getLog(summaryCategoryName);
		String summaryText = null;
		if (log.isInfoEnabled()) {
			summaryText = TextFormat.printToString(rpcCall.build());
		}

		String requestCategoryName = signature + ".data.request";
		Log reqlog = LogFactory.getLog(requestCategoryName);
		String requestText = null;
		if (isLogRequestProto() && request != null) {
			if (reqlog.isInfoEnabled()) {
				requestText = TextFormat.printToString(request);
			}
		}

		String responseCategoryName = signature + ".data.response";
		Log reslog = LogFactory.getLog(responseCategoryName);
		String responseText = null;
		if (isLogResponseProto() && response != null) {
			if (reslog.isInfoEnabled()) {
				responseText = TextFormat.printToString(response);
			}
		}

		if (summaryText != null) {
			synchronized (log) {
				log.info(summaryText);
			}
		}
		if (requestText != null) {
			synchronized (reqlog) {
				reqlog.info(requestText);
			}
		}
		if (responseText != null) {
			synchronized (reslog) {
				reslog.info(responseText);
			}
		}
		*/
	}


}
