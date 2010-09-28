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
package com.googlecode.protobuf.pro.duplex.logging;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.logging.RpcLogEntry.RpcCall;
import com.googlecode.protobuf.pro.duplex.logging.RpcLogEntry.RpcPayloadInfo;

/**
 * @author Peter Klauser
 * 
 */
public class CategoryPerServiceLogger implements RpcLogger {

	private boolean logRequestProto = true;
	private boolean logResponseProto = true;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.googlecode.protobuf.pro.duplex.logging.RpcLogger#logCall(com.googlecode
	 * .protobuf.pro.duplex.RpcClient,
	 * com.google.protobuf.Descriptors.MethodDescriptor,
	 * com.google.protobuf.Message, com.google.protobuf.Message, int, long, long)
	 */
	@Override
	public void logCall(PeerInfo client, PeerInfo server, String signature,
			Message request, Message response, String errorMessage,
			int correlationId, long requestTS, long responseTS) {
		int duration = (int)(requestTS - responseTS);
		
		String summaryCategoryName = signature + ".info";
		Log log = LogFactory.getLog(summaryCategoryName);
		String summaryText = null;
		if (log.isInfoEnabled()) {
			RpcCall.Builder rpcCall = RpcCall.newBuilder()
					.setCorId(correlationId)
					.setDuration(duration)
					.setClient(client.toString())
					.setServer(server.toString())
					.setSignature(signature);
			if (errorMessage != null) {
				rpcCall.setError(errorMessage);
			}
			if (request != null) {
				RpcPayloadInfo reqInfo = RpcPayloadInfo.newBuilder().setTs(requestTS).setSize(request.getSerializedSize()).build();
				rpcCall.setRequest(reqInfo);
			}
			if (response != null) {
				RpcPayloadInfo resInfo  = RpcPayloadInfo.newBuilder().setTs(responseTS).setSize(response.getSerializedSize()).build();
				rpcCall.setResponse(resInfo);
			}

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
	}

	/**
	 * @return the logRequestProto
	 */
	public boolean isLogRequestProto() {
		return logRequestProto;
	}

	/**
	 * @param logRequestProto
	 *            the logRequestProto to set
	 */
	public void setLogRequestProto(boolean logRequestProto) {
		this.logRequestProto = logRequestProto;
	}

	/**
	 * @return the logResponseProto
	 */
	public boolean isLogResponseProto() {
		return logResponseProto;
	}

	/**
	 * @param logResponseProto
	 *            the logResponseProto to set
	 */
	public void setLogResponseProto(boolean logResponseProto) {
		this.logResponseProto = logResponseProto;
	}

}
