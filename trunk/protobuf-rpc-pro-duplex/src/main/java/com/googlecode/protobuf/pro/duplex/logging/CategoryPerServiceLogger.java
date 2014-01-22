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
package com.googlecode.protobuf.pro.duplex.logging;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.logging.RpcLogEntry.OobMessage;
import com.googlecode.protobuf.pro.duplex.logging.RpcLogEntry.OobResponse;
import com.googlecode.protobuf.pro.duplex.logging.RpcLogEntry.PayloadContent;
import com.googlecode.protobuf.pro.duplex.logging.RpcLogEntry.RpcCall;
import com.googlecode.protobuf.pro.duplex.logging.RpcLogEntry.RpcPayloadInfo;

/**
 * @author Peter Klauser
 * 
 */
public class CategoryPerServiceLogger implements RpcLogger {

	private boolean logRequestProto = true;
	private boolean logResponseProto = true;
	private boolean logEventProto = true;

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
		int duration = (int)(responseTS - requestTS);
		String reqUUID = null;
		String resUUID = null;
		
		String summaryCategoryName = signature + ".info";
	 Logger log = LoggerFactory.getLogger(summaryCategoryName);
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
				reqUUID = UUID.randomUUID().toString();
				RpcPayloadInfo reqInfo = RpcPayloadInfo.newBuilder().setUuid(reqUUID).setTs(requestTS).setSize(request.getSerializedSize()).build();
				rpcCall.setRequest(reqInfo);
			}
			if (response != null) {
				resUUID = UUID.randomUUID().toString();
				RpcPayloadInfo resInfo  = RpcPayloadInfo.newBuilder().setUuid(resUUID).setTs(responseTS).setSize(response.getSerializedSize()).build();
				rpcCall.setResponse(resInfo);
			}

			summaryText = TextFormat.shortDebugString(rpcCall.build());
		}

		String requestCategoryName = signature + ".data.request";
	 Logger reqlog = LoggerFactory.getLogger(requestCategoryName);
		String requestText = null;
		if (isLogRequestProto() && request != null) {
			if (reqlog.isInfoEnabled()) {
				PayloadContent pc = PayloadContent.newBuilder().setUuid(reqUUID).setContent(TextFormat.shortDebugString(request)).build();
				requestText = TextFormat.shortDebugString(pc);
			}
		}

		String responseCategoryName = signature + ".data.response";
	 Logger reslog = LoggerFactory.getLogger(responseCategoryName);
		String responseText = null;
		if (isLogResponseProto() && response != null) {
			if (reslog.isInfoEnabled()) {
				PayloadContent pc = PayloadContent.newBuilder().setUuid(reqUUID).setContent(TextFormat.shortDebugString(response)).build();
				responseText = TextFormat.shortDebugString(pc);
			}
		}

		if (summaryText != null) {
			log.info(summaryText);
		}
		if (requestText != null) {
			reqlog.info(requestText);
		}
		if (responseText != null) {
			reslog.info(responseText);
		}
	}

	@Override
	public void logOobResponse(PeerInfo client, PeerInfo server,
			Message message, String signature, int correlationId, long eventTS) {
		String summaryCategoryName = signature + ".info.oob";
		String uuid = UUID.randomUUID().toString();
		
	 Logger log = LoggerFactory.getLogger(summaryCategoryName);
		String summaryText = null;
		if (log.isInfoEnabled()) {
			OobResponse.Builder rpcCall = OobResponse.newBuilder()
					.setCorId(correlationId)
					.setClient(client.toString())
					.setServer(server.toString())
					.setSignature(signature);
			if (message != null) {
				RpcPayloadInfo evInfo = RpcPayloadInfo.newBuilder().setUuid(uuid).setTs(eventTS).setSize(message.getSerializedSize()).build();
				rpcCall.setEvent(evInfo);
			}
			summaryText = TextFormat.shortDebugString(rpcCall.build());
		}

		String responseCategoryName = signature + ".data.response.oob";
	 Logger reslog = LoggerFactory.getLogger(responseCategoryName);
		String responseText = null;
		if (isLogResponseProto() && message != null) {
			if (reslog.isInfoEnabled()) {
				PayloadContent pc = PayloadContent.newBuilder().setUuid(uuid).setContent(TextFormat.shortDebugString(message)).build();
				responseText = TextFormat.shortDebugString(pc);
			}
		}

		if (summaryText != null) {
			log.info(summaryText);
		}
		if (responseText != null) {
			reslog.info(responseText);
		}
	}

	@Override
	public void logOobMessage(PeerInfo client, PeerInfo server,
			Message message, long eventTS) {
		String summaryCategoryName = "oobmessage.info";
		String uuid = UUID.randomUUID().toString();
	 Logger log = LoggerFactory.getLogger(summaryCategoryName);
		String summaryText = null;
		if (log.isInfoEnabled()) {
			OobMessage.Builder oobMessage = OobMessage.newBuilder()
					.setClient(client.toString())
					.setServer(server.toString());
			if (message != null) {
				RpcPayloadInfo evInfo = RpcPayloadInfo.newBuilder().setUuid(uuid).setTs(eventTS).setSize(message.getSerializedSize()).build();
				oobMessage.setEvent(evInfo);
			}
			summaryText = TextFormat.shortDebugString(oobMessage.build());
		}

		String eventCategoryName = "oobmessage.data";
	 Logger evlog = LoggerFactory.getLogger(eventCategoryName);
		String eventText = null;
		if (isLogEventProto() && message != null) {
			if (evlog.isInfoEnabled()) {
				PayloadContent pc = PayloadContent.newBuilder().setUuid(uuid).setContent(TextFormat.shortDebugString(message)).build();
				eventText = TextFormat.printToString(pc);
			}
		}

		if (summaryText != null) {
			log.info(summaryText);
		}
		if (eventText != null) {
			evlog.info(eventText);
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

	/**
	 * @return the logEventProto
	 */
	public boolean isLogEventProto() {
		return logEventProto;
	}

	/**
	 * @param logEventProto the logEventProto to set
	 */
	public void setLogEventProto(boolean logEventProto) {
		this.logEventProto = logEventProto;
	}


}
