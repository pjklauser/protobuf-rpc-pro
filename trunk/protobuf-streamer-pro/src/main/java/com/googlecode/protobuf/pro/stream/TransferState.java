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
package com.googlecode.protobuf.pro.stream;

import com.google.protobuf.Message;

/**
 * @author Peter Klauser
 * 
 */
public class TransferState<E extends Message, F extends Message> {

	private final long startTimestamp;
	private final E pullRequest;
	private final F pushRequest;
	private final TransferIn pullStream;
	private final TransferOut pushStream;

	public TransferState(long startTimestamp, E pullRequest, F pushRequest, TransferIn pullStream, TransferOut pushStream) {
		this.startTimestamp = startTimestamp;
		this.pullRequest = pullRequest;
		this.pushRequest = pushRequest;
		this.pullStream = pullStream;
		this.pushStream = pushStream;
	}

	/**
	 * @return the startTimestamp
	 */
	public long getStartTimestamp() {
		return startTimestamp;
	}

	/**
	 * @return the request
	 */
	public E getPullRequest() {
		return pullRequest;
	}

	/**
	 * @return the request
	 */
	public F getPushRequest() {
		return pushRequest;
	}

	public int getCorrelationId() {
		if (pullStream != null) {
			return pullStream.getCorrelationId();
		} else if (pushStream != null) {
			return pushStream.getCorrelationId();
		}
		throw new IllegalStateException("missing transfer");
	}

	/**
	 * @return the pullStream
	 */
	public TransferIn getPullStream() {
		return pullStream;
	}

	/**
	 * @return the pushStream
	 */
	public TransferOut getPushStream() {
		return pushStream;
	}
}
