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
package com.googlecode.protobuf.pro.stream.example.pipeline;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.googlecode.protobuf.pro.stream.PeerInfo;
import com.googlecode.protobuf.pro.stream.PushIn;
import com.googlecode.protobuf.pro.stream.TransferOut;
import com.googlecode.protobuf.pro.stream.client.StreamingTcpClientBootstrap;
import com.googlecode.protobuf.pro.stream.example.pipeline.Pipeline.Get;
import com.googlecode.protobuf.pro.stream.example.pipeline.Pipeline.Peer;
import com.googlecode.protobuf.pro.stream.example.pipeline.Pipeline.Post;
import com.googlecode.protobuf.pro.stream.server.PushHandler;

public class PipelinePushHandler implements PushHandler<Post> {

	private static Log log = LogFactory.getLog(PipelinePushHandler.class);

	private final PeerInfo serverInfo;
	private final StreamingTcpClientBootstrap<Get,Post> bootstrap;
	
	private static class PipelineState {
		PeerInfo nextServerInfo; // 
		TransferOut pushOut;
		FileChannel filechannel; // the local file being saved
	}
	
	private Map<Post,PipelineState> ioMap = new HashMap<Post,PipelineState>();
	
	public PipelinePushHandler(PeerInfo serverInfo, StreamingTcpClientBootstrap<Get,Post> bootstrap) {
		this.serverInfo = serverInfo;
		this.bootstrap = bootstrap;
	}
	
	@Override
	public Post getPrototype() {
		return Post.getDefaultInstance();
	}

	@Override
	public void init(Post message, PushIn transferIn) {
		log.info("Push init " + message);
		
		int serverIdx = getIndexInServers(message);
		
		File saveToFile = new File(message.getFilename()+"."+serverIdx);
		try {
			PipelineState state = new PipelineState();
			ioMap.put(message, state);
			
			state.filechannel = (new FileOutputStream(saveToFile)).getChannel();
			
			if ( !isLastIndexInServers(message, serverIdx)) {
				state.nextServerInfo = getNextServerInfo(message, serverIdx);
				try {
					state.pushOut = bootstrap.push(state.nextServerInfo, message);
				} catch ( IOException ioe ) {
					log.warn("Unable to connect to next server in pipeline " + state.nextServerInfo, ioe);
					try {
						transferIn.close();
					} catch ( IOException closeException ) {
						log.warn("Unable to force close transfer " + message, closeException);
					}
				}
			}
			
		} catch ( FileNotFoundException e ) {
			try {
				transferIn.close();
			} catch ( IOException ioe ) {
				log.warn("Unable to force close transfer after FileNotFound " + e, ioe);
			}
		}
	}

	@Override
	public void data(Post message, PushIn transferIn) {
		log.info("Push data " + message);
		PipelineState state = ioMap.get(message);
		if ( state != null ) {
			try {
				ByteBuffer buffer = transferIn.getCurrentChunkData();
				if ( state.pushOut != null ) { // not at end of chain
					buffer.mark();
					while (buffer.hasRemaining()) { // push data to next server in pipeline
						state.pushOut.write(buffer);
					}
					buffer.reset();
				}
				if ( state.filechannel != null ) {
					while (buffer.hasRemaining()) { // write data to local file too
						state.filechannel.write(buffer);
					}
				}
			} catch ( IOException e ) {
				log.warn("Exception processing transfer data for " + message, e);
				try {
					state.filechannel.close();
				} catch ( IOException ioe ) {
					log.warn("Unable to close local file transfer " + message, ioe);
				}
				state.filechannel = null;
				try {
					transferIn.close();
				} catch ( IOException ioe ) {
					log.warn("Unable to force close downward transfer " + message, ioe);
				}
				if ( state.pushOut != null ) {
					try {
						state.pushOut.close();
					} catch ( IOException ioe ) {
						log.warn("Unable to force close upward transfer " + message, ioe);
					}
					state.pushOut = null;
				}
			}
		} else {
			log.warn("Unable to find transfer state for " + message + " at transfer data.");
			try {
				transferIn.close();
			} catch ( IOException closeException ) {
				log.warn("Unable to force close transfer " + message, closeException);
			}
		}
	}

	@Override
	public void end(Post message, PushIn transferIn) {
		log.info("Push end " + message);
		PipelineState state = ioMap.remove(message);
		if ( state != null ) {
			if ( state.pushOut != null ) {
				// end the transfer to the next server
				try {
					state.pushOut.close();
				} catch ( IOException closeException ) {
					log.warn("Unable to normally close transfer to next server" + message, closeException);
				}
			}
			if ( state.filechannel != null ) {
				// end the write to the local file
				try {
					state.filechannel.close();	
				} catch ( IOException e ) {
					log.warn("Unable to close " + message + " at end of transfer.");
				}
			}
			state.filechannel = null;

			//TODO do check size parameter
			//TODO if not successful delete
		} else {
			log.warn("Unable to find transfer state for " + message + " at end of transfer.");
		}
	}

	private int getIndexInServers( Post message ) {
		int idx = 0;
		for( Peer peer : message.getPeerList()) {
			if ( serverInfo.getHostName().equals(peer.getHostname()) && serverInfo.getPort() == peer.getPort()) {
				return idx;
			}
			idx++;
		}
		return -1;
	}

	private boolean isLastIndexInServers( Post message, int idx ) {
		return idx >= message.getPeerCount() - 1;
	}
	
	private PeerInfo getNextServerInfo( Post message, int idx ) {
		Peer peer = message.getPeer(idx+1);
		PeerInfo nextServerInfo = new PeerInfo(peer.getHostname(), peer.getPort());
		return nextServerInfo;
	}
	
	
}
