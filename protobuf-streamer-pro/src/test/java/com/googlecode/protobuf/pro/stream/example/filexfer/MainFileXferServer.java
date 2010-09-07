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
package com.googlecode.protobuf.pro.stream.example.filexfer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.googlecode.protobuf.pro.stream.CleanShutdownHandler;
import com.googlecode.protobuf.pro.stream.PeerInfo;
import com.googlecode.protobuf.pro.stream.PushIn;
import com.googlecode.protobuf.pro.stream.TransferOut;
import com.googlecode.protobuf.pro.stream.example.filexfer.FileXfer.Get;
import com.googlecode.protobuf.pro.stream.example.filexfer.FileXfer.Post;
import com.googlecode.protobuf.pro.stream.server.PullHandler;
import com.googlecode.protobuf.pro.stream.server.PushHandler;
import com.googlecode.protobuf.pro.stream.server.StreamingServerBootstrap;
import com.googlecode.protobuf.pro.stream.util.FileTransferUtils;

public class MainFileXferServer {

	private static Log log = LogFactory.getLog(MainFileXferServer.class);

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("usage: <serverHostname> <serverPort>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		
		PeerInfo serverInfo = new PeerInfo(serverHostname, serverPort);

		PullHandler<Get> pullHandler = new PullHandler<Get>() {

			@Override
			public Get getPrototype() {
				return Get.getDefaultInstance();
			}

			@Override
			public void handlePull(Get message, TransferOut transferOut) {
				// The client wants to pull the Get.file back
				// the thread calling this is from a pool
				log.info("Pull " + message);
				
				String filename = message.getFilename();
				
				File file = new File(filename);
				try {
					FileTransferUtils.sendFile(file, transferOut, true);

					log.info("Sent " + filename);
				} catch (IOException e) {
					log.warn("Pull failed ", e);
				}
			}
		};

		PushHandler<Post> pushHandler = new PushHandler<Post>() {
			
			private Map<Post,FileChannel> ioMap = new HashMap<Post,FileChannel>();
			
			@Override
			public Post getPrototype() {
				return Post.getDefaultInstance();
			}

			@Override
			public void init(Post message, PushIn transferIn) {
				log.info("Push init " + message);
				
				File saveToFile = new File(message.getFilename());
				try {
					FileChannel out = (new FileOutputStream(saveToFile)).getChannel();
					ioMap.put(message, out);
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
				FileChannel out = ioMap.get(message);
				if ( out != null ) {
					try {
						ByteBuffer buffer = transferIn.getCurrentChunkData();
						while (buffer.hasRemaining()) {
							out.write(buffer);
						}
					} catch ( IOException e ) {
						log.warn("Exception reading from " + message + " transfer data.");
					}
				} else {
					log.warn("Unable to find FileChannel for " + message + " at transfer data.");
				}
			}

			@Override
			public void end(Post message, PushIn transferIn) {
				log.info("Push end " + message);
				FileChannel out = ioMap.get(message);
				if ( out != null ) {
					try {
						out.close();
					} catch ( IOException e ) {
						log.warn("Unable to close " + message + " at end of transfer.");
					}
					//TODO do check size parameter
					
					//TODO if not successful delete
				} else {
					log.warn("Unable to find FileChannel for " + message + " at end of transfer.");
				}
			}

		};

		// Configure the server.
		StreamingServerBootstrap<Get, Post> bootstrap = new StreamingServerBootstrap<Get, Post>(
				serverInfo, pullHandler, pushHandler,
				new NioServerSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		// give the bootstrap to the shutdown handler so it is shutdown cleanly.
		CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
		shutdownHandler.addResource(bootstrap);

		// Bind and start to accept incoming connections.
		bootstrap.bind();

		log.info("Handling " + serverInfo);
	}

}
