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
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.googlecode.protobuf.pro.stream.CleanShutdownHandler;
import com.googlecode.protobuf.pro.stream.PeerInfo;
import com.googlecode.protobuf.pro.stream.TransferIn;
import com.googlecode.protobuf.pro.stream.TransferOut;
import com.googlecode.protobuf.pro.stream.client.StreamingTcpClientBootstrap;
import com.googlecode.protobuf.pro.stream.example.filexfer.FileXfer.Get;
import com.googlecode.protobuf.pro.stream.example.filexfer.FileXfer.Post;
import com.googlecode.protobuf.pro.stream.util.FileTransferUtils;

public class SingleFileXferClient {

	private static Log log = LogFactory.getLog(SingleFileXferClient.class);

	private static Random rnd = new Random();
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("usage: <filename> <serverHostname> <serverPort>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		String filename = args[2];

		File file = new File(filename);
		if ( !file.exists() ) {
			System.err.println("File " + filename + " not found.");
			System.exit(-1);
		}
		if ( !file.isFile() ) {
			System.err.println(filename + " is not a file.");
			System.exit(-1);
		}
		
		PeerInfo server = new PeerInfo(serverHostname, serverPort);

		CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
		try {
			StreamingTcpClientBootstrap<Get,Post> bootstrap = new StreamingTcpClientBootstrap<Get,Post>(
					new PeerInfo("<undefined>", 0), new NioClientSocketChannelFactory(
							Executors.newCachedThreadPool(),
							Executors.newCachedThreadPool())
					);

			// give the bootstrap to the shutdown handler so it is shutdown cleanly.
			shutdownHandler.addResource(bootstrap);
			
			for ( int i = 0; i < 10; i++ ) {
				String filenameAtServer = filename + "." + rnd.nextInt();
				Post post = Post.newBuilder().setFilename(filenameAtServer).build();
				TransferOut out = bootstrap.push(server, post);
				FileTransferUtils.sendFile(file, out, true);
				log.info("Sent " + file + " to server as " + filenameAtServer);

				Get get = Get.newBuilder().setFilename(filenameAtServer).build();
				String localFilename = filenameAtServer +".local";
				TransferIn in = bootstrap.pull(server, get);
				FileTransferUtils.atomicSaveToFile(localFilename, in, true, true);
				log.info("Received " + filename + " from server as " + localFilename);
			}
						
			{
				String filenameAtServer = filename + ".cutoff." + rnd.nextInt();
				Post closedPost = Post.newBuilder().setFilename(filenameAtServer).build();
				TransferOut out = bootstrap.push(server, closedPost);
				out.write(ByteBuffer.wrap(new byte[100000]));
				bootstrap.releaseExternalResources();
				log.info("Sent " + file + " to server as " + filenameAtServer + " was closed half way...");
			}
			
		} catch ( Exception e ) {
			System.err.println("Exception " + e );
		} finally {
			System.exit(0);
		}

	}

}
