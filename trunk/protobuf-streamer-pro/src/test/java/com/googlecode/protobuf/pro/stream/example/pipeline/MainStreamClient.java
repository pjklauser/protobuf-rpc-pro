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
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.googlecode.protobuf.pro.stream.CleanShutdownHandler;
import com.googlecode.protobuf.pro.stream.PeerInfo;
import com.googlecode.protobuf.pro.stream.TransferOut;
import com.googlecode.protobuf.pro.stream.client.StreamingTcpClientBootstrap;
import com.googlecode.protobuf.pro.stream.example.pipeline.Pipeline.Get;
import com.googlecode.protobuf.pro.stream.example.pipeline.Pipeline.Peer;
import com.googlecode.protobuf.pro.stream.example.pipeline.Pipeline.Post;
import com.googlecode.protobuf.pro.stream.util.FileTransferUtils;

public class MainStreamClient {

	private static Log log = LogFactory.getLog(MainStreamClient.class);

	public static void main(String[] args) throws Exception {
		if (args.length != 7) {
			System.err.println("usage: <filename> <masterHostname> <masterPort> <slaveHostname1> <slavePort1> <slaveHostname2> <slavePort2>");
			System.exit(-1);
		}
		String filename = args[0];
		String masterHostname = args[1];
		int masterPort = Integer.parseInt(args[2]);
		String slaveHostname1 = args[3];
		int slavePort1 = Integer.parseInt(args[4]);
		String slaveHostname2 = args[5];
		int slavePort2 = Integer.parseInt(args[6]);

		File file = new File(filename);
		if ( !file.exists() ) {
			System.err.println("File " + filename + " not found.");
			System.exit(-1);
		}
		if ( !file.isFile() ) {
			System.err.println(filename + " is not a file.");
			System.exit(-1);
		}
		
		PeerInfo master = new PeerInfo(masterHostname, masterPort);
		PeerInfo slave1 = new PeerInfo(slaveHostname1, slavePort1);
		PeerInfo slave2 = new PeerInfo(slaveHostname2, slavePort2);

		CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
		try {
			StreamingTcpClientBootstrap<Get,Post> bootstrap = new StreamingTcpClientBootstrap<Get,Post>(
					new PeerInfo("<undefined>", 0), new NioClientSocketChannelFactory(
							Executors.newCachedThreadPool(),
							Executors.newCachedThreadPool())
					);

			// give the bootstrap to the shutdown handler so it is shutdown cleanly.
			shutdownHandler.addResource(bootstrap);

			Peer s0 = Peer.newBuilder().setHostname(master.getHostName()).setPort(master.getPort()).build();
			Peer s1 = Peer.newBuilder().setHostname(slave1.getHostName()).setPort(slave1.getPort()).build();
			Peer s2 = Peer.newBuilder().setHostname(slave2.getHostName()).setPort(slave2.getPort()).build();
			
			Post post = Post.newBuilder().addPeer(s0).addPeer(s1).addPeer(s2).setFilename(filename).build();
			
			TransferOut out = bootstrap.push(master, post);
			FileTransferUtils.sendFile(file, out, true);
			
			log.info("Sent " + file );
			
			Thread.sleep(1000);

		} catch ( Exception e ) {
			log.warn("Unexpected Exception ", e );
		} finally {
			System.exit(0);
		}
	}

}
