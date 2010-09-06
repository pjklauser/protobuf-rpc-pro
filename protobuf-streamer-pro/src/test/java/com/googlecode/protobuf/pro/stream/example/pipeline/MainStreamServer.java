package com.googlecode.protobuf.pro.stream.example.pipeline;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.googlecode.protobuf.pro.stream.CleanShutdownHandler;
import com.googlecode.protobuf.pro.stream.PeerInfo;
import com.googlecode.protobuf.pro.stream.TransferIn;
import com.googlecode.protobuf.pro.stream.TransferOut;
import com.googlecode.protobuf.pro.stream.example.pipeline.Pipeline.Get;
import com.googlecode.protobuf.pro.stream.example.pipeline.Pipeline.Post;
import com.googlecode.protobuf.pro.stream.server.PullHandler;
import com.googlecode.protobuf.pro.stream.server.PushHandler;
import com.googlecode.protobuf.pro.stream.server.StreamingServerBootstrap;
import com.googlecode.protobuf.pro.stream.util.FileTransferUtils;

public class MainStreamServer {

	private static Log log = LogFactory.getLog(MainStreamServer.class);

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err
					.println("usage: <serverHostname> <serverPort> <master|slave>");
			System.exit(-1);
		}
		String serverHostname = args[0];
		int serverPort = Integer.parseInt(args[1]);
		boolean master = "master".equalsIgnoreCase(args[2]);

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

			@Override
			public Post getPrototype() {
				return Post.getDefaultInstance();
			}

			@Override
			public void init(Post message, TransferIn transferIn) {
				log.info("Push init " + message);
			}

			@Override
			public void data(Post message, TransferIn transferIn) {
				log.info("Push data " + message);
			}

			@Override
			public void end(Post message, TransferIn transferIn) {
				log.info("Push end " + message);
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
