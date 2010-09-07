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

import com.googlecode.protobuf.pro.stream.PushIn;
import com.googlecode.protobuf.pro.stream.example.pipeline.Pipeline.Post;
import com.googlecode.protobuf.pro.stream.server.PushHandler;

public class PipelinePushHandler implements PushHandler<Post> {

	private static Log log = LogFactory.getLog(PipelinePushHandler.class);

	
	private Map<Post,FileChannel> ioMap = new HashMap<Post,FileChannel>();
	
	public PipelinePushHandler() {
		
	}
	
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
}
