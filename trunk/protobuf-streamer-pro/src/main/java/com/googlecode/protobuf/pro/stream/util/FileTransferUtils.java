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
package com.googlecode.protobuf.pro.stream.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.googlecode.protobuf.pro.stream.TransferIn;
import com.googlecode.protobuf.pro.stream.TransferOut;

public class FileTransferUtils {

	private static Log log = LogFactory.getLog(FileTransferUtils.class);
	
	public static final String CONTENT_LENGTH = "contentLength";
	public static final String FILE_NAME = "filename";
	
	public static Random rnd = new Random();
	
	/**
	 * Send the File's contents to TransferOut. 
	 * 
	 * The "fileName" parameter is the filename.
	 * The "contentLength" parameter is the length of the file in bytes.
	 * 
	 * The TransferOut will be closed coming out of this method
	 * under all circumstances.
	 * 
	 * @param file file to send
	 * @param out TransferOut
	 * @throws IOException
	 */
	public static void sendFile(File sendFrom, TransferOut out, boolean lengthCheckEnabled) throws IOException {
		FileChannel in = null;
		try {
			in = (new FileInputStream(sendFrom)).getChannel();
			long count = 0;
			long size = sendFrom.length();
			if ( lengthCheckEnabled ) {
				out.addParameter(FILE_NAME, sendFrom.getName());
				out.addParameter(CONTENT_LENGTH, ""+size);
			}
			while ((count += in.transferTo(count, size - count, out)) < size)
				;
		} finally {
			if ( in != null ) {
				try {
					in.close();
				} catch ( IOException e ) {
					log.warn("Unable to close file "+ sendFrom.getName());
				}
			}
			out.close();
		}
	}
	
	/**
	 * Receives streamed bytes from TransferIn and saves to a local saveToFile.
	 * 
	 * TransferIn is closed under all circumstances on exit. This could
	 * trigger a closeNotifiction to the remove streaming server if the
	 * TransferIn is not closed already - which can happen if the local
	 * file saving fails.
	 * 
	 * Optionally checks the length of bytes received and throws IOException if
	 * length mismatch. Use in conjunction with {@link #sendFile(File, TransferOut)}
	 * 
	 * @param saveTo
	 * @param in
	 * @param transferBufferSize intermediate buffer size - use 8k by default
	 * @param lengthCheck whether to check the actual against expected size
	 * @throws IOException
	 */
	public static void saveToFile(File saveTo, TransferIn in, int transferBufferSize, boolean lengthCheck) throws IOException {
		FileChannel out = null;
		try {
			long bytesRead = 0;
			out = (new FileOutputStream(saveTo)).getChannel();
			for (ByteBuffer buffer = ByteBuffer.allocate(transferBufferSize); 
					in.read(buffer) != -1; 
					buffer.clear()) {
				buffer.flip();
				while (buffer.hasRemaining()) {
					bytesRead += out.write(buffer);
				}
			}
			if ( lengthCheck ) {
				if ( in.getParameters().get(CONTENT_LENGTH) == null ) {
					throw new IOException("No contentLength provided for " + saveTo.getName());
				}
				long sentBytes = Long.parseLong(in.getParameters().get(CONTENT_LENGTH));
				if ( bytesRead != sentBytes ) {
					throw new IOException("Missing bytes for " + saveTo.getName() + " readBytes=" + bytesRead + " sentBytes=" + sentBytes);
				}
			}
		} finally {
			if ( out != null ) {
				try {
					out.close();
				} catch ( IOException e ) {
					log.warn("Unable to close file "+ saveTo.getName());
				}
			}
			in.close();
		}
	}

	/**
	 * Atomically receive a file from TransferIn to the a File named "filename".
	 * The file is received to a temporaryFile ( filename + ".tmp" ) and if all
	 * bytes are transferred successfully ( length check ) then the temporary
	 * file is renamed to filename. If there exists a file with filename before
	 * transfer starts, this file is renamed to a ".random" suffix temporarily 
	 * before the temporary file is renamed. If the temporary file cannot be renamed, 
	 * then the ".random" file is reverted and an IOException is thrown.
	 * If the random file cannot be reverted, then a warning is logged.
	 * 
	 * If no IOException is thrown, there will be a File created on the
	 * filesystem with the filename provided.
	 *
	 * TransferIn will always be closed leaving the method.
	 * 
	 * @param filename
	 * @param in
	 * @param lengthCheck whether to check the transfer length or not
	 * 
	 * @throws IOException if transfer not correct.
	 */
	public static void atomicSaveToFile(String filename, TransferIn in, boolean overwrite, boolean lengthCheck ) throws IOException {
		if ( filename == null ) {
			throw new IllegalArgumentException("filename missing");
		}
		File existingFile = new File(filename);
		File existingSafeguardFile = null;
		if ( existingFile.exists() ) {
			if ( overwrite ) {
				do {
					String rndSuffix = "."+rnd.nextInt();
					existingSafeguardFile = new File(filename + rndSuffix);
				} while( existingSafeguardFile.exists() ) ;
			} else {
				throw new IOException("Not allowed to overwrite " + existingFile.getName());
			}
		}
		try {
			String tempFilename = filename + ".tmp";
			
			File tempFile = new File(tempFilename);
			if ( tempFile.exists() ) {
				if ( !tempFile.delete() ) {
					throw new IOException("Unable to delete temporary File " + tempFilename);
				}
			}
			saveToFile(tempFile, in, 8096, lengthCheck);

			// all bytes transferred, TransferIn closed
			if ( existingSafeguardFile != null ) {
				if ( !existingFile.renameTo(existingSafeguardFile) ) {
					throw new IOException("Unable to rename safeguard existingFile " + existingFile.getName() + " to " + existingSafeguardFile.getName() );
				}
				if ( !tempFile.renameTo(existingFile) ) {
					if ( !existingSafeguardFile.renameTo(existingFile) ) {
						log.warn("Unable to re-instate safeguard file " + existingSafeguardFile.getName() + " to " + existingFile.getName() );
					}
					throw new IOException("Unable to rename temporaryFile " + tempFile.getName() + " to " + existingFile.getName() );
				}
				if ( !existingSafeguardFile.delete() ) {
					log.warn("Unable to delete safeguard file " + existingSafeguardFile.getName());
				}
			} else {
				if ( !tempFile.renameTo(existingFile) ) {
					throw new IOException("Unable to rename temporaryFile " + tempFile.getName() + " to " + existingFile.getName() );
				}
			}
		} finally {
			if ( in.isOpen() ) {
				in.close();
			}
		}
	}
}
