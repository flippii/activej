/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.csp.process.compression;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.parse.ParseException;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.jetbrains.annotations.Nullable;

import static io.activej.csp.process.compression.LZ4BlockCompressor.*;
import static java.lang.Math.min;

public final class LZ4BlockDecompressor implements BlockDecompressor {
	public static final int HEADER_LENGTH = LZ4BlockCompressor.HEADER_LENGTH;
	public static final ParseException STREAM_IS_CORRUPTED = new ParseException(ChannelDecompressor.class, "Stream is corrupted");

	private final LZ4FastDecompressor decompressor;
	private final StreamingXXHash32 checksum;
	private final Header header = new Header();

	// region creators
	private LZ4BlockDecompressor(LZ4FastDecompressor decompressor, StreamingXXHash32 checksum) {
		this.decompressor = decompressor;
		this.checksum = checksum;
	}

	public static LZ4BlockDecompressor create() {
		return create(
				LZ4Factory.fastestInstance().fastDecompressor(),
				XXHashFactory.fastestInstance());
	}

	public static LZ4BlockDecompressor create(LZ4FastDecompressor decompressor, XXHashFactory xxHashFactory) {
		return new LZ4BlockDecompressor(decompressor, xxHashFactory.newStreamingHash32(DEFAULT_SEED));
	}

	@Nullable
	@Override
	public ByteBuf tryDecompress(ByteBufQueue queue) throws ParseException {
		if (!queue.hasRemainingBytes(HEADER_LENGTH)) {
			for (int i = 0; i < min(queue.remainingBytes(), MAGIC.length); i++) {
				if (queue.peekByte(i) != MAGIC[i]) {
					throw STREAM_IS_CORRUPTED;
				}
			}
			return null;
		}

		readHeader(queue);

		if (!queue.hasRemainingBytes(HEADER_LENGTH + header.compressedLen)) return null;

		queue.skip(HEADER_LENGTH);
		if (header.finished) return ByteBuf.empty();
		return decompressBody(queue);
	}

	private void readHeader(ByteBufQueue queue) throws ParseException {
		for (int i = 0; i < MAGIC_LENGTH; ++i) {
			if (queue.peekByte(i) != MAGIC[i]) {
				throw STREAM_IS_CORRUPTED;
			}
		}
		int token = queue.peekByte(MAGIC_LENGTH) & 0xFF;
		header.compressionMethod = token & 0xF0;
		int compressionLevel = COMPRESSION_LEVEL_BASE + (token & 0x0F);
		if (header.compressionMethod != COMPRESSION_METHOD_RAW && header.compressionMethod != COMPRESSION_METHOD_LZ4) {
			throw STREAM_IS_CORRUPTED;
		}
		header.compressedLen = readIntLE(queue, MAGIC_LENGTH + 1);
		header.originalLen = readIntLE(queue, MAGIC_LENGTH + 5);
		header.check = readIntLE(queue, MAGIC_LENGTH + 9);
		if (header.originalLen > 1 << compressionLevel
				|| (header.originalLen < 0 || header.compressedLen < 0)
				|| (header.originalLen == 0 && header.compressedLen != 0)
				|| (header.originalLen != 0 && header.compressedLen == 0)
				|| (header.compressionMethod == COMPRESSION_METHOD_RAW && header.originalLen != header.compressedLen)) {
			throw STREAM_IS_CORRUPTED;
		}
		if (header.originalLen == 0) {
			if (header.check != 0) {
				throw STREAM_IS_CORRUPTED;
			}
			header.finished = true;
		}
	}

	private static int readIntLE(ByteBufQueue queue, int offset) {
		return (queue.peekByte(offset) & 0xFF) |
				((queue.peekByte(offset + 1) & 0xFF) << 8) |
				((queue.peekByte(offset + 2) & 0xFF) << 16) |
				((queue.peekByte(offset + 3) & 0xFF) << 24);
	}

	private ByteBuf decompressBody(ByteBufQueue queue) throws ParseException {
		ByteBuf inputBuf = queue.takeExactSize(header.compressedLen);
		try {
			return decompress(inputBuf.array(), inputBuf.head());
		} finally {
			inputBuf.recycle();
		}
	}

	private ByteBuf decompress(byte[] bytes, int off) throws ParseException {
		ByteBuf outputBuf = ByteBufPool.allocate(header.originalLen);
		outputBuf.tail(header.originalLen);
		switch (header.compressionMethod) {
			case COMPRESSION_METHOD_RAW:
				System.arraycopy(bytes, off, outputBuf.array(), 0, header.originalLen);
				break;
			case COMPRESSION_METHOD_LZ4:
				try {
					int compressedLen2 = decompressor.decompress(bytes, off, outputBuf.array(), 0, header.originalLen);
					if (header.compressedLen != compressedLen2) {
						throw STREAM_IS_CORRUPTED;
					}
				} catch (LZ4Exception e) {
					throw new ParseException(ChannelDecompressor.class, "Stream is corrupted", e);
				}
				break;
			default:
				throw STREAM_IS_CORRUPTED;
		}
		checksum.reset();
		checksum.update(outputBuf.array(), 0, header.originalLen);
		if (checksum.getValue() != header.check) {
			throw STREAM_IS_CORRUPTED;
		}
		return outputBuf;
	}

	private static final class Header {
		public int originalLen;
		public int compressedLen;
		public int compressionMethod;
		public int check;
		public boolean finished;
	}
}
