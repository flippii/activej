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
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.jetbrains.annotations.NotNull;

import static io.activej.common.Checks.checkArgument;
import static java.lang.Math.max;

public final class LZ4BlockCompressor implements BlockCompressor {
	static final byte[] MAGIC = {'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k'};
	static final int MAGIC_LENGTH = MAGIC.length;

	public static final int HEADER_LENGTH =
			MAGIC_LENGTH    // magic bytes
					+ 1     // token
					+ 4     // compressed length
					+ 4     // decompressed length
					+ 4;    // checksum

	static final int COMPRESSION_LEVEL_BASE = 10;

	static final int COMPRESSION_METHOD_RAW = 0x10;
	static final int COMPRESSION_METHOD_LZ4 = 0x20;

	static final int DEFAULT_SEED = 0x9747b28c;

	private static final int MIN_BLOCK_SIZE = 64;

	private final LZ4Compressor compressor;
	private final StreamingXXHash32 checksum = XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED);

	private boolean customEndOfStream;

	private LZ4BlockCompressor(LZ4Compressor compressor) {
		this.compressor = compressor;
	}

	public static LZ4BlockCompressor create(LZ4Compressor compressor) {
		return new LZ4BlockCompressor(compressor);
	}

	public static LZ4BlockCompressor create(int compressionLevel) {
		return compressionLevel == 0 ? createFastCompressor() : createHighCompressor(compressionLevel);
	}

	public static LZ4BlockCompressor createFastCompressor() {
		return new LZ4BlockCompressor(LZ4Factory.fastestInstance().fastCompressor());
	}

	public static LZ4BlockCompressor createHighCompressor() {
		return new LZ4BlockCompressor(LZ4Factory.fastestInstance().highCompressor());
	}

	public static LZ4BlockCompressor createHighCompressor(int compressionLevel) {
		return new LZ4BlockCompressor(LZ4Factory.fastestInstance().highCompressor(compressionLevel));
	}

	public LZ4BlockCompressor withCustomEndOfStreamBlock(boolean customEndOfStream) {
		this.customEndOfStream = customEndOfStream;
		return this;
	}

	@Override
	public ByteBuf compress(ByteBuf inputBuf) {
		checkArgument(inputBuf.readRemaining() > 0);
		return doCompress(inputBuf);
	}

	@Override
	public ByteBuf getEndOfStreamBlock() {
		if (customEndOfStream) {
			return doCompress(ByteBuf.empty());
		}

		int compressionLevel = compressionLevel(MIN_BLOCK_SIZE);

		ByteBuf outputBuf = ByteBufPool.allocate(HEADER_LENGTH);
		byte[] outputBytes = outputBuf.array();
		System.arraycopy(MAGIC, 0, outputBytes, 0, MAGIC_LENGTH);

		outputBytes[MAGIC_LENGTH] = (byte) (COMPRESSION_METHOD_RAW | compressionLevel);
		writeIntLE(0, outputBytes, MAGIC_LENGTH + 1);
		writeIntLE(0, outputBytes, MAGIC_LENGTH + 5);
		writeIntLE(0, outputBytes, MAGIC_LENGTH + 9);

		outputBuf.tail(HEADER_LENGTH);
		return outputBuf;
	}

	@NotNull
	private ByteBuf doCompress(ByteBuf inputBuf) {
		int len = inputBuf.readRemaining();
		int off = inputBuf.head();
		byte[] bytes = inputBuf.array();

		int compressionLevel = compressionLevel(max(len, MIN_BLOCK_SIZE));

		int outputBufMaxSize = HEADER_LENGTH + ((compressor == null) ? len : compressor.maxCompressedLength(len));
		ByteBuf outputBuf = ByteBufPool.allocate(outputBufMaxSize);
		outputBuf.put(MAGIC);

		byte[] outputBytes = outputBuf.array();

		checksum.reset();
		checksum.update(bytes, off, len);
		int check = checksum.getValue();

		int compressedLength = len;
		if (compressor != null) {
			compressedLength = compressor.compress(bytes, off, len, outputBytes, HEADER_LENGTH);
		}

		int compressMethod;
		if (compressor == null || compressedLength >= len) {
			compressMethod = COMPRESSION_METHOD_RAW;
			compressedLength = len;
			System.arraycopy(bytes, off, outputBytes, HEADER_LENGTH, len);
		} else {
			compressMethod = COMPRESSION_METHOD_LZ4;
		}

		outputBytes[MAGIC_LENGTH] = (byte) (compressMethod | compressionLevel);
		writeIntLE(compressedLength, outputBytes, MAGIC_LENGTH + 1);
		writeIntLE(len, outputBytes, MAGIC_LENGTH + 5);
		writeIntLE(check, outputBytes, MAGIC_LENGTH + 9);

		outputBuf.tail(HEADER_LENGTH + compressedLength);

		return outputBuf;
	}

	private static int compressionLevel(int blockSize) {
		int compressionLevel = 32 - Integer.numberOfLeadingZeros(blockSize - 1); // ceil of log2
		checkArgument((1 << compressionLevel) >= blockSize);
		checkArgument(blockSize * 2 > (1 << compressionLevel));
		compressionLevel = max(0, compressionLevel - COMPRESSION_LEVEL_BASE);
		checkArgument(compressionLevel <= 0x0F);
		return compressionLevel;
	}

	private static void writeIntLE(int i, byte[] buf, int off) {
		buf[off++] = (byte) i;
		buf[off++] = (byte) (i >>> 8);
		buf[off++] = (byte) (i >>> 16);
		buf[off] = (byte) (i >>> 24);
	}
}
