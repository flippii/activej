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
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.inspector.BaseInspector;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelOutput;
import io.activej.csp.binary.BinaryChannelInput;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.dsl.WithBinaryChannelInput;
import io.activej.csp.dsl.WithChannelTransformer;
import io.activej.csp.process.AbstractCommunicatingProcess;
import org.jetbrains.annotations.Nullable;

public final class ChannelDecompressor extends AbstractCommunicatingProcess
		implements WithChannelTransformer<ChannelDecompressor, ByteBuf, ByteBuf>, WithBinaryChannelInput<ChannelDecompressor> {
	private final LZ4BlockDecompressor decompressor;

	private ByteBufQueue bufs;
	private BinaryChannelSupplier input;
	private ChannelConsumer<ByteBuf> output;

	@Nullable
	private Inspector inspector;

	private ChannelDecompressor(LZ4BlockDecompressor decompressor) {
		this.decompressor = decompressor;
	}

	public static ChannelDecompressor create(LZ4BlockDecompressor decompressor) {
		return new ChannelDecompressor(decompressor);
	}

	public ChannelDecompressor withInspector(Inspector inspector) {
		this.inspector = inspector;
		return this;
	}

	@Override
	public BinaryChannelInput getInput() {
		return input -> {
			this.input = sanitize(input);
			this.bufs = input.getBufs();
			if (this.input != null && this.output != null) startProcess();
			return getProcessCompletion();
		};
	}

	@SuppressWarnings("ConstantConditions") //check output for clarity
	@Override
	public ChannelOutput<ByteBuf> getOutput() {
		return output -> {
			this.output = sanitize(output);
			if (this.input != null && this.output != null) startProcess();
		};
	}

	public interface Inspector extends BaseInspector<Inspector> {
		void onBlock(int blockLength, ByteBuf outputBuf);

		void onEndOfStreamBlock(int blockLength);
	}
	// endregion

	@Override
	protected void doProcess() {
		input.parse(
				queue -> {
					int queueLengthBefore = bufs.remainingBytes();
					ByteBuf buf = decompressor.tryDecompress(queue);
					if (inspector != null && buf != null) {
						if (!buf.canRead()) {
							inspector.onEndOfStreamBlock(queueLengthBefore - bufs.remainingBytes());
						} else {
							inspector.onBlock(queueLengthBefore - bufs.remainingBytes(), buf);
						}
					}
					return buf;
				})
				.whenResult(buf -> {
							if (buf.canRead()) {
								output.accept(buf)
										.whenResult(this::doProcess);
							} else {
								buf.recycle();
								input.endOfStream()
										.then(() -> output.acceptEndOfStream())
										.whenResult(this::completeProcess);
							}
						}
				);
	}

	@Override
	protected void doClose(Throwable e) {
		input.closeEx(e);
		output.closeEx(e);
	}
}
