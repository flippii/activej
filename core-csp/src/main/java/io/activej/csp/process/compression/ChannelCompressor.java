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
import io.activej.common.inspector.AbstractInspector;
import io.activej.common.inspector.BaseInspector;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelInput;
import io.activej.csp.ChannelOutput;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.WithChannelTransformer;
import io.activej.csp.process.AbstractCommunicatingProcess;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.ValueStats;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;

public final class ChannelCompressor extends AbstractCommunicatingProcess
		implements WithChannelTransformer<ChannelCompressor, ByteBuf, ByteBuf> {
	private final BlockCompressor compressor;

	private ChannelSupplier<ByteBuf> input;
	private ChannelConsumer<ByteBuf> output;

	@Nullable
	private Inspector inspector;

	public interface Inspector extends BaseInspector<Inspector> {
		void onBuf(ByteBuf in, ByteBuf out);

		void onEndOfStream(ByteBuf endOfStreamBlock);
	}

	public static class JmxInspector extends AbstractInspector<Inspector> implements Inspector {
		public static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);

		private final ValueStats bytesIn = ValueStats.create(SMOOTHING_WINDOW);
		private final ValueStats bytesOut = ValueStats.create(SMOOTHING_WINDOW);

		@Override
		public void onBuf(ByteBuf in, ByteBuf out) {
			bytesIn.recordValue(in.readRemaining());
			bytesOut.recordValue(out.readRemaining());
		}

		@Override
		public void onEndOfStream(ByteBuf endOfStreamBlock) {
			bytesOut.recordValue(endOfStreamBlock.readRemaining());
		}

		@JmxAttribute
		public ValueStats getBytesIn() {
			return bytesIn;
		}

		@JmxAttribute
		public ValueStats getBytesOut() {
			return bytesOut;
		}
	}

	// region creators
	private ChannelCompressor(BlockCompressor compressor) {
		this.compressor = compressor;
	}

	public static ChannelCompressor create(BlockCompressor compressor) {
		return new ChannelCompressor(compressor);
	}

	public ChannelCompressor withInspector(Inspector inspector) {
		this.inspector = inspector;
		return this;
	}

	//check input for clarity
	@Override
	public ChannelInput<ByteBuf> getInput() {
		return input -> {
			this.input = sanitize(input);
			//noinspection ConstantConditions
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

	@Override
	protected void doProcess() {
		input.filter(ByteBuf::canRead)
				.get()
				.whenResult(buf -> {
					if (buf != null) {
						ByteBuf outputBuf = compressor.compress(buf);
						if (inspector != null) inspector.onBuf(buf, outputBuf);
						buf.recycle();
						output.accept(outputBuf)
								.whenResult(this::doProcess);
					} else {
						ByteBuf endOfStreamBlock = compressor.getEndOfStreamBlock();
						if (inspector != null) inspector.onEndOfStream(endOfStreamBlock);
						output.acceptAll(endOfStreamBlock, null)
								.whenResult(this::completeProcess);
					}
				});
	}

	@Override
	protected void doClose(Throwable e) {
		input.closeEx(e);
		output.closeEx(e);
	}

	// endregion

}
