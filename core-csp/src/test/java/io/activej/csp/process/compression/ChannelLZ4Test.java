package io.activej.csp.process.compression;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.common.MemSize;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.process.ChannelByteChunker;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.activej.csp.binary.BinaryChannelSupplier.UNEXPECTED_DATA_EXCEPTION;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;

public class ChannelLZ4Test {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void test() {
		//[START EXAMPLE]
		int buffersCount = 100;

		List<ByteBuf> buffers = IntStream.range(0, buffersCount).mapToObj($ -> createRandomByteBuf()).collect(toList());
		byte[] expected = buffers.stream().map(ByteBuf::slice).collect(ByteBufQueue.collector()).asArray();

		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.ofList(buffers)
				.transformWith(ChannelByteChunker.create(MemSize.of(64), MemSize.of(128)))
				.transformWith(ChannelCompressor.create(LZ4BlockCompressor.createFastCompressor()))
				.transformWith(ChannelByteChunker.create(MemSize.of(64), MemSize.of(128)))
				.transformWith(ChannelDecompressor.create(LZ4BlockDecompressor.create()));

		ByteBuf collected = await(supplier.toCollector(ByteBufQueue.collector()));
		assertArrayEquals(expected, collected.asArray());
		//[END EXAMPLE]
	}

	@Test
	public void testLz4Fast() {
		doTest(LZ4BlockCompressor.createFastCompressor());
	}

	@Test
	public void testLz4High() {
		doTest(LZ4BlockCompressor.createHighCompressor());
	}

	@Test
	public void testLz4High10() {
		doTest(LZ4BlockCompressor.createHighCompressor(10));
	}

	@Test
	public void testTruncatedData() {
		ChannelCompressor compressor = ChannelCompressor.create(LZ4BlockCompressor.createFastCompressor());
		ChannelDecompressor decompressor = ChannelDecompressor.create(LZ4BlockDecompressor.create());
		ByteBufQueue queue = new ByteBufQueue();

		await(ChannelSupplier.of(ByteBufStrings.wrapAscii("TestData")).transformWith(compressor)
				.streamTo(ChannelConsumer.ofConsumer(queue::add)));

		// add trailing 0 - bytes
		queue.add(ByteBuf.wrapForReading(new byte[10]));

		Throwable e = awaitException(ChannelSupplier.of(queue.takeRemaining())
				.transformWith(decompressor)
				.streamTo(ChannelConsumer.ofConsumer(data -> System.out.println(data.asString(UTF_8)))));

		assertSame(UNEXPECTED_DATA_EXCEPTION, e);
	}

	@Test
	public void testCustomEndOfStreamBlock() {
		LZ4BlockCompressor compressor = LZ4BlockCompressor.createFastCompressor()
				.withCustomEndOfStreamBlock(true);
		LZ4BlockDecompressor decompressor = LZ4BlockDecompressor.create()
				.withCustomEndOfStreamBlock(true);

		doTest(compressor, decompressor);
	}

	private static void doTest(LZ4BlockCompressor compressor) {
		doTest(compressor, LZ4BlockDecompressor.create());
	}

	private static void doTest(LZ4BlockCompressor compressor, LZ4BlockDecompressor decompressor) {
		ByteBuf data = createRandomByteBuf();

		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.of(data.slice())
				.transformWith(ChannelCompressor.create(compressor))
				.transformWith(ChannelDecompressor.create(decompressor));

		ByteBuf collected = await(supplier.toCollector(ByteBufQueue.collector()));
		assertArrayEquals(data.asArray(), collected.asArray());
	}

	private static ByteBuf createRandomByteBuf() {
		ThreadLocalRandom random = ThreadLocalRandom.current();
		int offset = random.nextInt(10);
		int tail = random.nextInt(10);
		int len = random.nextInt(100);
		byte[] array = new byte[offset + len + tail];
		random.nextBytes(array);
		return ByteBuf.wrap(array, offset, offset + len);
	}

}
