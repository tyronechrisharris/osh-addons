package org.sensorhub.impl.process.video.transcoder;

import org.bytedeco.ffmpeg.avcodec.AVPacket;
import org.bytedeco.ffmpeg.avutil.AVFrame;
import org.bytedeco.javacpp.Pointer;
import org.junit.After;
import org.junit.Test;
import org.sensorhub.impl.process.video.transcoder.coders.Coder;
import org.sensorhub.impl.process.video.transcoder.formatters.RgbFormatter;
import org.vast.process.ProcessException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.bytedeco.ffmpeg.global.avcodec.AV_CODEC_ID_H264;
import static org.bytedeco.ffmpeg.global.avcodec.av_packet_alloc;
import static org.bytedeco.ffmpeg.global.avcodec.av_packet_free;
import static org.bytedeco.ffmpeg.global.avutil.AV_PIX_FMT_YUV420P;
import static org.bytedeco.ffmpeg.global.avutil.av_frame_alloc;
import static org.bytedeco.ffmpeg.global.avutil.av_frame_free;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FFMpegTranscoderLifecycleTest {

    private final List<FFMpegTranscoder> runningTranscoders = new ArrayList<>();

    @After
    public void stopPipelines() throws Exception {
        for (FFMpegTranscoder transcoder : runningTranscoders) {
            if (transcoder.isRunning.get()) {
                transcoder.stopProcessThreads();
            }
        }
    }

    @Test
    public void rawRgbProductionPipelineUsesLastCoderQueueAndPublishesH264() throws Exception {
        CapturingTranscoder transcoder = new CapturingTranscoder();
        configureRgbToH264(transcoder, 32, 32);
        transcoder.initCoders();
        runningTranscoders.add(transcoder);

        assertTrue(transcoder.inputPackets instanceof BlockingQueue);
        assertSame(transcoder.videoProcs.get(transcoder.videoProcs.size() - 1).getOutQueue(),
                transcoder.outputPackets);
        assertNotSame("The final queue must never fall back to untranscoded input",
                transcoder.inputPackets, transcoder.outputPackets);

        transcoder.startProcessThreads();
        byte[] rgb = new byte[32 * 32 * 3];
        for (int i = 0; i < rgb.length; i++) {
            rgb[i] = (byte)(i * 31);
        }
        transcoder.inputPackets.add(new RgbFormatter(32, 32).convertInput(rgb));

        assertTrue("Native RGB-to-H.264 pipeline produced no encoded packet",
                await(() -> transcoder.encodedPacketSizes.stream().anyMatch(size -> size > 0), 10_000L));
        transcoder.stopProcessThreads();

        transcoder.throwIfPipelineFailed();
        assertTrue(transcoder.inputPackets.isEmpty());
        assertTrue(transcoder.outputPackets.isEmpty());

        int firstRunPacketCount = transcoder.encodedPacketSizes.size();
        transcoder.startProcessThreads();
        transcoder.inputPackets.add(new RgbFormatter(32, 32).convertInput(rgb));
        assertTrue("Restarted native pipeline produced no encoded packet",
                await(() -> transcoder.encodedPacketSizes.size() > firstRunPacketCount, 10_000L));
        transcoder.stopProcessThreads();
        transcoder.throwIfPipelineFailed();
    }

    @Test
    public void outputPublisherBlocksWhileIdleInsteadOfSpinning() throws Exception {
        NativeOwnership ownership = new NativeOwnership();
        TrackingCoder coder = new TrackingCoder(ownership, false, false);
        CapturingTranscoder transcoder = wireAndStart(Collections.singletonList(coder));

        assertTrue("Output publisher never entered a blocking wait",
                await(() -> transcoder.outputThread.getState() == Thread.State.TIMED_WAITING,
                        2_000L));

        long startNanos = System.nanoTime();
        transcoder.stopProcessThreads();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        assertFalse(transcoder.outputThread.isAlive());
        assertTrue("Idle shutdown should not deadlock", elapsedMillis < 2_000L);
        ownership.assertEveryAllocationFreedExactlyOnce();
    }

    @Test
    public void gracefulStopFlushesStagesInOrderAndFreesEveryPacketOnce() throws Exception {
        NativeOwnership ownership = new NativeOwnership();
        PacketToFrameCoder first = new PacketToFrameCoder(ownership);
        FrameToPacketCoder second = new FrameToPacketCoder(ownership);
        CapturingTranscoder transcoder = wireAndStart(List.of(first, second));

        AVPacket input = ownership.allocatePacket(101L);
        transcoder.inputPackets.add(input);
        transcoder.stopProcessThreads();

        assertTrue("First stage did not flush", first.flushed);
        assertTrue("Second stage was finished before it consumed the upstream flush",
                second.consumedPts.contains(PacketToFrameCoder.FLUSH_PTS));
        assertTrue("Final delayed output was not published",
                transcoder.publishedPts.contains(FrameToPacketCoder.FLUSH_PTS));
        assertTrue(first.isOutputComplete());
        assertTrue(second.isOutputComplete());
        assertFalse(transcoder.outputThread.isAlive());
        ownership.assertEveryAllocationFreedExactlyOnce();
    }

    @Test
    public void workerFailureSurfacesAndQueuedPacketsAreFreedExactlyOnce() throws Exception {
        NativeOwnership ownership = new NativeOwnership();
        TrackingCoder failingCoder = new TrackingCoder(ownership, false, true);
        CapturingTranscoder transcoder = new CapturingTranscoder();
        transcoder.wireCoders(Collections.singletonList(failingCoder));
        transcoder.inputPackets.add(ownership.allocatePacket(1L));
        transcoder.inputPackets.add(ownership.allocatePacket(2L));
        runningTranscoders.add(transcoder);
        transcoder.startProcessThreads();

        assertTrue("Worker failure was not propagated to the transcoder",
                await(() -> failingCoder.getFailure() != null, 2_000L));
        try {
            transcoder.throwIfPipelineFailed();
            fail("Expected pipeline failure to surface as ProcessException");
        } catch (ProcessException expected) {
            assertSame(failingCoder.getFailure(), expected.getCause());
        }

        transcoder.stopProcessThreads();
        assertFalse(transcoder.outputThread.isAlive());
        ownership.assertEveryAllocationFreedExactlyOnce();
    }

    @Test
    public void shutdownAtomicallyRejectsInputAfterEndOfInputBegins() throws Exception {
        NativeOwnership ownership = new NativeOwnership();
        GateFlushCoder coder = new GateFlushCoder(ownership);
        CapturingTranscoder transcoder = wireAndStart(Collections.singletonList(coder));
        AtomicReference<Throwable> stopFailure = new AtomicReference<>();

        Thread stopper = new Thread(() -> {
            try {
                transcoder.stopProcessThreads();
            } catch (Throwable t) {
                stopFailure.set(t);
            }
        }, "transcoder-stop-test");
        stopper.start();
        assertTrue("Worker never reached end-of-input flush",
                coder.flushEntered.await(2, TimeUnit.SECONDS));

        AVPacket latePacket = ownership.allocatePacket(303L);
        try {
            transcoder.enqueueInputPacket(latePacket);
            fail("Input was accepted after shutdown closed the pipeline");
        } catch (ProcessException expected) {
            ownership.free(latePacket);
        } finally {
            coder.releaseFlush.countDown();
        }

        stopper.join(2_000L);
        assertFalse("Shutdown deadlocked at the enqueue boundary", stopper.isAlive());
        if (stopFailure.get() != null) {
            throw new AssertionError("Shutdown failed", stopFailure.get());
        }
        ownership.assertEveryAllocationFreedExactlyOnce();
    }

    @Test
    public void stuckCoderKeepsGenerationFailedAndBlocksRestartUntilItDies() throws Exception {
        NativeOwnership ownership = new NativeOwnership();
        StuckInputCoder coder = new StuckInputCoder(ownership);
        ShortTimeoutTranscoder transcoder = new ShortTimeoutTranscoder();
        transcoder.wireCoders(Collections.singletonList(coder));
        runningTranscoders.add(transcoder);
        transcoder.startProcessThreads();
        transcoder.inputPackets.add(ownership.allocatePacket(808L));
        assertTrue(coder.inputEntered.await(2, TimeUnit.SECONDS));

        try {
            transcoder.stopProcessThreads();
            fail("Expected shutdown to fail while a coder ignores cancellation");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("restart is blocked"));
        }
        assertTrue("A live failed generation must remain owned", transcoder.isRunning.get());
        try {
            transcoder.startProcessThreads();
            fail("Restart replaced a generation whose coder was still alive");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("restart is blocked"));
        }

        coder.releaseInput.countDown();
        assertTrue(await(() -> !coder.isAlive(), 2_000L));
        transcoder.stopProcessThreads();
        assertFalse(transcoder.isRunning.get());
        ownership.assertEveryAllocationFreedExactlyOnce();
    }

    @Test
    public void stuckPublisherUsesCapturedGenerationAndBlocksRestartUntilItDies() throws Exception {
        NativeOwnership ownership = new NativeOwnership();
        ForwardingCoder coder = new ForwardingCoder(ownership);
        BlockingPublisherTranscoder transcoder = new BlockingPublisherTranscoder();
        transcoder.wireCoders(Collections.singletonList(coder));
        runningTranscoders.add(transcoder);
        transcoder.startProcessThreads();
        transcoder.inputPackets.add(ownership.allocatePacket(909L));
        assertTrue(transcoder.publishEntered.await(2, TimeUnit.SECONDS));

        try {
            transcoder.stopProcessThreads();
            fail("Expected shutdown to fail while the publisher ignores cancellation");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("restart is blocked"));
        }
        BlockingQueue<Pointer> oldOutputQueue = transcoder.outputPackets;
        try {
            transcoder.startProcessThreads();
            fail("Restart replaced a generation whose publisher was still alive");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("restart is blocked"));
        }
        assertSame("A timed-out publisher must retain its captured generation queue",
                oldOutputQueue, transcoder.outputPackets);

        transcoder.releasePublish.countDown();
        assertTrue(await(() -> !transcoder.outputThread.isAlive(), 2_000L));
        transcoder.stopProcessThreads();
        assertFalse(transcoder.isRunning.get());
        ownership.assertEveryAllocationFreedExactlyOnce();
    }

    @Test
    public void activeReinitStopsOldGenerationBeforeReplacingPipelineReferences() throws Exception {
        CapturingTranscoder transcoder = new CapturingTranscoder();
        configureProcessParameters(transcoder, 32, 32);
        transcoder.init();
        runningTranscoders.add(transcoder);
        transcoder.startProcessThreads();
        List<Coder<?, ?>> oldCoders = new ArrayList<>(transcoder.videoProcs);
        Thread oldPublisher = transcoder.outputThread;

        transcoder.init();

        assertFalse(oldPublisher.isAlive());
        for (Coder<?, ?> oldCoder : oldCoders) {
            assertFalse("Re-init replaced a live coder reference", oldCoder.isAlive());
        }
        assertFalse(transcoder.isRunning.get());
        assertNotSame(oldCoders.get(0), transcoder.videoProcs.get(0));
        transcoder.startProcessThreads();
        transcoder.stopProcessThreads();
    }

    @Test
    public void concurrentStartsCreateOnlyOneGeneration() throws Exception {
        NativeOwnership ownership = new NativeOwnership();
        TrackingCoder coder = new TrackingCoder(ownership, false, false);
        CapturingTranscoder transcoder = new CapturingTranscoder();
        transcoder.wireCoders(Collections.singletonList(coder));
        runningTranscoders.add(transcoder);
        CyclicBarrier startGate = new CyclicBarrier(3);
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        AtomicReference<Throwable> secondFailure = new AtomicReference<>();

        Thread first = new Thread(() -> startAtBarrier(transcoder, startGate, firstFailure));
        Thread second = new Thread(() -> startAtBarrier(transcoder, startGate, secondFailure));
        first.start();
        second.start();
        startGate.await(2, TimeUnit.SECONDS);
        first.join(2_000L);
        second.join(2_000L);

        assertSame(null, firstFailure.get());
        assertSame(null, secondFailure.get());
        assertTrue(coder.isAlive());
        transcoder.stopProcessThreads();
    }

    @Test
    public void publicStopQuiescesBaseExecutorBeforePipelineTeardown() throws Exception {
        NativeOwnership ownership = new NativeOwnership();
        AdversarialBaseExecutorTranscoder transcoder =
                new AdversarialBaseExecutorTranscoder(ownership,
                        new TrackingCoder(ownership, false, false), 1_000L);
        transcoder.prepare();
        runningTranscoders.add(transcoder);
        transcoder.startProcessThreads();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<Throwable> executorFailure = new AtomicReference<>();
        try {
            transcoder.start(executor, executorFailure::set);
            assertTrue(transcoder.executeEntered.await(2, TimeUnit.SECONDS));

            transcoder.stop();

            assertFalse("Base executor remained started during native teardown",
                    transcoder.isBaseExecutorStarted());
            assertFalse(transcoder.isRunning.get());

            // Model an execution that was already inside an uninterruptible native call when
            // stop cancelled the Future. It must not recreate the just-stopped pipeline.
            transcoder.releaseExecute.countDown();
            assertTrue(transcoder.executeFinished.await(2, TimeUnit.SECONDS));
            assertTrue("Stale base execution was not rejected",
                    await(() -> executorFailure.get() != null, 2_000L));
            assertEquals("Stale execution created a second codec generation",
                    1, transcoder.generationCount.get());
            assertFalse(transcoder.isRunning.get());
        } finally {
            transcoder.releaseExecute.countDown();
            executor.shutdownNow();
            executor.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    public void publicStopStillQuiescesBaseExecutorWhenPipelineTeardownFails() throws Exception {
        NativeOwnership ownership = new NativeOwnership();
        StuckInputCoder stuckCoder = new StuckInputCoder(ownership);
        AdversarialBaseExecutorTranscoder transcoder =
                new AdversarialBaseExecutorTranscoder(ownership, stuckCoder, 75L);
        transcoder.prepare();
        runningTranscoders.add(transcoder);
        transcoder.startProcessThreads();
        transcoder.inputPackets.add(ownership.allocatePacket(1_010L));
        assertTrue(stuckCoder.inputEntered.await(2, TimeUnit.SECONDS));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            transcoder.start(executor, ignored -> { });
            assertTrue(transcoder.executeEntered.await(2, TimeUnit.SECONDS));

            try {
                transcoder.stop();
                fail("Expected native pipeline shutdown to report its stuck worker");
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage().contains("restart is blocked"));
            }
            assertFalse("Pipeline failure skipped base executor shutdown",
                    transcoder.isBaseExecutorStarted());
        } finally {
            transcoder.releaseExecute.countDown();
            stuckCoder.releaseInput.countDown();
            assertTrue(await(() -> !stuckCoder.isAlive(), 2_000L));
            if (transcoder.isRunning.get()) {
                transcoder.stopProcessThreads();
            }
            executor.shutdownNow();
            executor.awaitTermination(2, TimeUnit.SECONDS);
        }
        ownership.assertEveryAllocationFreedExactlyOnce();
    }

    @Test
    public void disposeGatesStaleBaseExecutionAndRecordsDisposedState() throws Exception {
        NativeOwnership ownership = new NativeOwnership();
        AdversarialBaseExecutorTranscoder transcoder =
                new AdversarialBaseExecutorTranscoder(ownership,
                        new TrackingCoder(ownership, false, false), 1_000L);
        transcoder.prepare();
        runningTranscoders.add(transcoder);
        transcoder.startProcessThreads();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<Throwable> executorFailure = new AtomicReference<>();
        try {
            transcoder.start(executor, executorFailure::set);
            assertTrue(transcoder.executeEntered.await(2, TimeUnit.SECONDS));

            transcoder.dispose();

            assertFalse(transcoder.isBaseExecutorStarted());
            assertFalse("Base dispose was not recorded", transcoder.isInitialized());
            assertFalse(transcoder.isRunning.get());

            transcoder.releaseExecute.countDown();
            assertTrue(transcoder.executeFinished.await(2, TimeUnit.SECONDS));
            assertTrue("Stale execution survived disposal",
                    await(() -> executorFailure.get() != null, 2_000L));
            assertEquals("Stale execution created a post-dispose codec generation",
                    1, transcoder.generationCount.get());
            assertFalse(transcoder.isRunning.get());
        } finally {
            transcoder.releaseExecute.countDown();
            executor.shutdownNow();
            executor.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    public void disposeRecordsBaseStateWhenNativeTeardownFails() throws Exception {
        NativeOwnership ownership = new NativeOwnership();
        StuckInputCoder stuckCoder = new StuckInputCoder(ownership);
        AdversarialBaseExecutorTranscoder transcoder =
                new AdversarialBaseExecutorTranscoder(ownership, stuckCoder, 75L);
        transcoder.prepare();
        runningTranscoders.add(transcoder);
        transcoder.startProcessThreads();
        transcoder.inputPackets.add(ownership.allocatePacket(1_111L));
        assertTrue(stuckCoder.inputEntered.await(2, TimeUnit.SECONDS));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            transcoder.start(executor, ignored -> { });
            assertTrue(transcoder.executeEntered.await(2, TimeUnit.SECONDS));

            try {
                transcoder.dispose();
                fail("Expected disposal to preserve the native teardown failure");
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage().contains("restart is blocked"));
            }
            assertFalse(transcoder.isBaseExecutorStarted());
            assertFalse("Native failure skipped base dispose", transcoder.isInitialized());
        } finally {
            transcoder.releaseExecute.countDown();
            stuckCoder.releaseInput.countDown();
            assertTrue(await(() -> !stuckCoder.isAlive(), 2_000L));
            if (transcoder.isRunning.get()) {
                transcoder.stopProcessThreads();
            }
            executor.shutdownNow();
            executor.awaitTermination(2, TimeUnit.SECONDS);
        }
        ownership.assertEveryAllocationFreedExactlyOnce();
    }

    @Test
    public void failedExplicitStartDoesNotReopenStoppedBoundary() throws Exception {
        NativeOwnership ownership = new NativeOwnership();
        AdversarialBaseExecutorTranscoder transcoder =
                new AdversarialBaseExecutorTranscoder(ownership,
                        new TrackingCoder(ownership, false, false), 1_000L);
        transcoder.prepare();
        runningTranscoders.add(transcoder);
        transcoder.stop();

        ExecutorService rejectedExecutor = Executors.newSingleThreadExecutor();
        rejectedExecutor.shutdownNow();
        try {
            transcoder.start(rejectedExecutor, ignored -> { });
            fail("Expected the stopped executor to reject process start");
        } catch (RuntimeException expected) {
            // Rejected execution is the adversarial start failure under test.
        }

        assertFalse(transcoder.isBaseExecutorStarted());
        try {
            transcoder.startProcessThreads();
            fail("Failed explicit start reopened the stopped process boundary");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("process is stopped"));
        }
        assertEquals(1, transcoder.generationCount.get());
    }

    private CapturingTranscoder wireAndStart(List<Coder<?, ?>> coders) {
        CapturingTranscoder transcoder = new CapturingTranscoder();
        transcoder.wireCoders(coders);
        runningTranscoders.add(transcoder);
        transcoder.startProcessThreads();
        return transcoder;
    }

    private static void configureRgbToH264(FFMpegTranscoder transcoder, int width, int height) {
        transcoder.inCodec = CodecEnum.RGB;
        transcoder.outCodec = CodecEnum.H264;
        transcoder.width = width;
        transcoder.height = height;
        transcoder.outWidth = width;
        transcoder.outHeight = height;
        transcoder.decOptions = new HashMap<>();
        transcoder.encOptions = new HashMap<>();
        transcoder.encOptions.put("width", width);
        transcoder.encOptions.put("height", height);
        transcoder.encOptions.put("fps", 15);
        transcoder.encOptions.put("pix_fmt", AV_PIX_FMT_YUV420P);
    }

    private static void configureProcessParameters(FFMpegTranscoder transcoder, int width,
            int height) {
        transcoder.inCodecParam.assignNewDataBlock();
        transcoder.inCodecParam.getData().setStringValue(CodecEnum.RGB.name());
        transcoder.outCodecParam.assignNewDataBlock();
        transcoder.outCodecParam.getData().setStringValue(CodecEnum.H264.name());
        transcoder.inputWidth.assignNewDataBlock();
        transcoder.inputWidth.getData().setIntValue(width);
        transcoder.inputHeight.assignNewDataBlock();
        transcoder.inputHeight.getData().setIntValue(height);
        transcoder.outputWidth.assignNewDataBlock();
        transcoder.outputWidth.getData().setIntValue(width);
        transcoder.outputHeight.assignNewDataBlock();
        transcoder.outputHeight.getData().setIntValue(height);
    }

    private static void startAtBarrier(FFMpegTranscoder transcoder, CyclicBarrier barrier,
            AtomicReference<Throwable> failure) {
        try {
            barrier.await(2, TimeUnit.SECONDS);
            transcoder.startProcessThreads();
        } catch (Throwable t) {
            failure.set(t);
        }
    }

    private static boolean await(BooleanSupplier condition, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(10L);
        }
        return condition.getAsBoolean();
    }

    private static class CapturingTranscoder extends FFMpegTranscoder {
        private final List<Long> publishedPts = new CopyOnWriteArrayList<>();
        private final List<Integer> encodedPacketSizes = new CopyOnWriteArrayList<>();

        @Override
        protected void publishOutputPacket(Pointer packet) {
            if (!(packet instanceof AVPacket)) {
                throw new AssertionError("Expected AVPacket output but received "
                        + packet.getClass().getSimpleName());
            }
            AVPacket avPacket = (AVPacket)packet;
            publishedPts.add(avPacket.pts());
            encodedPacketSizes.add(avPacket.size());
        }
    }

    private static class ShortTimeoutTranscoder extends CapturingTranscoder {
        @Override
        protected long pipelineJoinTimeoutMillis() {
            return 75L;
        }
    }

    private static final class BlockingPublisherTranscoder extends ShortTimeoutTranscoder {
        private final CountDownLatch publishEntered = new CountDownLatch(1);
        private final CountDownLatch releasePublish = new CountDownLatch(1);

        @Override
        protected void publishOutputPacket(Pointer packet) {
            publishEntered.countDown();
            boolean released = false;
            while (!released) {
                try {
                    released = releasePublish.await(25, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignored) {
                    // Model a native publishing call that does not respond to cancellation.
                }
            }
        }
    }

    private static final class AdversarialBaseExecutorTranscoder
            extends ShortTimeoutTranscoder {
        private final NativeOwnership ownership;
        private final long joinTimeoutMillis;
        private Coder<?, ?> nextCoder;
        private final AtomicInteger generationCount = new AtomicInteger();
        private final CountDownLatch executeEntered = new CountDownLatch(1);
        private final CountDownLatch releaseExecute = new CountDownLatch(1);
        private final CountDownLatch executeFinished = new CountDownLatch(1);

        private AdversarialBaseExecutorTranscoder(NativeOwnership ownership,
                Coder<?, ?> firstCoder, long joinTimeoutMillis) {
            this.ownership = ownership;
            this.nextCoder = firstCoder;
            this.joinTimeoutMillis = joinTimeoutMillis;
        }

        private void prepare() {
            initCoders();
            initialized = true;
        }

        private boolean isBaseExecutorStarted() {
            return started;
        }

        private boolean isInitialized() {
            return initialized;
        }

        @Override
        protected long pipelineJoinTimeoutMillis() {
            return joinTimeoutMillis;
        }

        @Override
        void initCoders() {
            Coder<?, ?> coder = nextCoder;
            if (coder == null) {
                coder = new TrackingCoder(ownership, false, false);
            }
            nextCoder = null;
            wireCoders(Collections.singletonList(coder));
            generationCount.incrementAndGet();
        }

        @Override
        public void execute() {
            executeEntered.countDown();
            boolean released = false;
            while (!released) {
                try {
                    released = releaseExecute.await(25, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignored) {
                    // Model an in-flight native call that ignores Future cancellation.
                }
            }
            try {
                startProcessThreads();
            } finally {
                executeFinished.countDown();
            }
        }
    }

    private static final class TrackingCoder extends Coder<AVPacket, AVPacket> {
        private static final long FLUSH_PTS = 9_001L;

        private final NativeOwnership ownership;
        private final boolean emitOnFlush;
        private final boolean failOnInput;
        private final List<Long> consumedPts = new CopyOnWriteArrayList<>();
        private volatile boolean flushed;

        private TrackingCoder(NativeOwnership ownership, boolean emitOnFlush,
                boolean failOnInput) {
            super(AV_CODEC_ID_H264, AVPacket.class, AVPacket.class, new HashMap<>());
            this.ownership = ownership;
            this.emitOnFlush = emitOnFlush;
            this.failOnInput = failOnInput;
        }

        @Override
        protected void initContext() {
        }

        @Override
        protected void sendInPacket(AVPacket packet) {
            consumedPts.add(packet.pts());
            ownership.free(packet);
            if (failOnInput) {
                throw new IllegalStateException("intentional test failure");
            }
        }

        @Override
        protected void receiveOutPacket() {
        }

        @Override
        protected void flush() {
            flushed = true;
            if (emitOnFlush) {
                outPackets.add(ownership.allocatePacket(FLUSH_PTS));
            }
        }

        @Override
        protected void allocatePackets() {
        }

        @Override
        protected void deallocatePackets() {
            AVPacket packet;
            while ((packet = inPackets.poll()) != null) {
                ownership.free(packet);
            }
            if (!isOutputComplete()) {
                deallocateOutQueue();
            }
        }

        @Override
        protected void deallocateInputPacket(AVPacket packet) {
            ownership.free(packet);
        }

        @Override
        protected void deallocateOutputPacket(AVPacket packet) {
            ownership.free(packet);
        }

        @Override
        protected void deallocateOutQueue() {
            AVPacket packet;
            while ((packet = outPackets.poll()) != null) {
                ownership.free(packet);
            }
        }
    }

    private static final class PacketToFrameCoder extends Coder<AVPacket, AVFrame> {
        private static final long FLUSH_PTS = 4_001L;

        private final NativeOwnership ownership;
        private volatile boolean flushed;

        private PacketToFrameCoder(NativeOwnership ownership) {
            super(AV_CODEC_ID_H264, AVPacket.class, AVFrame.class, new HashMap<>());
            this.ownership = ownership;
        }

        @Override protected void initContext() { }
        @Override protected void allocatePackets() { }
        @Override protected void receiveOutPacket() { }

        @Override
        protected void sendInPacket(AVPacket packet) {
            ownership.free(packet);
        }

        @Override
        protected void flush() {
            flushed = true;
            outPackets.add(ownership.allocateFrame(FLUSH_PTS));
        }

        @Override
        protected void deallocatePackets() {
            AVPacket input;
            while ((input = inPackets.poll()) != null) {
                ownership.free(input);
            }
            if (!isOutputComplete()) {
                deallocateOutQueue();
            }
        }

        @Override protected void deallocateInputPacket(AVPacket packet) { ownership.free(packet); }
        @Override protected void deallocateOutputPacket(AVFrame frame) { ownership.free(frame); }

        @Override
        protected void deallocateOutQueue() {
            AVFrame frame;
            while ((frame = outPackets.poll()) != null) {
                ownership.free(frame);
            }
        }
    }

    private static final class GateFlushCoder extends Coder<AVPacket, AVPacket> {
        private final NativeOwnership ownership;
        private final CountDownLatch flushEntered = new CountDownLatch(1);
        private final CountDownLatch releaseFlush = new CountDownLatch(1);

        private GateFlushCoder(NativeOwnership ownership) {
            super(AV_CODEC_ID_H264, AVPacket.class, AVPacket.class, new HashMap<>());
            this.ownership = ownership;
        }

        @Override protected void initContext() { }
        @Override protected void allocatePackets() { }
        @Override protected void receiveOutPacket() { }
        @Override protected void sendInPacket(AVPacket packet) { ownership.free(packet); }

        @Override
        protected void flush() {
            flushEntered.countDown();
            try {
                if (!releaseFlush.await(2, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Test did not release flush gate");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Flush gate interrupted", e);
            }
        }

        @Override
        protected void deallocatePackets() {
            AVPacket packet;
            while ((packet = inPackets.poll()) != null) {
                ownership.free(packet);
            }
            if (!isOutputComplete()) {
                deallocateOutQueue();
            }
        }

        @Override protected void deallocateInputPacket(AVPacket packet) { ownership.free(packet); }
        @Override protected void deallocateOutputPacket(AVPacket packet) { ownership.free(packet); }

        @Override
        protected void deallocateOutQueue() {
            AVPacket packet;
            while ((packet = outPackets.poll()) != null) {
                ownership.free(packet);
            }
        }
    }

    private static final class StuckInputCoder extends Coder<AVPacket, AVPacket> {
        private final NativeOwnership ownership;
        private final CountDownLatch inputEntered = new CountDownLatch(1);
        private final CountDownLatch releaseInput = new CountDownLatch(1);

        private StuckInputCoder(NativeOwnership ownership) {
            super(AV_CODEC_ID_H264, AVPacket.class, AVPacket.class, new HashMap<>());
            this.ownership = ownership;
        }

        @Override protected void initContext() { }
        @Override protected void allocatePackets() { }
        @Override protected void receiveOutPacket() { }

        @Override
        protected void sendInPacket(AVPacket packet) {
            inputEntered.countDown();
            boolean released = false;
            while (!released) {
                try {
                    released = releaseInput.await(25, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignored) {
                    // Model a native codec call that does not respond to cancellation.
                }
            }
            ownership.free(packet);
        }

        @Override
        protected void deallocatePackets() {
            AVPacket packet;
            while ((packet = inPackets.poll()) != null) {
                ownership.free(packet);
            }
            if (!isOutputComplete()) {
                deallocateOutQueue();
            }
        }

        @Override protected void deallocateInputPacket(AVPacket packet) { ownership.free(packet); }
        @Override protected void deallocateOutputPacket(AVPacket packet) { ownership.free(packet); }

        @Override
        protected void deallocateOutQueue() {
            AVPacket packet;
            while ((packet = outPackets.poll()) != null) {
                ownership.free(packet);
            }
        }
    }

    private static final class ForwardingCoder extends Coder<AVPacket, AVPacket> {
        private final NativeOwnership ownership;

        private ForwardingCoder(NativeOwnership ownership) {
            super(AV_CODEC_ID_H264, AVPacket.class, AVPacket.class, new HashMap<>());
            this.ownership = ownership;
        }

        @Override protected void initContext() { }
        @Override protected void allocatePackets() { }
        @Override protected void receiveOutPacket() { }

        @Override
        protected void sendInPacket(AVPacket packet) {
            // Move the same native ownership token to the publisher queue.
            outPackets.add(packet);
        }

        @Override
        protected void deallocatePackets() {
            AVPacket packet;
            while ((packet = inPackets.poll()) != null) {
                ownership.free(packet);
            }
            if (!isOutputComplete()) {
                deallocateOutQueue();
            }
        }

        @Override protected void deallocateInputPacket(AVPacket packet) { ownership.free(packet); }
        @Override protected void deallocateOutputPacket(AVPacket packet) { ownership.free(packet); }

        @Override
        protected void deallocateOutQueue() {
            AVPacket packet;
            while ((packet = outPackets.poll()) != null) {
                ownership.free(packet);
            }
        }
    }

    private static final class FrameToPacketCoder extends Coder<AVFrame, AVPacket> {
        private static final long FLUSH_PTS = 9_001L;

        private final NativeOwnership ownership;
        private final List<Long> consumedPts = new CopyOnWriteArrayList<>();

        private FrameToPacketCoder(NativeOwnership ownership) {
            super(AV_CODEC_ID_H264, AVFrame.class, AVPacket.class, new HashMap<>());
            this.ownership = ownership;
        }

        @Override protected void initContext() { }
        @Override protected void allocatePackets() { }
        @Override protected void receiveOutPacket() { }

        @Override
        protected void sendInPacket(AVFrame frame) {
            consumedPts.add(frame.pts());
            ownership.free(frame);
        }

        @Override
        protected void flush() {
            outPackets.add(ownership.allocatePacket(FLUSH_PTS));
        }

        @Override
        protected void deallocatePackets() {
            AVFrame input;
            while ((input = inPackets.poll()) != null) {
                ownership.free(input);
            }
            if (!isOutputComplete()) {
                deallocateOutQueue();
            }
        }

        @Override protected void deallocateInputPacket(AVFrame frame) { ownership.free(frame); }
        @Override protected void deallocateOutputPacket(AVPacket packet) { ownership.free(packet); }

        @Override
        protected void deallocateOutQueue() {
            AVPacket packet;
            while ((packet = outPackets.poll()) != null) {
                ownership.free(packet);
            }
        }
    }

    private static final class NativeOwnership {
        private final Map<Pointer, Integer> releases = new IdentityHashMap<>();

        synchronized AVPacket allocatePacket(long pts) {
            AVPacket packet = av_packet_alloc();
            if (packet == null || packet.isNull()) {
                throw new AssertionError("Could not allocate native test packet");
            }
            packet.pts(pts);
            releases.put(packet, 0);
            return packet;
        }

        synchronized AVFrame allocateFrame(long pts) {
            AVFrame frame = av_frame_alloc();
            if (frame == null || frame.isNull()) {
                throw new AssertionError("Could not allocate native test frame");
            }
            frame.pts(pts);
            releases.put(frame, 0);
            return frame;
        }

        synchronized void free(Pointer pointer) {
            Integer releaseCount = releases.get(pointer);
            if (releaseCount == null) {
                throw new AssertionError("Released native data not owned by this pipeline");
            }
            releases.put(pointer, releaseCount + 1);
            if (pointer instanceof AVPacket) {
                av_packet_free((AVPacket)pointer);
            } else if (pointer instanceof AVFrame) {
                av_frame_free((AVFrame)pointer);
            } else {
                throw new AssertionError("Unsupported native test pointer "
                        + pointer.getClass().getSimpleName());
            }
        }

        synchronized void assertEveryAllocationFreedExactlyOnce() {
            for (Integer releaseCount : releases.values()) {
                assertEquals("Native packet ownership was leaked or released more than once",
                        Integer.valueOf(1), releaseCount);
            }
        }
    }
}
