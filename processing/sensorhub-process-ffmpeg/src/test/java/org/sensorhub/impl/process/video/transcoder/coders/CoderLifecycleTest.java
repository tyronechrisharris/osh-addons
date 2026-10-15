package org.sensorhub.impl.process.video.transcoder.coders;

import org.bytedeco.ffmpeg.avcodec.AVPacket;
import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.bytedeco.ffmpeg.global.avcodec.av_packet_alloc;
import static org.bytedeco.ffmpeg.global.avcodec.av_packet_free;

public class CoderLifecycleTest {

    @Test
    public void shutdownRequestedBeforeWorkerInitializationIsNotReversed() throws Exception {
        StubCoder coder = new StubCoder();
        coder.doRun.set(false);

        coder.start();
        coder.join(2_000L);

        assertFalse("Worker must not revive itself after shutdown was requested", coder.isAlive());
    }

    @Test
    public void crossThreadQueuesUseAtomicOwnershipTransfers() {
        StubCoder coder = new StubCoder();

        assertTrue(coder.inPackets instanceof ConcurrentLinkedQueue);
        assertTrue(coder.getOutQueue() instanceof ConcurrentLinkedQueue);
    }

    @Test
    public void fractionalFrameRateIsNotRounded() {
        HashMap<String, Integer> options = new HashMap<>();
        options.put("fps_num", 30_000);
        options.put("fps_den", 1_001);

        assertArrayEquals(new int[] {30_000, 1_001}, Coder.frameRateFromOptions(options));
    }

    @Test
    public void endOfInputFlushesAndPreservesDelayedOutput() throws Exception {
        StubCoder coder = new StubCoder();
        AVPacket delayed = av_packet_alloc();
        coder.delayedOutput = delayed;
        try {
            coder.start();
            coder.finishInput();
            coder.join(2_000L);

            assertFalse("Worker did not stop after flushing end-of-input", coder.isAlive());
            assertTrue("Worker did not report flushed output", coder.isOutputComplete());
            assertSame("Delayed output was discarded during worker cleanup",
                    delayed, coder.getOutQueue().poll());
        } finally {
            av_packet_free(delayed);
        }
    }

    private static final class StubCoder extends Coder<AVPacket, AVPacket> {
        private AVPacket delayedOutput;

        private StubCoder() {
            super(0, AVPacket.class, AVPacket.class, new HashMap<>());
        }

        @Override protected void deallocateInputPacket(AVPacket packet) {}
        @Override protected void deallocateOutputPacket(AVPacket packet) {}
        @Override protected void deallocateOutQueue() { outPackets.clear(); }
        @Override protected void initContext() {}
        @Override protected void sendInPacket() {}
        @Override protected void receiveOutPacket() {}
        @Override protected void flush() {
            if (delayedOutput != null) {
                outPackets.add(delayedOutput);
            }
        }
        @Override protected void allocatePackets() {}
        @Override protected void deallocatePackets() {}
    }
}
