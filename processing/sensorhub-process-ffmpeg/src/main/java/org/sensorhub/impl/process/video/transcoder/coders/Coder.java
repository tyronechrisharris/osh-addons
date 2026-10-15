package org.sensorhub.impl.process.video.transcoder.coders;

import static org.bytedeco.ffmpeg.global.avutil.*;

import java.util.HashMap;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static org.bytedeco.ffmpeg.global.avcodec.*;

import org.bytedeco.ffmpeg.avcodec.AVCodecContext;
import org.bytedeco.ffmpeg.avcodec.AVPacket;
import org.bytedeco.ffmpeg.avutil.AVFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Coder<I, O> extends Thread {

    protected static final Logger logger = LoggerFactory.getLogger(Coder.class);

    int codecId;
    private static final int MAX_QUEUE_SIZE = 500;
    protected AVCodecContext codec_ctx;
    protected Queue<I> inPackets; // ONLY allow the main loop to poll
    protected final Queue<O> outPackets; // ONLY allow the main loop to add
    protected I inPacket;
    protected O outPacket;
    //volatile boolean isProcessing = false; // Set to true at the start of the loop, false at the end
    volatile boolean isGettingPackets = false;
    final Object waitingObj = new Object();
    Class<I> inputClass;
    Class<O> outputClass;
    HashMap<String, Integer> options;

    private volatile Throwable failure;
    private final AtomicBoolean endOfInput = new AtomicBoolean(false);
    private volatile boolean outputComplete;

    public AtomicBoolean doRun = new AtomicBoolean(true);

    public Coder(int codecId, Class<I> inputClass, Class<O> outputClass, HashMap<String, Integer> options) {
        super();

        assert inputClass == AVPacket.class || inputClass == AVFrame.class;
        assert outputClass == AVPacket.class || outputClass == AVFrame.class;
        assert options != null;

        this.codecId = codecId;
        // These queues cross thread boundaries. ArrayDeque is not safe for a codec worker and its
        // producer/consumer to mutate concurrently, and corruption here also loses native packet
        // ownership. ConcurrentLinkedQueue makes every poll an atomic ownership transfer.
        this.inPackets = new ConcurrentLinkedQueue<>();
        this.outPackets = new ConcurrentLinkedQueue<>();
        this.inputClass = inputClass;
        this.outputClass = outputClass;
        this.options = options;
    }

    /**
     * Gets reference to output queue. Typically used as the input queue for another {@link Coder}.
     * @return Output queue containing either {@link AVPacket}s or {@link AVFrame}s.
     */
    public Queue<O> getOutQueue() {
        return outPackets;
    }

    /** Returns a terminal worker failure, if codec initialization or processing failed. */
    public Throwable getFailure() {
        return failure;
    }

    /**
     * Signals that no more input will be queued. The worker drains codec-delayed output and exits
     * after all input already in the queue has been consumed.
     */
    public void finishInput() {
        endOfInput.set(true);
        LockSupport.unpark(this);
    }

    /** Returns true after end-of-input has been flushed through the codec. */
    public boolean isOutputComplete() {
        return outputComplete;
    }

    /**
     * Sets input queue.
     * @param inPackets Typically the output queue of another {@link Coder}, containing either
     *                  {@link AVPacket}s or {@link AVFrame}s.
     */
    public void setInQueue(Queue <?> inPackets) {
        this.inPackets = (Queue<I>) Objects.requireNonNull(inPackets, "inPackets");
    }

    // Safety net queue purging.
    // Sometimes, queues can grow very large (like when a thread hangs up or is paused in the debugger).
    // Can recover, so we don't want memory issues from too many items in the queues.
    private void queuePurge() {
        if (outPackets.size() > MAX_QUEUE_SIZE) {
            logger.warn("Output queue is larger than max ({} > {}). Purging queue.",
                    outPackets.size(), MAX_QUEUE_SIZE);
        }
        while (outPackets.size() > MAX_QUEUE_SIZE) {
            O packet = outPackets.poll();
            if (packet != null) {
                deallocateOutputPacket(packet);
            }
        }
    }

    protected abstract void deallocateInputPacket(I packet);

    protected abstract void deallocateOutputPacket(O packet);

    protected abstract void deallocateOutQueue();

    // To be implemented by subclasses, encoder/decoder
    protected abstract void initContext();

    // Take data from input queue and send to encoder/decoder
    protected abstract void sendInPacket();

    // Take data from encoder/decoder and send to output queue
    protected abstract void receiveOutPacket();

    // Send the codec's end-of-input marker and receive all delayed output. Stateless transforms
    // have nothing to flush and inherit the no-op implementation.
    protected void flush() {
    }

    // Allocate packets/frames
    protected abstract void allocatePackets();

    // Deallocate packets/frames
    protected abstract void deallocatePackets();

    /**
     * Set certain options in the codec context.
     * @param codec_ctx Codec context. Context must be allocated first.
     */
    protected void initOptions(AVCodecContext codec_ctx) {
        int[] frameRate = frameRateFromOptions(options);
        // FFmpeg's time_base is seconds per frame, the inverse of framerate. Keep both sides of
        // the rational so rates such as 30000/1001 do not silently become 30/1.
        codec_ctx.time_base(av_make_q(frameRate[1], frameRate[0]));
        codec_ctx.framerate(av_make_q(frameRate[0], frameRate[1]));

        if (options.containsKey("bit_rate")) {
            codec_ctx.bit_rate(options.get("bit_rate") * 1000);
        } else {
            //codec_ctx.bit_rate(150*1000);
        }

        codec_ctx.width(options.getOrDefault("width", 1920));

        codec_ctx.height(options.getOrDefault("height", 1080));

        if (options.containsKey("pix_fmt")) {
            codec_ctx.pix_fmt(options.get("pix_fmt"));
        } else {
            if (codecId == AV_CODEC_ID_MJPEG) {
                codec_ctx.pix_fmt(AV_PIX_FMT_YUVJ420P);
            } else {
                codec_ctx.pix_fmt(AV_PIX_FMT_YUV420P);
            }
        }

        av_opt_set(codec_ctx.priv_data(), "preset", "ultrafast", 0);
        av_opt_set(codec_ctx.priv_data(), "tune", "zerolatency", 0);
        codec_ctx.strict_std_compliance(FF_COMPLIANCE_UNOFFICIAL); // Needed so that yuvj420p works (used for mjpeg)
    }

    static int[] frameRateFromOptions(HashMap<String, Integer> options) {
        int numerator = options.getOrDefault("fps_num", options.getOrDefault("fps", 30));
        int denominator = options.getOrDefault("fps_den", 1);
        if (numerator <= 0 || denominator <= 0) {
            return new int[] {30, 1};
        }
        return new int[] {numerator, denominator};
    }

    @Override
    public void run() {
        boolean packetsAllocated = false;
        try {
            initContext();
            // Subclasses clean null members safely, so mark cleanup required before allocation in
            // case a later native allocation fails after an earlier one succeeded.
            packetsAllocated = true;
            allocatePackets();

            // doRun is intentionally not reset here. A caller is allowed to request shutdown
            // immediately after start(); reviving the flag after that request leaves join()
            // blocked forever on an empty queue.
            while (doRun.get() && !Thread.currentThread().isInterrupted()) {
                if (inPackets == null || inPackets.isEmpty()) {
                    if (endOfInput.get()) {
                        flush();
                        outputComplete = true;
                        break;
                    }
                    // Codec workers are long-lived. A pure spin here pins one CPU core per idle
                    // worker; a short park keeps sub-millisecond response while interrupt makes
                    // shutdown immediate.
                    LockSupport.parkNanos(500_000L);
                    continue;
                }

                queuePurge();
                sendInPacket();
                receiveOutPacket();
            }
        } catch (Throwable t) {
            failure = t;
            doRun.set(false);
            logger.error("{} worker failed", getClass().getSimpleName(), t);
        } finally {
            synchronized (waitingObj) {
                waitingObj.notifyAll();
            }

            if (packetsAllocated) {
                try {
                    deallocatePackets();
                } catch (Throwable t) {
                    if (failure == null) {
                        failure = t;
                    } else {
                        failure.addSuppressed(t);
                    }
                    logger.error("{} packet cleanup failed", getClass().getSimpleName(), t);
                }
            }
            if (codec_ctx != null) {
                avcodec_free_context(codec_ctx);
            }
            codec_ctx = null;
        }
    }
}
