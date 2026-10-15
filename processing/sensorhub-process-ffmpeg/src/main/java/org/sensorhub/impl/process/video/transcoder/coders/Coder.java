package org.sensorhub.impl.process.video.transcoder.coders;

import static org.bytedeco.ffmpeg.global.avutil.*;

import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.bytedeco.ffmpeg.global.avcodec.*;

import org.bytedeco.ffmpeg.avcodec.AVCodecContext;
import org.bytedeco.ffmpeg.avcodec.AVPacket;
import org.bytedeco.ffmpeg.avutil.AVFrame;
import org.bytedeco.javacpp.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Coder<I extends Pointer, O extends Pointer> extends Thread {

    protected static final Logger logger = LoggerFactory.getLogger(Coder.class);

    int codecId;
    private static final int MAX_QUEUE_SIZE = 500;
    protected AVCodecContext codec_ctx;
    private static final long INPUT_WAIT_MILLIS = 100L;

    protected BlockingQueue<I> inPackets; // ONLY allow the main loop to poll
    protected final BlockingQueue<O> outPackets; // ONLY allow the main loop to add
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
    private volatile Consumer<Throwable> failureHandler = ignored -> { };

    public AtomicBoolean doRun = new AtomicBoolean(true);

    public Coder(int codecId, Class<I> inputClass, Class<O> outputClass, HashMap<String, Integer> options) {
        super();

        if (inputClass != AVPacket.class && inputClass != AVFrame.class) {
            throw new IllegalArgumentException("Coder input must be AVPacket or AVFrame");
        }
        if (outputClass != AVPacket.class && outputClass != AVFrame.class) {
            throw new IllegalArgumentException("Coder output must be AVPacket or AVFrame");
        }
        Objects.requireNonNull(options, "options");

        this.codecId = codecId;
        // These queues cross thread boundaries. Polling is the native-object ownership transfer;
        // a blocking queue makes that transfer atomic and lets an idle codec sleep instead of
        // consuming an entire CPU core while it waits for the next frame.
        this.inPackets = new LinkedBlockingQueue<>();
        this.outPackets = new LinkedBlockingQueue<>();
        this.inputClass = inputClass;
        this.outputClass = outputClass;
        this.options = options;
    }

    /**
     * Gets reference to output queue. Typically used as the input queue for another {@link Coder}.
     * @return Output queue containing either {@link AVPacket}s or {@link AVFrame}s.
     */
    public BlockingQueue<O> getOutQueue() {
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
    }

    /** Returns true after end-of-input has been flushed through the codec. */
    public boolean isOutputComplete() {
        return outputComplete;
    }

    /** Requests an immediate shutdown. Queued native objects are released by worker cleanup. */
    public void cancel() {
        doRun.set(false);
        interrupt();
    }

    /** Installs the owner callback used to surface asynchronous worker failures. */
    public void setFailureHandler(Consumer<Throwable> failureHandler) {
        this.failureHandler = Objects.requireNonNull(failureHandler, "failureHandler");
    }

    /** Releases one output after the downstream consumer has taken ownership of it. */
    public final void releaseOutput(Pointer packet) {
        if (packet != null) {
            deallocateOutputPacket(outputClass.cast(packet));
        }
    }

    /**
     * Sets input queue.
     * @param inPackets Typically the output queue of another {@link Coder}, containing either
     *                  {@link AVPacket}s or {@link AVFrame}s.
     */
    public void setInQueue(BlockingQueue<?> inPackets) {
        this.inPackets = (BlockingQueue<I>) Objects.requireNonNull(inPackets, "inPackets");
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
    protected abstract void sendInPacket(I packet);

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
        try {
            initContext();
            allocatePackets();

            // doRun is intentionally not reset here. A caller is allowed to request shutdown
            // immediately after start(); reviving the flag after that request leaves join()
            // blocked forever on an empty queue.
            while (doRun.get() && !Thread.currentThread().isInterrupted()) {
                I packet = inPackets.poll(INPUT_WAIT_MILLIS, TimeUnit.MILLISECONDS);
                if (packet == null) {
                    if (endOfInput.get() && inPackets.isEmpty()) {
                        flush();
                        outputComplete = true;
                        break;
                    }
                    continue;
                }

                queuePurge();
                sendInPacket(packet);
                receiveOutPacket();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (doRun.get()) {
                recordFailure(e);
            }
        } catch (Throwable t) {
            recordFailure(t);
        } finally {
            synchronized (waitingObj) {
                waitingObj.notifyAll();
            }

            // Concrete cleanup methods are null-safe and also drain the input queue. Run cleanup
            // even when codec initialization failed, otherwise input accepted just before a
            // native init error would be orphaned.
            try {
                deallocatePackets();
            } catch (Throwable t) {
                if (failure == null) {
                    failure = t;
                    try {
                        failureHandler.accept(t);
                    } catch (Throwable callbackFailure) {
                        t.addSuppressed(callbackFailure);
                    }
                } else {
                    failure.addSuppressed(t);
                }
                logger.error("{} packet cleanup failed", getClass().getSimpleName(), t);
            }
            if (codec_ctx != null) {
                avcodec_free_context(codec_ctx);
            }
            codec_ctx = null;
        }
    }

    private void recordFailure(Throwable t) {
        failure = t;
        doRun.set(false);
        try {
            failureHandler.accept(t);
        } catch (Throwable callbackFailure) {
            t.addSuppressed(callbackFailure);
        }
        logger.error("{} worker failed", getClass().getSimpleName(), t);
    }
}
