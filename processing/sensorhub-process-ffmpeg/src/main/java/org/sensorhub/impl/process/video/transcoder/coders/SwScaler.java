package org.sensorhub.impl.process.video.transcoder.coders;

import org.bytedeco.ffmpeg.avutil.AVFrame;
import org.bytedeco.ffmpeg.swscale.SwsContext;
import org.bytedeco.javacpp.DoublePointer;

import java.util.HashMap;

import static org.bytedeco.ffmpeg.global.avcodec.*;
import static org.bytedeco.ffmpeg.global.avutil.*;
import static org.bytedeco.ffmpeg.global.swscale.*;

public class SwScaler extends Coder<AVFrame, AVFrame> {
    long pts = 0;
    SwsContext swsContext;
    final int inPixFmt, outPixFmt, inWidth, inHeight, outWidth, outHeight;

    public SwScaler(int inPixFmt, int outPixFmt, int inWidth, int inHeight, int outWidth, int outHeight) {
        super(0, AVFrame.class, AVFrame.class, new HashMap<String, Integer>());
        this.inPixFmt = inPixFmt;
        this.outPixFmt = outPixFmt;
        this.inWidth = inWidth;
        this.inHeight = inHeight;
        this.outWidth = outWidth;
        this.outHeight = outHeight;
    }

    @Override
    protected void deallocateOutQueue() {
        AVFrame frame;
        while ((frame = outPackets.poll()) != null) {
            av_frame_free(frame);
        }
    }

    @Override
    protected void initContext() {
        pts = 0;
        swsContext = sws_getContext(inWidth, inHeight, inPixFmt,
                outWidth, outHeight, outPixFmt,
                SWS_BICUBIC, null, null, (DoublePointer) null);
        if (swsContext == null || swsContext.isNull()) {
            throw new IllegalStateException("Could not initialize scaler context");
        }
    }

    @Override
    protected void sendInPacket(AVFrame frame) {
        inPacket = frame;
        if (frame == null || outPacket == null) {
            return;
        }
        try {
            int ret = sws_scale_frame(swsContext, outPacket, frame);
            if (ret < 0) {
                throw new IllegalStateException("Scaler rejected input frame (FFmpeg error " + ret + ")");
            }
        } finally {
            // Polling transfers sole ownership of the input frame to this worker.
            av_frame_free(inPacket);
            inPacket = null;
        }
    }

    @Override
    protected void receiveOutPacket() {
        // We already have outPacket at this point (one method needed for scaling rather than two)
        // Just use this to add the output to the out queue and make the buffers writable
        AVFrame scaledFrame = av_frame_clone(outPacket);
        if (scaledFrame == null || scaledFrame.isNull()) {
            throw new IllegalStateException("Could not clone scaled frame");
        }
        outPackets.add(scaledFrame);
        int ret = av_frame_make_writable(outPacket);
        if (ret < 0) {
            throw new IllegalStateException("Could not make scaler output writable (FFmpeg error " + ret + ")");
        }
    }

    @Override
    protected void deallocateInputPacket(AVFrame packet) {
        av_frame_free(packet);
        packet = null;
    }

    @Override
    protected void deallocateOutputPacket(AVFrame packet) {
        av_frame_free(packet);
        packet = null;
    }

    @Override
    protected void allocatePackets() {
        outPacket = av_frame_alloc();
        if (outPacket == null || outPacket.isNull()) {
            throw new IllegalStateException("Could not allocate scaler output frame");
        }
        outPacket.format(outPixFmt);
        outPacket.width(outWidth);
        outPacket.height(outHeight);
        int ret = av_frame_get_buffer(outPacket, 32);
        if (ret < 0) {
            throw new IllegalStateException("Could not allocate scaler output buffer (FFmpeg error " + ret + ")");
        }
    }

    @Override
    protected void deallocatePackets() {
        if (swsContext != null) {
            sws_freeContext(swsContext);
            swsContext = null;
        }
        if (inPacket != null) {
            av_frame_free(inPacket);
            inPacket = null;
        }
        if (outPacket != null) {
            av_frame_free(outPacket);
            outPacket = null;
        }
        if (!isOutputComplete()) {
            deallocateOutQueue();
        }
        AVFrame frame;
        while (inPackets != null && (frame = inPackets.poll()) != null) {
            av_frame_free(frame);
        }
    }
}
