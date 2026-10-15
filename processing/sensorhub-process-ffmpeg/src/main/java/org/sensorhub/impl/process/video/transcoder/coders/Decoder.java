package org.sensorhub.impl.process.video.transcoder.coders;

import org.bytedeco.ffmpeg.avcodec.AVCodec;
import org.bytedeco.ffmpeg.avcodec.AVPacket;
import org.bytedeco.ffmpeg.avutil.AVFrame;
import org.bytedeco.javacpp.PointerPointer;

import java.util.HashMap;

import static org.bytedeco.ffmpeg.global.avcodec.*;
import static org.bytedeco.ffmpeg.global.avutil.*;

public class Decoder extends Coder<AVPacket, AVFrame> {
    public Decoder(int codecId, HashMap<String, Integer> options) {
        super(codecId, AVPacket.class, AVFrame.class, options);
    }

    @Override
    protected void initContext() {
        AVCodec codec = avcodec_find_decoder(codecId);
        if (codec == null || codec.isNull()) {
            throw new IllegalStateException("Could not find decoder for codec " + codecId);
        }
        codec_ctx = avcodec_alloc_context3(codec);
        if (codec_ctx == null || codec_ctx.isNull()) {
            throw new IllegalStateException("Could not allocate decoder context for " + codec.name().getString());
        }

        initOptions(codec_ctx);

        if (avcodec_open2(codec_ctx, codec, (PointerPointer<?>)null) < 0) {
            throw new IllegalStateException("Error initializing " + codec.name().getString() + " decoder");
        }
    }

    // Get compressed packet, send to decoder
    @Override
    protected void sendInPacket() {
        inPacket = inPackets.poll();
        if (inPacket == null) {
            return;
        }
        try {
            int ret = avcodec_send_packet(codec_ctx, inPacket);
            if (ret < 0) {
                throw new IllegalStateException("Decoder rejected input packet (FFmpeg error " + ret + ")");
            }
        } finally {
            // Polling transfers the queue's sole native ownership to this worker. libavcodec does
            // not retain the packet after avcodec_send_packet returns.
            av_packet_free(inPacket);
            inPacket = null;
        }
    }

    // Receive uncompressed frame from decoder
    @Override
    protected void receiveOutPacket() {
        int ret;
        while ((ret = avcodec_receive_frame(codec_ctx, outPacket)) >= 0) {
            AVFrame decodedFrame = av_frame_clone(outPacket);
            av_frame_unref(outPacket);
            if (decodedFrame == null || decodedFrame.isNull()) {
                throw new IllegalStateException("Could not clone decoded frame");
            }
            outPackets.add(decodedFrame);
        }
        if (ret != AVERROR_EAGAIN() && ret != AVERROR_EOF) {
            throw new IllegalStateException("Decoder could not receive a frame (FFmpeg error " + ret + ")");
        }
    }

    @Override
    protected void flush() {
        int ret;
        while ((ret = avcodec_send_packet(codec_ctx, (AVPacket) null)) == AVERROR_EAGAIN()) {
            receiveOutPacket();
        }
        if (ret < 0 && ret != AVERROR_EOF) {
            throw new IllegalStateException("Decoder could not start flushing (FFmpeg error " + ret + ")");
        }
        receiveOutPacket();
    }

    @Override
    protected void deallocateInputPacket(AVPacket packet) {
        av_packet_free(packet);
        packet = null;
    }

    @Override
    protected void deallocateOutputPacket(AVFrame packet) {
        av_frame_free(packet);
        packet = null;
    }

    @Override
    protected void deallocateOutQueue() {
        AVFrame frame;
        while ((frame = outPackets.poll()) != null) {
            av_frame_free(frame);
        }
    }

    @Override
    protected void allocatePackets() {
        outPacket = av_frame_alloc();
        if (outPacket == null || outPacket.isNull()) {
            throw new IllegalStateException("Could not allocate decoder output frame");
        }
    }

    @Override
    protected void deallocatePackets() {
        if (inPacket != null) {
            av_packet_free(inPacket);
            inPacket = null;
        }
        if (outPacket != null) {
            av_frame_free(outPacket);
            outPacket = null;
        }
        // Preserve successfully flushed frames for the downstream consumer. On interruption or
        // failure there is no complete output to deliver, so retain the historical cleanup path.
        if (!isOutputComplete()) {
            deallocateOutQueue();
        }
        AVPacket packet;
        while (inPackets != null && (packet = inPackets.poll()) != null) {
            av_packet_free(packet);
        }
    }
}
