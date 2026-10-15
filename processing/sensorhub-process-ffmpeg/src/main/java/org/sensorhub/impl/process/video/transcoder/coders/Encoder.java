package org.sensorhub.impl.process.video.transcoder.coders;

import org.bytedeco.ffmpeg.avcodec.AVCodec;
import org.bytedeco.ffmpeg.avcodec.AVPacket;
import org.bytedeco.ffmpeg.avutil.AVFrame;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.PointerPointer;

import java.util.HashMap;

import static org.bytedeco.ffmpeg.global.avcodec.*;
import static org.bytedeco.ffmpeg.global.avutil.*;

public class Encoder extends Coder<AVFrame, AVPacket> {
    long pts = 0;

    public Encoder(int codecId, HashMap<String, Integer> options) {
        super(codecId, AVFrame.class, AVPacket.class, options);
    }

    @Override
    protected void deallocateOutQueue() {
        AVPacket packet;
        while ((packet = outPackets.poll()) != null) {
            av_packet_free(packet);
        }
    }

    @Override
    protected void initContext() {
        pts = 0;
        AVCodec codec = avcodec_find_encoder(codecId);
        if (codec == null || codec.isNull()) {
            throw new IllegalStateException("Could not find encoder for codec " + codecId);
        }
        codec_ctx = avcodec_alloc_context3(codec);
        if (codec_ctx == null || codec_ctx.isNull()) {
            throw new IllegalStateException("Could not allocate encoder context for " + codec.name().getString());
        }

        initOptions(codec_ctx);
        int ret = avcodec_open2(codec_ctx, codec, (PointerPointer<?>)null);
        if (ret < 0) {
            try (BytePointer errorBuffer = new BytePointer(AV_ERROR_MAX_STRING_SIZE)) {
                av_strerror(ret, errorBuffer, AV_ERROR_MAX_STRING_SIZE);
                logger.debug("Receive Error: {}", errorBuffer.getString());
            }
            throw new IllegalStateException("Error initializing " + codec.name().getString() + " encoder");
        }
    }

    @Override
    protected void sendInPacket(AVFrame frame) {
        inPacket = frame;
        if (frame == null) {
            return;
        }
        try {
            // Decoder timestamps are expressed in the source stream time base. This encoder emits
            // constant-frame-rate H.264, so assign timestamps in codec_ctx.time_base explicitly
            // rather than interpreting source ticks as encoder ticks.
            inPacket.pts(pts++);
            int ret = avcodec_send_frame(codec_ctx, inPacket);
            if (ret < 0) {
                throw new IllegalStateException("Encoder rejected input frame (FFmpeg error " + ret + ")");
            }
        } finally {
            // Polling transfers the queue's sole native ownership to this worker. libavcodec does
            // not retain the frame after avcodec_send_frame returns.
            av_frame_free(inPacket);
            inPacket = null;
        }
    }

    @Override
    protected void receiveOutPacket() {
        int ret;
        while ((ret = avcodec_receive_packet(codec_ctx, outPacket)) >= 0) {
            AVPacket encodedPacket = av_packet_clone(outPacket);
            av_packet_unref(outPacket);
            if (encodedPacket == null || encodedPacket.isNull()) {
                throw new IllegalStateException("Could not clone encoded packet");
            }
            // AVPacket.time_base is not guaranteed to be populated by every encoder. Downstream
            // remuxers need the exact units used by the PTS/DTS above.
            encodedPacket.time_base(codec_ctx.time_base());
            outPackets.add(encodedPacket);
        }
        if (ret != AVERROR_EAGAIN() && ret != AVERROR_EOF) {
            throw new IllegalStateException("Encoder could not receive a packet (FFmpeg error " + ret + ")");
        }
    }

    @Override
    protected void flush() {
        int ret;
        while ((ret = avcodec_send_frame(codec_ctx, (AVFrame) null)) == AVERROR_EAGAIN()) {
            receiveOutPacket();
        }
        if (ret < 0 && ret != AVERROR_EOF) {
            throw new IllegalStateException("Encoder could not start flushing (FFmpeg error " + ret + ")");
        }
        receiveOutPacket();
    }

    @Override
    protected void deallocateInputPacket(AVFrame packet) {
        av_frame_free(packet);
        packet = null;
    }

    @Override
    protected void deallocateOutputPacket(AVPacket packet) {
        av_packet_free(packet);
        packet = null;
    }

    @Override
    protected void allocatePackets() {
        outPacket = av_packet_alloc();
        if (outPacket == null || outPacket.isNull()) {
            throw new IllegalStateException("Could not allocate encoder output packet");
        }
    }

    @Override
    protected void deallocatePackets() {
        if (inPacket != null) {
            av_frame_free(inPacket);
            inPacket = null;
        }
        if (outPacket != null) {
            av_packet_free(outPacket);
            outPacket = null;
        }
        // Preserve successfully flushed packets for the downstream consumer. On interruption or
        // failure there is no complete output to deliver, so retain the historical cleanup path.
        if (!isOutputComplete()) {
            deallocateOutQueue();
        }
        AVFrame frame;
        while (inPackets != null && (frame = inPackets.poll()) != null) {
            av_frame_free(frame);
        }
    }
}
