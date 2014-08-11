package org.apache.flume;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cursor {
    private static final Logger LOG = LoggerFactory.getLogger(Cursor.class);

    final File file;
    final ByteBuffer buf = ByteBuffer.allocateDirect(2097152);

    RandomAccessFile raf = null;

    FileChannel in = null;
    private ChannelProcessor channelProcessor;

    public long lastFileMod;
    public long lastChannelPos;
    public long lastChannelSize;
    public int readFailures;
    public long allBufNum = 0L;


    Cursor(File f, long lastReadOffset, long lastFileLen, long lastMod) {
        this.file = f;
        this.lastChannelPos = lastReadOffset;
        this.lastChannelSize = lastFileLen;
        this.lastFileMod = lastMod;
        this.readFailures = 0;
    }

    public long getLastFileMod() {
        return this.lastFileMod;
    }

    public long getLastChannelPos() {
        return this.lastChannelPos;
    }

    public long getLastChannelSize() {
        return this.lastChannelSize;
    }

    public void setLastChannelPos(long pos) {
        this.lastChannelPos = pos;
    }

    public void setLastChannelSize(long pos) {
        this.lastChannelSize = pos;
    }

    public ChannelProcessor getChannelProcessor() {
        return this.channelProcessor;
    }

    public void setChannelProcessor(ChannelProcessor channelProcessor) {
        this.channelProcessor = channelProcessor;
    }

    void initCursorPos()
            throws InterruptedException {
        if (this.raf != null) {
            try {
                this.raf.close();
            } catch (IOException e) {
                LOG.error("problem closing file " + e.getMessage(), e);
            }
        }
        try {
            LOG.debug("initCursorPos " + this.file);
            this.raf = new RandomAccessFile(this.file, "r");
            this.raf.seek(this.lastChannelPos);
            this.in = this.raf.getChannel();
        } catch (FileNotFoundException e) {
            resetRAF();
        } catch (IOException e) {
            resetRAF();
        }
    }

    void flush()
            throws InterruptedException {
        LOG.debug("raf:" + this.raf);
        if (this.raf != null) {
            try {
                this.raf.close();
            } catch (IOException e) {
                LOG.error("problem closing file " + e.getMessage(), e);
            }
        }

        this.buf.flip();
        int remaining = this.buf.remaining();
        if (remaining > 0) {
            byte[] body = new byte[remaining];
            this.buf.get(body, 0, remaining);
            Event e = EventBuilder.withBody(body);
            this.channelProcessor.processEvent(e);
            this.lastChannelPos += body.length;
        }
        this.in = null;
        this.buf.clear();
    }

    void close() {
        if (this.raf != null) {
            try {
                this.raf.close();
            } catch (IOException e) {
                LOG.error("problem closing file " + e.getMessage(), e);
            }
        }

        this.in = null;
        this.buf.clear();
    }

    void resetRAF()
            throws InterruptedException {
        LOG.debug("reseting cursor");
        flush();
        this.lastChannelPos = 0L;
        this.lastFileMod = 0L;
        this.readFailures = 0;
    }

    boolean checkForUpdates()
            throws IOException {
        LOG.debug("tail " + this.file + " : recheck");
        if (this.file.isDirectory()) {
            IOException ioe = new IOException("Tail expects a file '" + this.file + "', but it is a dir!");

            LOG.error(ioe.getMessage());
            throw ioe;
        }

        if (!this.file.exists()) {
            LOG.debug("Tail '" + this.file + "': nothing to do, waiting for a file");
            return false;
        }

        if (!this.file.canRead()) {
            throw new IOException("Permission denied on " + this.file);
        }

        try {
            if (this.raf != null) {
                try {
                    this.raf.close();
                } catch (IOException e) {
                    LOG.error("problem closing file " + e.getMessage(), e);
                }
            }
            this.raf = new RandomAccessFile(this.file, "r");
            this.lastFileMod = this.file.lastModified();
            this.in = this.raf.getChannel();

            LOG.debug("###lastChannelPos=" + this.lastChannelPos);

            if (this.lastChannelPos > 0L)
                this.raf.seek(this.lastChannelPos);
            else {
                this.lastChannelPos = 0L;
            }

            this.lastChannelSize = this.in.size();

            LOG.debug("Tail '" + this.file + "': opened last mod=" + this.lastFileMod + " lastChannelPos=" + this.lastChannelPos + " lastChannelSize=" + this.lastChannelSize);

            return true;
        } catch (FileNotFoundException fnfe) {
            LOG.debug("Tail '" + this.file + "': a file existed then disappeared, odd but continue");
        }
        return false;
    }

    boolean extractLines(ByteBuffer buf) throws IOException, InterruptedException {
        boolean madeProgress = false;
        int start = buf.position();
        buf.mark();

        while (buf.hasRemaining()) {
            byte b = buf.get();

            if (b == 10) {
                int end = buf.position();
                int sz = end - start;
                byte[] body = new byte[sz - 1];
                buf.reset();
                buf.get(body, 0, sz - 1);
                buf.get();
                buf.mark();
                start = buf.position();
                Event e = EventBuilder.withBody(body);
                this.lastChannelPos += body.length + 1;
                this.channelProcessor.processEvent(e);
                madeProgress = true;
            }

        }

        if (buf.remaining() == 0) {
            this.allBufNum += 1L;
            handleBufFull();
            madeProgress = true;
        }

        buf.reset();
        buf.compact();
        return madeProgress;
    }

    void handleBufFull() throws IOException, InterruptedException {
        int allBuf = this.buf.limit() - this.buf.position();
        if (allBuf == 0) {
            return;
        }
        byte[] body = new byte[allBuf];

        Event e = EventBuilder.withBody(body);
        this.channelProcessor.processEvent(e);
        this.lastChannelPos += body.length;
    }

    public boolean tailBody()
            throws InterruptedException {
        try {
            if (this.in == null) {
                LOG.debug("tail " + this.file + " : cur file is null");
                return checkForUpdates();
            }

            long chlen = this.in.size();
            boolean madeProgress = readAllFromChannel();

            if (madeProgress) {
                this.lastChannelSize = this.lastChannelPos;

                this.lastFileMod = this.file.lastModified();
                LOG.debug("tail " + this.file + " : new data found");
                return true;
            }

            long fmod = this.file.lastModified();
            long flen = this.file.length();

            if ((flen == this.lastChannelSize) && (fmod == this.lastFileMod)) {
                LOG.debug("tail " + this.file + " : no change");
                return false;
            }

            LOG.debug("tail " + this.file + " : file rotated?");

            if ((flen == this.lastChannelSize) && (fmod != this.lastFileMod)) {
                LOG.debug("tail " + this.file + " : file rotated with new one with " + "same length?");

                this.raf.getFD().sync();
                Thread.sleep(1000L);
            }

            if (this.in.size() != chlen) {
                LOG.debug("tail " + this.file + " : there's extra data to be read from " + "file, aborting file rotation handling");

                return true;
            }

            if (chlen < this.lastChannelSize) {
                LOG.debug("tail " + this.file + " : file was truncated, " + "aborting file rotation handling");

                this.lastChannelSize = chlen;
                this.lastChannelPos = chlen;
                this.lastFileMod = this.file.lastModified();
                this.in.position(chlen);

                return false;
            }

            LOG.debug("tail " + this.file + " : file rotated!");
            resetRAF();

            return flen > 0L;
        } catch (IOException e) {
            LOG.debug(e.getMessage(), e);
            this.in = null;
            this.readFailures += 1;

            if (this.readFailures > 3) {
                LOG.warn("Encountered " + this.readFailures + " failures on " + this.file.getAbsolutePath() + " - sleeping");

                return false;
            }
        }
        return true;
    }

    private boolean readAllFromChannel()
            throws IOException, InterruptedException {
        boolean madeProgress = false;

        int rd = this.in.read(this.buf);

        while (rd > 0) {
            madeProgress = true;

            int lastRd = 0;
            boolean progress = false;
            do {
                if ((lastRd == -1) && (rd == -1)) {
                    return true;
                }

                this.buf.flip();

                extractLines(this.buf);

                lastRd = rd;
            } while (progress);

            LOG.debug("###buf remaining size=" + this.buf.remaining());
            if (this.buf.remaining() == 0) {
                LOG.debug("###buf remaining clear");
            this.buf.position(0);
        }

            rd = this.in.read(this.buf);
            LOG.debug("###custom cursor rd=" + rd);
        }

        LOG.debug("tail " + this.file + ": last read position " + this.lastChannelPos + ", madeProgress: " + madeProgress);

        return madeProgress;
    }
}