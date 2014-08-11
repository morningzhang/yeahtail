package org.apache.flume;

import com.google.common.base.Preconditions;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class YeahTail extends AbstractSource
        implements EventDrivenSource, Configurable
{
    private static final Logger LOG = LoggerFactory.getLogger(YeahTail.class);

    private ChannelProcessor channelProcessor;
    private volatile boolean done = false;

    private long sleepTime = 1000L;
    final List<Cursor> cursors = new ArrayList<Cursor>();
    private final List<Cursor> newCursors = new ArrayList<Cursor>();
    private final List<Cursor> rmCursors = new ArrayList<Cursor>();
    private TailThread thd = null;
    private File file;
    private Cursor c;

    public YeahTail()
    {
        LOG.debug("YeahTail starting......");
    }

    public void configure(Context context)
    {
        this.sleepTime = context.getInteger("waitTime", Integer.valueOf(1000)).intValue();
        String fileName = context.getString("file");

        Preconditions.checkArgument(fileName != null, "Null File is an illegal argument");

        Preconditions.checkArgument(this.sleepTime > 0L, "waitTime <=0 is an illegal argument");

        this.file = new File(fileName);

        long fileLen = this.file.length();
        boolean startFromEnd = context.getBoolean("startFromEnd", Boolean.valueOf(false)).booleanValue();
        long offset = context.getLong("offset", Long.valueOf(0L)).longValue();
        long readOffset = startFromEnd ? fileLen : offset;
        long modTime = this.file.lastModified();
        this.c = new Cursor(this.file, readOffset, fileLen, modTime);
        addCursor(this.c);
    }

    public void start()
    {
        if (this.thd != null) {
            throw new IllegalStateException("Attempted to open tail source twice!");
        }

        this.thd = new TailThread();
        this.thd.start();
        this.channelProcessor = getChannelProcessor();
        this.c.setChannelProcessor(this.channelProcessor);
    }

    public void stop()
    {
        try {
            synchronized (this) {
                this.done = true;
                if (this.thd == null) {
                    LOG.warn("YeahTail double closed");
                    return;
                }
                while (this.thd.isAlive()) {
                    this.thd.join(100L);
                    this.thd.interrupt();
                }
                this.thd = null;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    synchronized void addCursor(Cursor cursor)
    {
        Preconditions.checkArgument(cursor != null);

        if (this.thd == null) {
            this.cursors.add(cursor);
            LOG.info("Unstarted Tail has added cursor: " + cursor.file);
        }
        else {
            synchronized (this.newCursors) {
                this.newCursors.add(cursor);
            }
            LOG.info("Tail added new cursor to new cursor list: " + cursor.file);
        }
    }

    public synchronized void removeCursor(Cursor cursor)
    {
        Preconditions.checkArgument(cursor != null);

        LOG.info("Tail added rm cursor to rmCursors: " + cursor.file);

        if (this.thd == null) {
            this.cursors.remove(cursor);
        }
        else
            synchronized (this.rmCursors) {
                this.rmCursors.add(cursor);
            }
    }

    class TailThread extends Thread
    {
        TailThread()
        {
            super();
        }

        public void run()
        {
            try
            {
                for (Cursor c : YeahTail.this.cursors) {
                    c.initCursorPos();
                }

                while (!YeahTail.this.done) {
                    synchronized (YeahTail.this.newCursors) {
                        YeahTail.this.cursors.addAll(YeahTail.this.newCursors);
                        YeahTail.this.newCursors.clear();
                    }

                    synchronized (YeahTail.this.rmCursors) {
                        YeahTail.this.cursors.removeAll(YeahTail.this.rmCursors);
                        for (Cursor c : YeahTail.this.rmCursors) {
                            c.flush();
                        }
                        YeahTail.this.rmCursors.clear();
                    }

                    boolean madeProgress = false;
                    for (Cursor c : YeahTail.this.cursors) {
                        YeahTail.LOG.debug("Progress loop: " + c.file);
                        if (c.tailBody()) {
                            madeProgress = true;
                        }
                    }

                    if (!madeProgress) {
                        Clock.sleep(YeahTail.this.sleepTime);
                    }
                }
                YeahTail.LOG.debug("Tail got done flag");
            } catch (InterruptedException e) {
                YeahTail.LOG.error("Tail thread nterrupted: " + e.getMessage(), e);
            } finally {
                YeahTail.LOG.info("TailThread has exited");
            }
        }
    }
}