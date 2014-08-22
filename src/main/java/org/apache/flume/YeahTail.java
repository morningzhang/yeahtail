package org.apache.flume;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.File;
import java.nio.file.*;

import static java.nio.file.StandardWatchEventKinds.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.*;

public class YeahTail extends AbstractSource
        implements EventDrivenSource, Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(YeahTail.class);

    private LogConfig logConfig ;

    private ExecutorService daemonThreadPool=Executors.newSingleThreadExecutor();
    private ExecutorService runThreadPool;
    private int nThreads = 0;
    private int fetchInterval=0;

    private WatchService watcher;

    private volatile boolean shutdown = false;


    public YeahTail() {
        LOG.info("YeahTail starting......");

        try {
            watcher = FileSystems.getDefault().newWatchService();
        } catch (IOException e) {
            LOG.error("", e);
        }

    }

    public void configure(Context context) {
        //log filename
        String logFileName = context.getString("logFileName");
        //buffer size
        int bufferSize = context.getInteger("bufferSize", 409600);
        //pool size
        int poolSize = context.getInteger("poolSize", 1);
        //fetch interval
        int fetchInterval = context.getInteger("fetchInterval", 100);

        Preconditions.checkArgument(logFileName != null, "Null File is an illegal argument");
        Preconditions.checkArgument(bufferSize > 0L, "bufferSize <=0 is an illegal argument");
        Preconditions.checkArgument(poolSize > 0L, "poolSize <=0 is an illegal argument");
        Preconditions.checkArgument(fetchInterval > 0L, "fetchInterval <=0 is an illegal argument");

        try {
            logConfig = new LogConfig(logFileName, bufferSize);
            logConfig.getParentPath().register(watcher, ENTRY_CREATE);

            //thread pools
            nThreads = poolSize;
            runThreadPool = new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(16));
            this.fetchInterval=fetchInterval;
        } catch (Throwable t) {
            String ss = Throwables.getStackTraceAsString(t);
            System.err.println(ss);
            System.exit(1);
        }

    }

    public void start() {
        super.start();

        final ChannelProcessor cp = getChannelProcessor();
        final LogConfig logConfig=this.logConfig;
        //start thread
        daemonThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                while (!shutdown) {
                    //submit collect log task to threadpool
                    try {
                        for (final Cursor cursor : logConfig.getCursors()) {
                            runThreadPool.execute(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        int readFileLen = cursor.process(new Cursor.ProcessCallBack() {
                                            @Override
                                            public void doCallBack(byte[] data) {
                                                cp.processEvent(EventBuilder.withBody(data));
                                            }
                                        });
                                        File logFile = cursor.getLogFile();
                                        long nowTime = System.currentTimeMillis();
                                        long lastModified = logFile.lastModified();

                                        //check if read the file end and the file not update for 10 minutes and is not today file
                                        if (readFileLen == -1 && (lastModified + 600000) < nowTime && !isInOneDay(nowTime, lastModified)) {
                                            logConfig.removeOldLog(cursor);
                                        }
                                    } catch (IOException e) {
                                        if (!cursor.getLogFile().exists()) {
                                            LOG.warn("the file {} is not exist, remove from cursors.", cursor.getLogFile());
                                            logConfig.removeOldLog(cursor);
                                            return;
                                        }
                                    }

                                    //check the file create
                                    handleLogFileCreate();

                                }
                            });

                        }
                        Thread.sleep(fetchInterval);
                    } catch (Exception e) {
                        LOG.error("", e);
                    }
                }
            }
        });



    }

    public void stop() {

        try {
            shutdown = true;
            //shutdown the thread
            if (!runThreadPool.isShutdown()) {
                runThreadPool.shutdown();
            }
            //shutdown the thread
            if (!daemonThreadPool.isShutdown()) {
                daemonThreadPool.shutdown();
            }
            //stop watcher
            watcher.close();
        } catch (Exception e) {
            LOG.error("", e);
        }

        super.stop();

    }


    private boolean isInOneDay(long nowTime, long lastModified){
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
        return sdf.format(new Date(nowTime)).equals(sdf.format(new Date(lastModified)));
    }

    private void handleLogFileCreate() {
            try {
                WatchKey key = watcher.poll();
                if(key==null){
                    return;
                }
                for (WatchEvent event : key.pollEvents()) {
                    WatchEvent.Kind kind = event.kind();
                    if (kind == OVERFLOW) {
                        continue;
                    }
                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> e = (WatchEvent<Path>) event;

                    String newFileName = e.context().toFile().getName();
                    //entry created and would not be a offset file
                    if (kind == ENTRY_CREATE && !newFileName.endsWith(".offset")) {
                        LOG.info("the new file {} is created. ", newFileName);

                        File newFile = new File(logConfig.getParentPath().toString() + "/" + newFileName);
                        if (logConfig.addNewLog(newFile)) {
                            LOG.info("the new file {} add to collect ", newFile.getAbsolutePath());
                            return;
                        }


                    }
                }
            } catch (Exception e) {
                LOG.error("", e);
            }

    }

}