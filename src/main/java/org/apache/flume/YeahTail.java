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
    private LinkedBlockingQueue<Runnable> workQueue;
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
        nThreads= context.getInteger("poolSize", 1);
        //fetch interval
        fetchInterval = context.getInteger("fetchInterval", 100);

        Preconditions.checkArgument(logFileName != null, "Null File is an illegal argument");
        Preconditions.checkArgument(bufferSize > 0L, "bufferSize <=0 is an illegal argument");
        Preconditions.checkArgument(nThreads > 0L, "poolSize <=0 is an illegal argument");
        Preconditions.checkArgument(fetchInterval > 0L, "fetchInterval <=0 is an illegal argument");

        try {
            logConfig = new LogConfig(logFileName, bufferSize);
            logConfig.getParentPath().register(watcher, ENTRY_CREATE);

            //thread pools
            workQueue=new LinkedBlockingQueue<Runnable>(nThreads);
            runThreadPool = new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, workQueue);
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
                    try {

                        //submit collect log task to thread pool
                        for (final Cursor cursor : logConfig.getCursors()) {
                            //check workQueueSize is greater than threads poolSize
                            while(workQueue.size()>=nThreads){
                                //check the file create
                                handleLogFileCreate(Long.valueOf(fetchInterval));
                            }
                            //submit
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

                                        //check if read the file end and the file not update for 5 minutes and is not today file
                                        if (readFileLen == -1 && (lastModified + 300000) < nowTime && !isInOneDay(nowTime, lastModified, logConfig.getDateFormat())) {
                                            logConfig.removeOldLog(cursor);
                                            LOG.warn("The file {} is old, remove from cursors.", cursor.getLogFile());
                                        }
                                    } catch (IOException e) {
                                        if (!cursor.getLogFile().exists()) {
                                            logConfig.removeOldLog(cursor);
                                            LOG.warn("The file {} is not exist, remove from cursors.", cursor.getLogFile());
                                            return;
                                        }
                                    }

                                }
                            });

                        }
                        //check the file create
                        handleLogFileCreate(Long.valueOf(fetchInterval));

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


    private boolean isInOneDay(long nowTime, long lastModified, String fmt){
        SimpleDateFormat sdf=new SimpleDateFormat(fmt);
        return sdf.format(new Date(nowTime)).equals(sdf.format(new Date(lastModified)));
    }

    private void handleLogFileCreate(long waitTime) {
            try {
                WatchKey key = watcher.poll(waitTime,TimeUnit.MILLISECONDS);
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
                        LOG.info("The watcher find a new file {} is created. ", newFileName);

                        File newFile = new File(logConfig.getParentPath().toString() + "/" + newFileName);
                        if (logConfig.addNewLog(newFile)) {
                            LOG.info("The new file {} add to for collecting.", newFile.getAbsolutePath());
                        }else {
                            LOG.info("It is not the file we needed. ");
                        }


                    }
                }
            } catch (Exception e) {
                LOG.error("", e);
            }

    }

}