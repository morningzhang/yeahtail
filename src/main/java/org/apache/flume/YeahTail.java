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

import java.util.Set;
import java.util.concurrent.*;

public class YeahTail extends AbstractSource
        implements EventDrivenSource, Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(YeahTail.class);

    final Set<LogConfig> logs = new CopyOnWriteArraySet<LogConfig>();

    private ExecutorService daemonThread = Executors.newSingleThreadExecutor();
    private ExecutorService runThreadPool;
    private int nThreads = 0;
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
        //buffer size
        int poolSize = context.getInteger("poolSize", 1);

        Preconditions.checkArgument(logFileName != null, "Null File is an illegal argument");
        Preconditions.checkArgument(bufferSize > 0L, "bufferSize <=0 is an illegal argument");
        Preconditions.checkArgument(poolSize > 0L, "poolSize <=0 is an illegal argument");
        try {
            LogConfig config = new LogConfig(logFileName, bufferSize);
            config.setChannelProcessor(getChannelProcessor());
            config.getParentPath().register(watcher, ENTRY_CREATE);
            logs.add(config);
            //thread pools
            nThreads += poolSize;
            runThreadPool = new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(100));

        } catch (Throwable t) {
            String ss = Throwables.getStackTraceAsString(t);
            System.err.println(ss);
            System.exit(1);
        }

    }

    public void start() {
        super.start();
        //start thread
        daemonThread.submit(new Runnable() {
            @Override
            public void run() {
                while (!shutdown) {
                    //submit collect log task to threadpool
                    for (final LogConfig logConfig : logs) {
                        try {
                            final ChannelProcessor cp = logConfig.getChannelProcessor();
                            for (final Cursor cursor : logConfig.getCursors()) {
                                runThreadPool.submit(new Runnable() {
                                    @Override
                                    public void run() {
                                        try {
                                           int readFileLen= cursor.process(new Cursor.ProcessCallBack() {
                                                @Override
                                                public void doCallBack(byte[] data) {
                                                    cp.processEvent(EventBuilder.withBody(data));
                                                }
                                            });

                                           File logFile=cursor.getLogFile();
                                           long nowTime=System.currentTimeMillis();
                                           //check if read the file end and the file not update for 10 minutes and is not today file
                                            if(readFileLen==-1&&(logFile.lastModified()+600000)<nowTime&&logConfig.checkIsTodayLogFile(logFile)){
                                                logConfig.removeOldLog(cursor);
                                            }

                                        } catch (IOException e) {
                                            LOG.error("", e);
                                        }

                                    }
                                });
                            }
                            LOG.info("logfile {} started.", logConfig.getLogFileName());
                        } catch (Exception e) {
                            LOG.error("", e);
                        }
                    }
                    //check the file create
                    runThreadPool.submit(new Runnable() {
                        @Override
                        public void run() {
                            handleLogFileCreate();
                        }
                    });


                }
            }
        });


    }

    public void stop() {

        try {
            shutdown = true;
            //stop watcher
            watcher.close();

            //shutdown the thread
            if (!runThreadPool.isShutdown()) {
                runThreadPool.shutdown();
            }
            //shutdown the daemonThread
            if (!daemonThread.isShutdown()) {
                daemonThread.shutdown();
            }


        } catch (Exception e) {
            LOG.error("", e);
        }

        super.stop();

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

                    String newFileName = e.context().toFile().getPath();
                    //entry created and would not be a offset file
                    if (kind == ENTRY_CREATE && !newFileName.endsWith(".offset")) {
                        LOG.info("the new file {} is created. ", newFileName);

                        for (LogConfig logConfig : logs) {
                            boolean isAdded=logConfig.addNewLog(newFileName);
                            if(isAdded==true){
                                LOG.info("the new file {} add to collect ", newFileName);
                                return;
                            }

                        }

                    }
                }
            } catch (Exception e) {
                LOG.error("", e);
            }

    }

}