package org.apache.flume;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

public class YeahTail extends AbstractSource implements EventDrivenSource, Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(YeahTail.class);

    private LogConfig logConfig ;

    private ExecutorService runThreadPool=Executors.newSingleThreadExecutor();

    private int fetchInterval=0;
    private volatile double weighing=1.0;

    private WatchService watcher;

    private volatile boolean shutdown = false;

    public YeahTail() {
        LOG.info("YeahTail Starting......");
        watcher =newWatchService();
    }

    public void configure(Context context) {
        //log filename
        String logFileName = context.getString("logFileName");
        //buffer size
        int bufferSize = context.getInteger("bufferSize", 409600);
        //fetch interval
        fetchInterval = context.getInteger("fetchInterval", 100);

        Preconditions.checkArgument(logFileName != null, "Null File is an illegal argument");
        Preconditions.checkArgument(bufferSize > 0L, "bufferSize <=0 is an illegal argument");
        Preconditions.checkArgument(fetchInterval > 0L, "fetchInterval <=0 is an illegal argument");

        try {
            logConfig = new LogConfig(logFileName, bufferSize);
            logConfig.getParentPath().register(watcher, ENTRY_CREATE, ENTRY_MODIFY);
        } catch (Throwable t) {
            String ss = Throwables.getStackTraceAsString(t);
            System.err.println(ss);
            System.exit(1);
        }

    }

    public void start() {
        final ChannelProcessor cp = getChannelProcessor();
        final LogConfig logConfig=this.logConfig;
        //start thread
        runThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                while (!shutdown) {
                    try {
                        //submit collect log task to thread pool
                        for (Cursor cursor : logConfig.getCursors()) {
                            try {
                                int readFileLen=0;
                                for(int i=0;i<3;i++){
                                    readFileLen = cursor.process(new Cursor.ProcessCallBack() {
                                        @Override
                                        public void doCallBack(byte[] data) {
                                            cp.processEvent(EventBuilder.withBody(data));
                                        }
                                    });
                                    if(readFileLen<logConfig.getBufferSize()){
                                        break;
                                    }
                                }
                                File logFile = cursor.getLogFile();
                                long nowTime = System.currentTimeMillis();
                                long lastModified = logFile.lastModified();

                                //check if read the file end and the file not update for 5 minutes and is not today file
                                if (readFileLen == -1 && (lastModified + 300000) < nowTime && !isInOneDay(nowTime, lastModified, logConfig.getDateFormat())) {
                                    logConfig.removeOldLog(cursor);
                                    LOG.warn("The file {} is old, remove from cursors.", cursor.getLogFile());
                                }
                                //dynamic weighing
                                long fileLen=logFile.length();
                                if(fileLen>0){
                                    double readRate=cursor.getOffset().getCurrentValue()/fileLen;
                                    if(readRate<0.95){
                                        weighing=weighing*0.5;
                                    }else if(readRate>=0.99){
                                        double tmp=weighing*1.2;
                                        weighing=tmp>1.0?1.0:tmp;
                                    }
                                }

                            } catch (IOException e) {
                                //if log is beging rename
                                if (!cursor.getLogFile().exists()) {
                                    //await 100ms
                                    try {
                                        Thread.sleep(100);
                                    } catch (InterruptedException e1) {

                                    }
                                    //check again
                                    if (!cursor.getLogFile().exists()) {
                                        logConfig.removeOldLog(cursor);
                                        LOG.warn("The file {} is not exist, remove from cursors.", cursor.getLogFile());
                                    }
                                } else {
                                    logConfig.removeOldLog(cursor);
                                    LOG.warn("The file {} is closed, remove from cursors.", cursor.getLogFile());
                                }
                            }
                        }
                        //check file changed
                        handleLogFileChanged((long) (fetchInterval * weighing));

                    } catch (Exception e) {
                        LOG.error("", e);
                    }
                }
            }
        });

        super.start();

    }

    public void stop() {

        try {
            shutdown = true;
            //shutdown the thread
            if (!runThreadPool.isShutdown()) {
                runThreadPool.shutdown();
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

    private void handleLogFileChanged(long waitTime) {
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
                    if (kind == ENTRY_CREATE || kind == ENTRY_MODIFY) {
                        @SuppressWarnings("unchecked")
                        WatchEvent<Path> e = (WatchEvent<Path>) event;

                        String modifiedFileName = e.context().toFile().getName();
                        //entry created and would not be a offset file
                        if (!modifiedFileName.endsWith(".offset")) {
                            Date today = new Date();
                            String dateFormatStr=new SimpleDateFormat(logConfig.getDateFormat()).format(today);
                            File modifiedFile = new File(logConfig.getParentPath().toString() + "/" + modifiedFileName);
                            if (logConfig.isMatchLog(modifiedFile, today)) {
                                logConfig.addLog2Collect(dateFormatStr, modifiedFile);
                            }
                        }
                    }

                }
                key.reset();
            } catch (Exception e) {
                LOG.error("", e);
            }

    }

    private WatchService newWatchService(){
        try{
           return FileSystems.getDefault().newWatchService();
        }catch (IOException e){
            LOG.error("", e);
        }
        return null;
    }

}