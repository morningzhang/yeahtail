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
import java.nio.file.*;

import static java.nio.file.StandardWatchEventKinds.*;

import java.util.Set;
import java.util.concurrent.*;

public class YeahTail extends AbstractSource
        implements EventDrivenSource, Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(YeahTail.class);

    final Set<LogConfig> logs =new CopyOnWriteArraySet<LogConfig>();

    ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

    WatchService watcher;


    public YeahTail() {
        LOG.info("YeahTail starting......");

        try{
           watcher= FileSystems.getDefault().newWatchService();
        }catch (IOException e){
            LOG.error("",e);
        }

    }

    public void configure(Context context) {
        //log filename
        String logFileName = context.getString("logFileName");
        //date pattern for log
        String pattern = context.getString("pattern");
        //the log file always have date pattern
        boolean alwaysIncludePattern = context.getBoolean("alwaysIncludePattern", false);
        //fetch interval
        long fetchInterval = context.getLong("fetchInterval",1000L);


        Preconditions.checkArgument(logFileName != null, "Null File is an illegal argument");
        Preconditions.checkArgument(pattern != null&&pattern.length()>0, "Null or blank pattern is an illegal argument");

        Preconditions.checkArgument(fetchInterval > 0L, "fetchInterval <=0 is an illegal argument");

        try {

            LogConfig config=new LogConfig();
            config.setAlwaysIncludePattern(alwaysIncludePattern);
            config.setFetchInterval(fetchInterval);
            config.setLogFileName(logFileName);
            config.setPattern(pattern);

            config.generateCursor();
            config.getParentPath().register(watcher,ENTRY_CREATE);
            logs.add(config);

        } catch (Throwable t) {
            String ss = Throwables.getStackTraceAsString(t);
            System.err.println(ss);
            System.exit(1);
        }

    }

    public void start() {

        //start all cursors
        final ChannelProcessor cp=getChannelProcessor();
        for(LogConfig logConfig: logs){
            try {
                 logConfig.getCursor().start(new Cursor.ProcessCallBack() {
                     @Override
                     public void doCallBack(byte[] data) {
                         cp.processEvent(EventBuilder.withBody(data));
                     }
                 });
                LOG.info("logfile {} started.",logConfig.getCursor().getLogFile().getName());
            } catch (Exception e) {
                LOG.error("", e);
            }

            super.start();
        }
        //start thread
        singleThreadExecutor.submit(new Runnable() {
            @Override
            public void run() {
                handleEvents();
            }
        });

    }

    public void stop() {
        super.stop();
        //stop
        for(LogConfig logConfig: logs){
            try {
                logConfig.getCursor().close();
            } catch (Exception e) {
                LOG.error("", e);
            }
        }
        //stop watcher
        try {
            watcher.close();
            //shutdown the thread
            if(!singleThreadExecutor.isShutdown()){
                singleThreadExecutor.shutdown();
            }
        }catch (Exception e){
            LOG.error("", e);
        }

    }

    private void handleEvents(){
        while(true){
            try {
                WatchKey key = watcher.take();
                for (WatchEvent event : key.pollEvents()) {
                    WatchEvent.Kind kind = event.kind();
                    if (kind == OVERFLOW) {
                        continue;
                    }
                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> e = (WatchEvent<Path>) event;

                    Path logFilePath = e.context();
                    String realLogFile = logFilePath.toFile().getPath();
                    if (kind == ENTRY_CREATE) {
                        LOG.info("the new logfile {} is created. ", realLogFile);
                        for (LogConfig logConfig : logs) {
                            //the new date logfile
                            if (realLogFile.equals(logConfig.getRealLogFile())) {
                                LOG.info("matched for the old logfile {}. ", logConfig.getCursor().getLogFile().getName());
                                logConfig.getCursor().setDone(true);
                                logConfig.generateCursor();
                            }
                        }
                    }
                }
            }catch (ClosedWatchServiceException cwse){
                break;
            }catch (Exception e){
                LOG.error("", e);
            }

        }
    }



}