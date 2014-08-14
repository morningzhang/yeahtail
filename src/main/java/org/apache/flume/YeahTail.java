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
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import static java.nio.file.StandardWatchEventKinds.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class YeahTail extends AbstractSource
        implements EventDrivenSource, Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(YeahTail.class);

    final ConcurrentHashMap<String,LogConfig> pathToLogMap =new ConcurrentHashMap<String, LogConfig>();

    ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

    WatchService watcher;


    public YeahTail() {
        LOG.debug("YeahTail starting......");

        try{
           watcher= FileSystems.getDefault().newWatchService();
        }catch (IOException e){
            e.printStackTrace();
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
        long fetchInterval = context.getLong("fetchInterval", Long.valueOf(1000).longValue());


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
            config.getParentPath().register(watcher,ENTRY_CREATE,ENTRY_DELETE);
            pathToLogMap.put(config.getRealLogFile(), config);


        } catch (Throwable t) {
            String ss = Throwables.getStackTraceAsString(t);
            System.err.println("Throwable:"+ss);
            System.exit(1);
        }

    }

    public void start() {
        super.start();
        //start all cursors
        final ChannelProcessor cp=getChannelProcessor();
        for(LogConfig logConfig:pathToLogMap.values()){
            try {
                 logConfig.getCursor().start(new Cursor.ProcessCallBack() {
                     @Override
                     public void doCallBack(byte[] data) {
                         cp.processEvent(EventBuilder.withBody(data));
                     }
                 });
            } catch (Exception e) {
                e.printStackTrace();
            }
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
        for(LogConfig logConfig:pathToLogMap.values()){
            try {
                logConfig.getCursor().close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //stop watcher
        try {
            if(!singleThreadExecutor.isShutdown()){
                singleThreadExecutor.shutdown();
            }
            watcher.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void handleEvents(){
        while(true){
            try{
                WatchKey key = watcher.take();
                for(WatchEvent event : key.pollEvents()){
                    WatchEvent.Kind kind = event.kind();

                    if(kind == OVERFLOW){
                        continue;
                    }

                    WatchEvent<Path> e = (WatchEvent<Path>)event;

                    Path logFilePath = e.context();
                    String realLogFile=logFilePath.toFile().getPath();

                    if(kind==ENTRY_DELETE){
                        LogConfig logConfig=pathToLogMap.get(realLogFile);
                        if(logConfig!=null){
                            logConfig.getCursor().setDone(true);
                            pathToLogMap.remove(realLogFile);
                        }

                    }
                    else
                    if(kind==ENTRY_CREATE){
                       for(LogConfig logConfig:pathToLogMap.values()){
                           if(realLogFile.equals(logConfig.getRealLogFile())){

                           }

                       }
                    }
                }
                if(!key.reset()){
                    break;
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }



}