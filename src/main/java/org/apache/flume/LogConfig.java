package org.apache.flume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;


public class LogConfig {

    private static final Logger LOG = LoggerFactory.getLogger(LogConfig.class);

    private String logFileName;
    private String pattern;
    private boolean alwaysIncludePattern;
    private long fetchInterval;
    private int bufferSize;

    private Cursor cursor;
    private Path parentPath;

    public void setAlwaysIncludePattern(boolean alwaysIncludePattern) {
        this.alwaysIncludePattern = alwaysIncludePattern;
    }

    public void setFetchInterval(long fetchInterval) {
        this.fetchInterval = fetchInterval;
    }

    public void setLogFileName(String logFileName) {
        this.logFileName = logFileName;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public String getLogFileName() {
        return logFileName;
    }

    public String getRealLogFile(){
        String logFileName=this.logFileName;
        if(alwaysIncludePattern){
            SimpleDateFormat sdf=new SimpleDateFormat(pattern);
            logFileName = logFileName + sdf.format(new Date());
        }

        return logFileName;
    }
    public boolean isAlwaysIncludePattern() {
        return alwaysIncludePattern;
    }

    public void generateCursor() throws IOException{
        File logFile= new File(getRealLogFile());
        LOG.info("the realLogFile is {} ",logFile.getAbsolutePath());

        parentPath=logFile.getParentFile().toPath();
        LOG.info("the parentPath is {} ",parentPath);

        cursor=new Cursor(logFile,bufferSize);
        cursor.setSleepTime(fetchInterval);
    }

    public Cursor getCursor() {
        return cursor;
    }

    public Path getParentPath() {
        return parentPath;
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof LogConfig)){
            return false;
        }
        if(!getLogFileName().equals(((LogConfig)obj).getLogFileName())){
            return false;
        }
        return true;
    }

}
