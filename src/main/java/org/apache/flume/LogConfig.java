package org.apache.flume;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by zhangliming on 14-8-14.
 */
public class LogConfig {

    private String logFileName;
    private String pattern;
    private boolean alwaysIncludePattern;
    private long fetchInterval;

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

    public boolean isAlwaysIncludePattern() {
        return alwaysIncludePattern;
    }

    public String getLogFileName() {
        return logFileName;
    }

    public long getFetchInterval() {
        return fetchInterval;
    }

    public String getPattern() {
        return pattern;
    }

    public String getRealLogFile(){
        String logFileName=this.logFileName;
        if(alwaysIncludePattern){
            SimpleDateFormat sdf=new SimpleDateFormat(pattern);
            logFileName = logFileName + sdf.format(new Date());
        }
        return logFileName;
    }

    public void generateCursor() throws IOException{
        File logFile= new File(getRealLogFile());
        parentPath=logFile.getParentFile().toPath();

        cursor=new Cursor(new File(getRealLogFile()));
        cursor.setSleepTime(fetchInterval);
    }

    public Cursor getCursor() {
        return cursor;
    }

    public Path getParentPath() {
        return parentPath;
    }
}
