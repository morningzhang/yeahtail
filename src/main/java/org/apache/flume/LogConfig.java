package org.apache.flume;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Date;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.channel.ChannelProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogConfig {

    private static final Logger LOG = LoggerFactory.getLogger(LogConfig.class);
    //传入的日志文件名称
    private String logFileName;
    //传入的缓存大小
    private int bufferSize;
    //channel
    ChannelProcessor channelProcessor;

    //----------------------------
    private Path parentPath;
    private String logPattern;
    private String dateFormat;
    private List<Cursor> cursors=new CopyOnWriteArrayList<Cursor>();


    LogConfig(String logFileName,int bufferSize){
        setBufferSize(bufferSize);
        setLogFileName(logFileName);
    }

    public boolean addNewLog(String fileName) {
        String todayLogPattern = getDateLogPattern(this.logPattern, this.dateFormat, new Date());
        File newLogFile = new File(fileName);
        if (newLogFile.getName().matches(todayLogPattern)) {
            addNewCursor(newLogFile);
            return true;
        }
        return false;
    }

    public boolean removeOldLog(Cursor cursor){
        try {
            cursor.close();
        }catch (IOException e){
            LOG.error("", e);
            return false;
        }
        return cursors.remove(cursor);
    }

    public boolean checkIsTodayLogFile(File needCheckFile){
        String todayLogPattern= getDateLogPattern(this.logPattern, this.dateFormat, new Date());
        File[] logFiles=getLogFilesInParent(todayLogPattern);
        if(logFiles!=null){
            for(File realLogFile:logFiles){
               if(realLogFile.equals(needCheckFile)){
                   return true;
               }
            }
        }
        return false;
    }

    public void setChannelProcessor(ChannelProcessor channelProcessor) {
        this.channelProcessor = channelProcessor;
    }

    public ChannelProcessor getChannelProcessor() {
        return channelProcessor;
    }

    public List<Cursor> getCursors(){
        return cursors;
    }

    public String getLogFileName() {
        return logFileName;
    }

    public Path getParentPath() {
        return parentPath;
    }


    private void setLogFileName(String logFileName) {
        this.logFileName = logFileName;

        File logFile= new File(logFileName);
        this.logPattern=logFile.getName();
        this.parentPath=logFile.getParentFile().toPath();

        this.dateFormat=getDateFormatStr(this.logPattern);
        //get today log files
        String todayLogPattern= getDateLogPattern(this.logPattern, this.dateFormat, new Date());
        File[] logFiles=getLogFilesInParent(todayLogPattern);
        if(logFiles!=null){
            for(File realLogFile:logFiles){
                addNewCursor(realLogFile);
                LOG.info("Add File {} to cursors. ",realLogFile.getAbsolutePath());
            }
        }

    }

    private void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }


    private void addNewCursor(File realLogFile){
        try {
            Cursor cursor=new Cursor(realLogFile,this.bufferSize);
            cursors.add(cursor);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private String getDateFormatStr(String logPattern){
        Matcher m = Pattern.compile(".*?\\$\\{(.*?)\\}.*").matcher(logPattern);
        if(m.matches()) {
            return m.group(1);
        }
        return null;
    }

    private String getDateLogPattern(String logPattern, String dateFormat, Date date){
        return logPattern.replaceAll("\\$\\{.*?\\}",new SimpleDateFormat(dateFormat).format(date));
    }

    /**
     *列出所有的符合正则的文件。初始化的时候用的。
     * @return
     */
    private File[] getLogFilesInParent(final String logPattern) {
        return this.getParentPath().toFile().listFiles(new FileFilter() {
            public boolean accept(File file) {
                if (file.getName().matches(logPattern)) {
                    return true;
                }
                return false;
            }
        });
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
