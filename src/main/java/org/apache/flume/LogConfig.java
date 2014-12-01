package org.apache.flume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogConfig {

    private static final Logger LOG = LoggerFactory.getLogger(LogConfig.class);
    //传入的日志文件名称
    private String logFileName;
    //传入的缓存大小
    private int bufferSize;

    private Path parentPath;
    private String logPattern;
    private String dateFormat;
    private Set<Cursor> cursors=new CopyOnWriteArraySet<Cursor>();


    LogConfig(String logFileName,int bufferSize){
        setBufferSize(bufferSize);
        setLogFileName(logFileName);
    }

    public boolean isMatchLog(File newLogFile, Date date){
        String todayLogPattern = getDateLogPattern(this.logPattern, this.dateFormat, date);
        if (newLogFile.getName().matches(todayLogPattern)) {
            return true;
        }
        return false;
    }


    public Cursor addNewLog(File realLogFile){
        try {
            Cursor cursor=new Cursor(realLogFile,this.bufferSize);
            cursors.add(cursor);
            return cursor;
        }catch (IOException e){
            LOG.warn(e.getMessage());
            return null;
        }
    }

    public boolean removeOldLog(Cursor cursor){
        try {
            //close the cursor
            cursor.close();
            //remove from List
            cursors.remove(cursor);
        }catch (IOException e){
            LOG.warn(e.getMessage());
            return false;
        }
        return true;
    }

    public boolean isContainInCursors(File realLogFile){
        if(cursors.size()>0){
            for(Cursor c:cursors){
                if(c.getLogFile().equals(realLogFile)){
                    return true;
                }
            }
        }
        return false;
    }


    public Set<Cursor> getCursors(){
        return cursors;
    }

    public String getLogFileName() {
        return logFileName;
    }

    public Path getParentPath() {
        return parentPath;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public int getBufferSize(){
        return bufferSize;
    }


    private void setLogFileName(String logFileName) {
        this.logFileName = logFileName;

        File logFile= new File(logFileName);
        this.logPattern=logFile.getName();
        this.parentPath=logFile.getAbsoluteFile().getParentFile().toPath();

        this.dateFormat=getDateFormatStr(this.logPattern);
        //get today log files
        Date today=new Date();
        File[] logFiles=getLogFilesInParent(getDateLogPattern(this.logPattern, this.dateFormat,today));
        if(logFiles!=null){
            String todayFmtStr=new SimpleDateFormat(getDateFormat()).format(today);
            for(File realLogFile:logFiles){
                addLog2Collect(todayFmtStr, realLogFile);
            }
        }else {
            LOG.info("There is no file found in the path with the pattern {}. ",this.logFileName);
        }

    }

    private void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
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


    private String getDateLogSymbolicLink(String fileName, Date date){
        String patternWithDate=getDateLogPattern(this.logPattern,this.dateFormat,date);
        Matcher m = Pattern.compile("^.*\\((.*?)\\)\\?.*$").matcher(patternWithDate);
        if(m.matches()) {
           String lossPart=m.group(1).replaceAll("\\\\","");
           for(int i=fileName.length();i>0;i--){
               StringBuilder sb=new StringBuilder(fileName);
               sb.insert(i,lossPart);
               String insertedLogName=sb.toString();
               if(insertedLogName.matches(patternWithDate)){
                   return insertedLogName;
               }
           }

        }
       return null;
    }

    private File createSymbolicLink(File noDateLogFile){
        try{
            File linkFile = new File(this.parentPath.toFile().getAbsolutePath()+"/"+getDateLogSymbolicLink(noDateLogFile.getName(),new Date(noDateLogFile.lastModified())));
            if(!Files.exists(linkFile.toPath())){
                Path link= Files.createSymbolicLink(linkFile.toPath(), noDateLogFile.toPath());
                if(link!=null){
                    return linkFile;
                }
            }else{
                return null;
            }
        }catch (Exception e){
            LOG.error("",e);
        }
        return null;
    }


    public void addLog2Collect(String todayFmtStr, File logFile){
        //check with date string
        if(logFile.getName().contains(todayFmtStr)){
            if(!isContainInCursors(logFile)) {
                addNewLog(logFile);
                LOG.info("Add File {} to cursors. ", logFile.getAbsolutePath());
            }

        }else {
            //if not contain date string ,create link
            File linkFile= createSymbolicLink(logFile);
            if(linkFile!=null&&linkFile.getName().contains(todayFmtStr)){
                if(!isContainInCursors(linkFile)) {
                    addNewLog(linkFile);
                    LOG.info("Create link and add File {} to cursors. ",linkFile.getAbsolutePath());
                }
            }
        }
    }


    /**
     *列出所有的符合正则的文件。初始化的时候用的。
     *
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
