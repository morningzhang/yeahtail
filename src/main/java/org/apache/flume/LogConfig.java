package org.apache.flume;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Date;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogConfig {

    private static final Logger LOG = LoggerFactory.getLogger(LogConfig.class);
    //传入的日志文件名称
    private String logFileName;
    //传入的缓存大小
    private int bufferSize;

    private Path parentPath;
    private String logPattern;
    private String dateFormat;
    private List<Cursor> cursors=new CopyOnWriteArrayList<Cursor>();


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

    public List<Cursor> getCursors(){
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

    private void setLogFileName(String logFileName) {
        this.logFileName = logFileName;

        File logFile= new File(logFileName);
        this.logPattern=logFile.getName();
        this.parentPath=logFile.getAbsoluteFile().getParentFile().toPath();

        this.dateFormat=getDateFormatStr(this.logPattern);

        //get today log files
        Date today=new Date();
        String todayLogPattern= getDateLogPattern(this.logPattern, this.dateFormat, today);
        File[] logFiles=getLogFilesInParent(todayLogPattern);
        if(logFiles!=null){
            String todayFmtStr=new SimpleDateFormat(getDateFormat()).format(today);
            for(File realLogFile:logFiles){
                //check with date string
                if(realLogFile.getName().contains(todayFmtStr)){
                    addNewLog(realLogFile);
                    LOG.info("Add File {} to cursors. ",realLogFile.getAbsolutePath());
                }else {
                    //if not contain date string ,create link
                    try{
                        File linkFile = new File(this.parentPath.toFile().getAbsolutePath()+"/"+getDateLogSymbolicLink(realLogFile.getName(),today));
                        if(!Files.exists(linkFile.toPath())){
                            Path link= Files.createSymbolicLink(linkFile.toPath(), realLogFile.toPath());
                            if(link!=null){
                                addNewLog(linkFile);
                                LOG.info("create link and add File {} to cursors. ",linkFile.getAbsolutePath());
                            }

                        }

                    }catch (Exception e){
                        LOG.error("",e);
                    }

                }

            }
        }else {
            LOG.info("There is no file found in the path with the pattern {}. ",todayLogPattern);
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


    public String getDateLogSymbolicLink(String fileName, Date date){
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
