package org.apache.flume;


import org.junit.Test;
import java.io.File;

public class testLogConfig {

    private LogConfig logConfig;

    @org.junit.Before
    public void setUp() throws Exception {
        logConfig=new LogConfig();
        logConfig.setAlwaysIncludePattern(false);
        logConfig.setPattern(".yyyy-MM-dd");
        logConfig.setFetchInterval(1000L);
        logConfig.setLogFileName(new File("logs/aaa.log.2014-08-14").getAbsolutePath());
    }
    @Test
    public void testGetRealLogFile(){
        String realLogFile=logConfig.getRealLogFile();
        System.out.println(realLogFile);
    }
    @Test
    public void testGenerateCursor() throws Exception{
        logConfig.generateCursor();
    }



}