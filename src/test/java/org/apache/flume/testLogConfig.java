package org.apache.flume;


import org.junit.Test;

public class testLogConfig {

   private LogConfig logConfig;

    @org.junit.Before
    public void setUp() throws Exception {
        logConfig=new LogConfig("logs/worker-([0-9]+)-${yyyyMMdd}.log",4096);
    }

    @Test
    public void testSetLogFileName(){
        //logConfig.setLogFileName("logs/worker-([0-9]+)-${yyyyMMdd}.log");
    }



}