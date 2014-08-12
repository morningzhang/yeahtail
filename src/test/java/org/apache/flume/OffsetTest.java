package org.apache.flume;


import org.junit.After;
import java.io.File;

public class OffsetTest {

    private  Offset offset=null;

    @org.junit.Before
    public void setUp() throws Exception {

        offset=  new Offset(new File("fff.txt"));
    }

    @org.junit.Test
    public void testIncreaseBy() throws Exception {
        try {
            offset=  new Offset(new File("fff.txt"));
            offset.increaseBy(5);
            System.out.println( offset.getCurrentValue());
            offset.increaseBy(100);
            System.out.println( offset.getCurrentValue());

        }catch (Exception e){
            e.printStackTrace();
        }
    }
    @After
    public void tearDown(){
        try{
            offset.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}