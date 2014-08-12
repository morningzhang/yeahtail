package org.apache.flume;


import java.io.File;
import java.nio.ByteBuffer;

import org.junit.Assert;

public class Cursor1Test {

    private  Cursor1 c;

    @org.junit.Before
    public void setUp() throws Exception {

        c =new Cursor1(new File("logs/aaa.log"));
    }

    @org.junit.Test
    public void testCompactBuffer() throws Exception {
        ByteBuffer b=ByteBuffer.allocate(6).put((byte)10).put((byte)10).put((byte) 1).put((byte)3);

        int a= c.compactBuffer(b);
        System.out.println("compactSize="+a);

        ByteBuffer c=b.slice();



        StringBuilder sb=new StringBuilder();
        while (c.hasRemaining()){
            sb.append(c.get());
        }
        Assert.assertEquals("ok","1010", sb.toString());
        //Assert.assertEquals(sb.toString(),"1210");
    }
}