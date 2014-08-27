package org.apache.flume;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

public class CursorTest {

    private Cursor c;

    @org.junit.Before
    public void setUp() throws Exception {
        c =new Cursor(new File("logs/worker-1-20140827.log"),4096000);
    }

    @org.junit.Test
    public void testCompactBuffer() throws Exception {

        ByteBuffer b=ByteBuffer.allocate(6).put((byte)10).put((byte) 10).put((byte) 1).put((byte)3);

        int a= c.compactBuffer(b);
        System.out.println("compactSize="+a);

        ByteBuffer c=b.slice();

        StringBuilder sb=new StringBuilder();
        while (c.hasRemaining()){
            sb.append(c.get());
        }
        Assert.assertEquals("ok", "1010", sb.toString());
    }

    @Test
    public void testProcess(){
        try {
            c.process(new Cursor.ProcessCallBack() {
                @Override
                public void doCallBack(byte[] data) {
                    System.out.print(new String(data));
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTrim(){
        ByteBuffer b=ByteBuffer.allocate(2).put((byte)10).put((byte) 10);
        byte[] trimedBytes= Cursor.trim(b.array());
        System.out.println(new String(trimedBytes));
    }


}