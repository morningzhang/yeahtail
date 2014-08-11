package org.apache.flume;

import com.sun.org.apache.bcel.internal.generic.RETURN;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Cursor1 {
    private static final Logger LOG = LoggerFactory.getLogger(Cursor1.class);

    /* buffer size=4M  */
    private static final long MAX_BUFFER_SIZE=4096000L;

    private RandomAccessFile logRandomAccessFile;
    private FileChannel channel;
    private Offset logOffset;

    private MappedByteBuffer mappedByteBuffer;

   public Cursor1(File logFile) throws IOException{

        logRandomAccessFile=getLogRandomAccessFile(logFile);
        logOffset=getOffsetObject(logFile);

        channel=getLogFileChannel(logRandomAccessFile,logOffset);


        mappedByteBuffer=reMapTheBuffer(logFile);
        extractLines(mappedByteBuffer);
    }


    private Offset getOffsetObject(File logFile) throws IOException{
        File offsetFile= new File(logFile.getAbsolutePath()+".offset");
        if(!offsetFile.exists()){
            offsetFile.createNewFile();
        }
        return new Offset(offsetFile);
    }

    private RandomAccessFile getLogRandomAccessFile(File logFile) throws IOException{
        return new RandomAccessFile(logFile,"r");
    }

    private FileChannel getLogFileChannel(RandomAccessFile logRandomAccessFile,Offset logOffset) throws IOException{
      logRandomAccessFile.seek(logOffset.getCurrentValue());
      return logRandomAccessFile.getChannel();
    }

    private MappedByteBuffer reMapTheBuffer(File logFile) throws IOException{
        long fileLen=logFile.length();
        long offset=logOffset.getCurrentValue();
        long theEnd=Math.min(fileLen,offset+MAX_BUFFER_SIZE);
        return channel.map(FileChannel.MapMode.READ_ONLY, offset,theEnd);
    }


    private boolean extractLines(ByteBuffer buffer) {
        buffer.rewind();
        byte b=buffer.get();

        while (b==10){


            b=buffer.get();
        }

        return false;
    }





    public static void  main(String[] args){
        try {
            new Cursor1(new File("logs/aaa.log"));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}