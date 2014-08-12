package org.apache.flume;

import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Cursor1 {
    private static final Logger LOG = LoggerFactory.getLogger(Cursor1.class);

    /* buffer size=1M  */
    private static final int BUFFER_SIZE=1024*1024;
    private ByteBuffer buffer=ByteBuffer.allocate(BUFFER_SIZE);

    /* file */
    private RandomAccessFile logRandomAccessFile;
    private FileChannel channel;
    private Offset logOffset;

    /* flume channel*/
    private ChannelProcessor channelProcessor;

   public Cursor1(File logFile) throws IOException{

        logRandomAccessFile=getLogRandomAccessFile(logFile);
        logOffset=getOffsetObject(logFile);

        channel=getLogFileChannel(logRandomAccessFile,logOffset);


    }

    public void setChannelProcessor(ChannelProcessor channelProcessor) {
        this.channelProcessor = channelProcessor;
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

    private void process() throws IOException{

        channel.read(buffer);
        int compactSize=compactBuffer(buffer);
        //
        channel.position(channel.position()-compactSize);
        //
        byte[] sb=new byte[buffer.limit()];
        buffer.get(sb);

        channelProcessor.processEvent(EventBuilder.withBody(sb));
        buffer.clear();

    }

    /**
     * 从后面开始查找是否含有换行。如果查到换行进行切割。并把切割掉的尺寸返回。
     * @param buffers
     * @return
     */
    public int compactBuffer(ByteBuffer buffer){

        int compactSize=0;

        int currPosition=buffer.position();
        for(int i=currPosition-1;i>=0;i--){
            buffer.position(i);
            byte b=buffer.get();
            System.out.println("b="+b);
            if(b==10){
                buffer.limit(i+1);
                buffer.position(0);
                compactSize=currPosition-i-1;
                return compactSize;
            }
        }
       return compactSize;
    }

}