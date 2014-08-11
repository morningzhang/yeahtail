package org.apache.flume;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhangliming on 14-8-8.
 */
public class Offset {
    AtomicLong offsetValue;

    FileChannel channel;
    MappedByteBuffer mappedByteBuffer;

    public Offset(File file) throws IOException {
        channel = new RandomAccessFile(file,"rw").getChannel();
        mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 8);
        offsetValue= new AtomicLong(getCurrentValue());
    }

    public void close() throws IOException {
        if(channel.isOpen()){
            channel.force(true);
            channel.close();
        }
    }

    public void increaseBy(long i){
        putValue(offsetValue.addAndGet(i));
    }

    private void putValue(long newOffsetValue){
        mappedByteBuffer.rewind();
        mappedByteBuffer.putLong(newOffsetValue);
    }

    public long getCurrentValue(){
        mappedByteBuffer.rewind();
        return mappedByteBuffer.getLong();
    }

   public static void main(String[] args){
       Offset offset=null;
       try {
           offset=  new Offset(new File("fff.txt"));
           offset.increaseBy(5);
           System.out.println( offset.getCurrentValue());
           offset.increaseBy(100);
           System.out.println( offset.getCurrentValue());

       }catch (Exception e){
            e.printStackTrace();
       }finally {
          try{
              offset.close();
          }catch (Exception e){
                e.printStackTrace();
          }
       }

   }

}