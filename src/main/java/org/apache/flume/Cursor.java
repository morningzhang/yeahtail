package org.apache.flume;

import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Cursor implements Closeable{
    private static final Logger LOG = LoggerFactory.getLogger(Cursor.class);

    /* buffer size=400K  */
    private static final int BUFFER_SIZE=409600;
    private ByteBuffer buffer=ByteBuffer.allocate(BUFFER_SIZE);

    /* file and channel */
    private RandomAccessFile logRandomAccessFile;
    private FileChannel channel;
    /* offset */
    private Offset logOffset;
    private volatile boolean done=false;
    /* sleep time*/
    private long sleepTime=1000;//1s

   public Cursor(File logFile) throws IOException{

        logRandomAccessFile=getLogRandomAccessFile(logFile);
        logOffset=getOffsetObject(logFile);

        channel=getLogFileChannel(logRandomAccessFile,logOffset);

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

    public synchronized int process(ProcessCallBack processCallBack) throws IOException{
        channel.position(logOffset.getCurrentValue());
        //读取到buffer
        int len=channel.read(buffer);
        if(len==-1){
            return len;
        }
        //切割最后一行
        int compactSize=compactBuffer(buffer);
        //回退到一定的position
        channel.position(channel.position()-compactSize);
        //读取到数组
        byte[] data=new byte[buffer.limit()];
        buffer.get(data);
        //do process
        processCallBack.doCallBack(data);
        //更新offset
        logOffset.increaseBy(data.length);
        //清除
        buffer.clear();

        return len;
    }

    /**
     * 从后面开始查找是否含有换行。如果查到换行进行切割。并把切割掉的尺寸返回。
     * @param buffer
     * @return int
     */
    public int compactBuffer(ByteBuffer buffer){

        int currPosition=buffer.position();
        for(int i=currPosition-1;i>=0;i--){
            buffer.position(i);
            byte b=buffer.get();
            if(b==10){//换行符号
                buffer.position(0);
                buffer.limit(i+1);
                return currPosition-i-1;
            }
        }
        //没在buffer中查找的换行的时候
        buffer.position(0);
        buffer.limit(currPosition);
        return 0;
    }

    public void setSleepTime(long sleepTime){
        this.sleepTime=sleepTime;
    }

    public void setDone(boolean done){
        this.done=done;
    }

    public void start(ProcessCallBack processCallBack) throws IOException,InterruptedException{
        //重试300次
        int retryTimes=300;

        while (true){
            //1.没有可以读的了
            //2.新的日期已经生成
            //3.满足以上条件再重试300次
            if(process(processCallBack)==-1&&done){
                if(--retryTimes<=0){
                    close();
                    break;
                }
            }
            Thread.currentThread().sleep(sleepTime);
        }
    }

    public synchronized void close(){
        Closeables.closeQuietly(channel);
        Closeables.closeQuietly(logRandomAccessFile);
        Closeables.closeQuietly(logOffset);
    }

   public interface ProcessCallBack{
       void doCallBack(byte[] data);
    }

}