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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Cursor implements Closeable{
    private static final Logger LOG = LoggerFactory.getLogger(Cursor.class);

    /* buffer size=400K  */
    private static final int BUFFER_SIZE=409600;
    private ByteBuffer buffer=ByteBuffer.allocate(BUFFER_SIZE);

    /* file and channel */
    private File logFile;
    private RandomAccessFile logRandomAccessFile;
    private FileChannel channel;
    /* offset */
    private Offset logOffset;
    private volatile boolean done=false;
    /* sleep time*/
    private long sleepTime=1000;//1s

    ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

    public Cursor(File logFile)  throws IOException{
        this.logFile=logFile;
        init(logFile);
    }

    private void init(File logFile) throws IOException{
        logRandomAccessFile=new RandomAccessFile(logFile,"r");
        channel=logRandomAccessFile.getChannel();

        if(logOffset==null){
            logOffset=getOffsetObject(logFile);
        }
        channel.position(logOffset.getCurrentValue());
    }


    private Offset getOffsetObject(File logFile) throws IOException{
        File offsetFile= new File(logFile.getAbsolutePath()+".offset");
        if(!offsetFile.exists()){
           boolean isOk= offsetFile.createNewFile();
           if(!isOk){
                throw new IOException("cloud not be create the offset file "+offsetFile.getName());
            }
        }
        return new Offset(offsetFile);
    }

    public File getLogFile() {
        return logFile;
    }

    public synchronized int process(ProcessCallBack processCallBack) throws IOException{
        init(logFile);
        //读取到buffer
        int len=channel.read(buffer);
        if(len==-1){
            LOG.info("transfer size {} for the logfile {} and transfer velocity is greater than log produced. ",len,logFile.getName());
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

        LOG.info("transfer {} for the logfile {} ",len,logFile.getName());

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

    public void start(final ProcessCallBack processCallBack){

        singleThreadExecutor.submit(new Runnable() {
            @Override
            public void run() {
                //重试300次
                int retryTimes=300;
                while (true){

                    try {
                        //1.没有可以读的了
                        //2.新的日期已经生成
                        //3.满足以上条件再重试300次
                        if(process(processCallBack)==-1&&done){
                            LOG.info("left retryTimes %d .it would be closed.",retryTimes);
                            if(--retryTimes<=0){
                                close();
                                break;
                            }
                        }
                        process(processCallBack);
                        Thread.sleep(sleepTime);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }
        });

    }

    private void closeFileChannel() throws IOException{
        Closeables.close(channel,true);
        Closeables.close(logRandomAccessFile, true);
        Closeables.close(logOffset, true);
    }

    public synchronized void close() throws IOException{
        if(!singleThreadExecutor.isShutdown()){
            singleThreadExecutor.shutdown();
        }
        closeFileChannel();
        LOG.info("close the cursor {} is ok",logFile.getName());
    }

   public interface ProcessCallBack{
       void doCallBack(byte[] data);
    }

}