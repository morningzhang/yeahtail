package org.apache.flume;

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

    private ByteBuffer buffer;

    /* file and channel */
    private File logFile;
    private RandomAccessFile logRandomAccessFile;
    private FileChannel channel;
    /* offset */
    private Offset logOffset;


    public Cursor(File logFile,int bufferSize)  throws IOException{
        this.logFile=logFile;
        buffer=ByteBuffer.allocateDirect(10);
        init(this.logFile);
    }


    private void init(File logFile) throws IOException{
        //close last time opened
        closeFileChannel();
        //open for this time
        logRandomAccessFile=new RandomAccessFile(logFile,"r");
        channel=logRandomAccessFile.getChannel();
        //if opened,need not open again
        if(logOffset==null){
            logOffset=getOffsetObject(logFile);
        }
        //seek to current position
        channel.position(logOffset.getCurrentValue());
    }

    public static  String getLogOffsetFileName(File logFile) {
        return logFile.getParent()+"/."+logFile.getName()+".offset";
    }

    private Offset getOffsetObject(File logFile) throws IOException{
        File offsetFile= new File(getLogOffsetFileName(logFile));
        if(!offsetFile.exists()){
            boolean isOk= offsetFile.createNewFile();
            if(!isOk){
                throw new IOException("cloud not be create the offset file "+offsetFile.getName());
            }
            LOG.info("create a new offset file {}",offsetFile.getAbsolutePath());
        }else{
            LOG.info("find a existed offset file {} ",offsetFile.getAbsolutePath());
        }

        return new Offset(offsetFile);
    }

    public File getLogFile() {
        return logFile;
    }

    public synchronized int process(ProcessCallBack processCallBack) throws IOException{
        //读取到buffer
        int len=channel.read(buffer);
        if(len==-1){
            //如果读到了文件的末尾，重新获取channel
            init(logFile);
            //LOG.info("transfer size {} for the logfile {} and transfer velocity is greater than log produced.",0,logFile.getName());
            return len;
        }
        //切割最后一行
        int compactSize=compactBuffer(buffer);
        if(compactSize>0){
            //回退到一定的position
            channel.position(channel.position()-compactSize);
        }
        //读取到数组
        byte[] data = new byte[buffer.limit()];
        buffer.get(data);
        //trim
        data=trim(data);
        //do process
        processCallBack.doCallBack(data);
        //更新offset
        logOffset.increaseBy(buffer.limit());
        //清除
        buffer.clear();

        LOG.info("transfer {} for the logfile {} ",data.length,logFile.getName());

        return len;
    }

    public static byte[] trim(byte[] data) {
        ByteBuffer dataBuffer = ByteBuffer.wrap(data);
        byte firstByte = dataBuffer.get(0);
        if (firstByte == 10) {
            data = new byte[dataBuffer.limit() - 1];
            dataBuffer.position(1);
            dataBuffer.get(data);
            dataBuffer = ByteBuffer.wrap(data);
        }
        byte theLastByte = dataBuffer.get(dataBuffer.limit() - 1);
        if (theLastByte == 10) {
            data = new byte[dataBuffer.limit() - 1];
            dataBuffer.position(0);
            dataBuffer.get(data);
        }
        return data;
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



    private void closeFileChannel() throws IOException{
        if(channel!=null&&channel.isOpen()){
            channel.close();
        }
        if(logRandomAccessFile!=null){
            logRandomAccessFile.close();
        }
    }

    private void closeOffset() throws IOException{
        if(logOffset!=null){
            logOffset.close();
        }
    }

    public synchronized void close() throws IOException{
        closeFileChannel();
        closeOffset();
        LOG.info("close the cursor {} is ok",logFile.getName());
    }

    public interface ProcessCallBack{
        void doCallBack(byte[] data);
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof Cursor)){
            return false;
        }
        if(!getLogFile().equals(((Cursor)obj).getLogFile())){
            return false;
        }
        return true;
    }
}