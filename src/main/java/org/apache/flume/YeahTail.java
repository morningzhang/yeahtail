package org.apache.flume;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class YeahTail extends AbstractSource
        implements EventDrivenSource, Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(YeahTail.class);

    private ChannelProcessor channelProcessor;

    final List<Cursor> cursors = new ArrayList<Cursor>();


    public YeahTail() {
        LOG.debug("YeahTail starting......");
    }

    public void configure(Context context) {
        //log filename
        String fileName = context.getString("file");
        //date pattern for log
        String pattern = context.getString("pattern");
        boolean alwaysIncludePattern = context.getBoolean("alwaysIncludePattern", false);
        //fetch interval
        long fetchInterval = context.getLong("fetchInterval", Long.valueOf(1000).longValue());


        Preconditions.checkArgument(fileName != null, "Null File is an illegal argument");
        Preconditions.checkArgument(pattern != null&&pattern.length()>0, "Null or blank pattern is an illegal argument");

        Preconditions.checkArgument(fetchInterval > 0L, "fetchInterval <=0 is an illegal argument");

        try {
            if(alwaysIncludePattern){
                SimpleDateFormat sdf=new SimpleDateFormat(pattern);
                fileName+=sdf;
            }
            File file = new File(fileName);
            Cursor cursor=new Cursor(file);
            cursor.setSleepTime(fetchInterval);

        } catch (Throwable t) {
            String ss = Throwables.getStackTraceAsString(t);
            System.err.println("Throwable:"+ss);
            System.exit(1);
        }

    }

    public void start() {

    }

    public void stop() {

    }



}