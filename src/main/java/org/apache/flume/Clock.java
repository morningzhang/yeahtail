package org.apache.flume;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public abstract class Clock
{
    private static Clock clock = new DefaultClock();

    private static final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmssSSSZ");

    public static String timeStamp()
    {
        synchronized (dateFormat) {
            return dateFormat.format(date());
        }
    }

    public static void resetDefault()
    {
        clock = new DefaultClock();
    }

    public static void setClock(Clock c) {
        clock = c;
    }

    public static long unixTime() {
        return clock.getUnixTime();
    }

    public static long nanos() {
        return clock.getNanos();
    }

    public static Date date() {
        return clock.getDate();
    }

    public static void sleep(long millis) throws InterruptedException {
        clock.doSleep(millis);
    }

    public abstract long getUnixTime();

    public abstract long getNanos();

    public abstract Date getDate();

    public abstract void doSleep(long paramLong)
            throws InterruptedException;

    static class DefaultClock extends Clock
    {
        public long getNanos()
        {
            return System.nanoTime();
        }

        public long getUnixTime()
        {
            return System.currentTimeMillis();
        }

        public Date getDate()
        {
            return new Date();
        }

        public void doSleep(long millis) throws InterruptedException
        {
            Thread.sleep(millis);
        }
    }
}