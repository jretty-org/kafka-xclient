package org.jretty.kafka.xclient.consumer;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerStopListener implements Runnable {
    
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerStopListener.class);

    private Runnable callback;

    private AtomicInteger threadNum = new AtomicInteger();

    public ConsumerStopListener(Runnable callback) {
        this.callback = callback;
    }

    public void run() {
        if (threadNum.decrementAndGet() < 1) {
            LOG.info("Start to run callback...");
            
            if (callback != null) {
                callback.run();
            }
        } else {
            LOG.info("remain {} threads to stop!", threadNum);
        }
    }

    public Integer getThreadNum() {
        return threadNum.get();
    }

    public void setThreadNum(Integer threadNum) {
        this.threadNum.set(threadNum);
    }

}
