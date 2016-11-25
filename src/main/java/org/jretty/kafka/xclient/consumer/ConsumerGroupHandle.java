package org.jretty.kafka.xclient.consumer;

import java.util.List;

public interface ConsumerGroupHandle {
    
    public void start(final Runnable callback);
    
    public void addConsumerThread(final int consumerThreadNum, final Runnable callback);

    public void shutdown(final Runnable callback);
    
    @SuppressWarnings("rawtypes")
    public List<ConsumerThread> getConsumerThreadList();
    
    /**
     * consumer group线程是否启动
     * 
     * @return
     */
    public boolean isRunning();

}
