package org.jretty.kafka.xclient.producer;

public interface ProducerListener<K, V> {
    
    public void onCompletion(long startTime, CallbackData<K, V> callbackData, Exception e);

}
