package org.jretty.kafka.xclient.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingProducerListener<K, V> implements ProducerListener<K, V> {

    private final Logger logger = LoggerFactory.getLogger("ASYNC-LOGGER");

    private int maxContentLength = 100;
    
    private boolean interestedInSuccess = false;

    public LoggingProducerListener() {

    }

    /**
     * see {{@link #setMaxContentLength(int)}
     * 
     * @param maxContentLength
     */
    public LoggingProducerListener(int maxContentLength) {
        this.maxContentLength = maxContentLength;
    }

    /**
     * The maximum amount of data to be logged for either key or password. 
     * As message sizes may vary and become fairly large, this allows limiting the
     * amount of data sent to logs.
     *
     * @param maxContentLength
     *            the maximum amount of data being logged.
     */
    public void setMaxContentLength(int maxContentLength) {
        this.maxContentLength = maxContentLength;
    }

    @Override
    public void onCompletion(long startTime, CallbackData<K, V> callbackData, Exception e) {
        long elapsedTime = System.currentTimeMillis() - startTime;

        if (e != null) {
            logger.error("producer.send() Callback error!  cost time=" + elapsedTime + " topic="
                    + callbackData.getRecord().topic() + ", key="
                    + shortString(String.valueOf(callbackData.getRecord().key()), maxContentLength) + ", value="
                    + shortString(String.valueOf(callbackData.getRecord().value()), maxContentLength) + ". Reason: ",
                    e);
            return;
        }

        if (interestedInSuccess && callbackData.getMetadata() != null && logger.isDebugEnabled()) {
            logger.debug("message({}, {}) sent to topic({}) partition({}), offset({}) in {} ms",
                    callbackData.getRecord().key(), callbackData.getRecord().value(), callbackData.getRecord().topic(),
                    callbackData.getMetadata().partition(), callbackData.getMetadata().offset(), elapsedTime);
        }
    }

    private String shortString(String original, int maxCharacters) {
        if (original.length() <= maxCharacters) {
            return original;
        }
        return original.substring(0, maxCharacters) + "...";
    }

    public boolean isInterestedInSuccess() {
        return interestedInSuccess;
    }

    public void setInterestedInSuccess(boolean interestedInSuccess) {
        this.interestedInSuccess = interestedInSuccess;
    }

}