package org.jretty.kafka.xclient.consumer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jretty.util.Const;
import org.jretty.util.FileUtils;
import org.jretty.util.IOUtils;
import org.jretty.util.json.JSONUtils;

/**
 * 将offset持久化到磁盘中
 * 
 * @author zollty
 * @since 2016-10-16
 */
public class ConsumerLog {
    
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerLog.class);
    private static final Logger OFFSET_BEIGIN = LoggerFactory.getLogger("OFFSET-BEGIN");
    private static final Logger OFFSET_END = LoggerFactory.getLogger("OFFSET-END");
    
    public static synchronized void error(String log, Exception e) {
        LOG.error(log, e);
    }
    
    public static synchronized void warn(String log, Exception e) {
        LOG.warn(log, e);
    }
    
    public static synchronized void info(String log, Object ... param) {
        LOG.info(log, param);
    }
    
    public static synchronized void debug(String log, Object ... param) {
        LOG.info(log, param);
    }
    
    public static synchronized void trace(String log, Object ... param) {
        LOG.info(log, param);
    }
    
    protected static Map<String, Long> tempMap = new HashMap<>();
    
    public static void logFirstOffset(String key, long val) {
        
        synchronized (tempMap) {
            if(tempMap.containsKey(key)) {
                return;
            }
            tempMap.put(key, val);
        }
        
//        Map<String, Long> oldMap = ConsumerLog.getOffsetMap();
//        Long lastPos = oldMap.get(key);
//        if (lastPos != null && lastPos.longValue() != val) {
//            LOG.error("last offset = {}, but this first poll from {}", lastPos, val);
//        }
        
        OFFSET_BEIGIN.info("{\"partition\":\"" + key + "\",\"offset\":" + val + "}");
    }
    
    
    private static Map<String, Long> lastOffsetMap = new HashMap<String, Long>();
    private static boolean flushed = false;
    
    public static synchronized void logLastOffset(Map<String, Long> offsetMap) {
        
        for (Map.Entry<String, Long> entry : offsetMap.entrySet()) {
            String key = entry.getKey();
            // 如果已有该offset，则与之比较offset的大小，保存较大的一个offset值
            if (lastOffsetMap.containsKey(key)) {
                Long val = lastOffsetMap.get(key);
                Long val2 = entry.getValue();
                if (val2 > val) {
                    lastOffsetMap.put(key, val2);
                }
            } else {
                lastOffsetMap.put(key, entry.getValue());
            }
        }

    }
    
    public static synchronized void flushLastOffset() {
        if (flushed) {
            LOG.error("already flushed LastOffset!");
            return;
        }
        if (lastOffsetMap.isEmpty()) {
            return;
        }
        
        Map<String, Long> allOffsetMap = new HashMap<String, Long>(ConsumerLog.getLastendOffsetMap());
        for (Map.Entry<String, Long> entry : lastOffsetMap.entrySet()) {
            // 用新值覆盖上一次的值
            allOffsetMap.put(entry.getKey(), entry.getValue());
        }
        
        for (Map.Entry<String, Long> entry : allOffsetMap.entrySet()) {
            OFFSET_END.info("{\"partition\":\"" + entry.getKey() + "\",\"offset\":" + entry.getValue() + "}");
        }
        
        lastOffsetMap.clear();
        
        copyOffsetEnd2Lastend();
        
        flushed = true;
    }
    
    private static void copyOffsetEnd2Lastend() {
        String prefix = System.getProperty("catalina.base");
        if (prefix == null) {
            prefix = ".";
        }
        String fileFullPath = prefix + "/logs/offset-end.log";
        BufferedReader br3 = null;
        BufferedWriter bw2 = null;
        try {
            File f = new File(fileFullPath);
            if(f.exists()) {
                br3 = IOUtils.getBufferedReader(fileFullPath, "UTF-8");
                bw2 = IOUtils.getBufferedWriter(prefix + "/logs/offset-lastend.log", false, "UTF-8");
                IOUtils.clone(br3, bw2);
            }
        } catch (IOException e) {
            LOG.error("", e);
            IOUtils.closeIO(br3, bw2);
        }
    }
    
    
    private static Map<String, Long> lastendOffsetMap;
    
    public static Map<String, Long> getNewStartOffset() {
        if (lastendOffsetMap == null) {
            getLastendOffsetMap();
        }
        Map<String, Long> ret = new HashMap<String, Long>();
        for (Map.Entry<String, Long> entry : lastendOffsetMap.entrySet()) {
            ret.put(entry.getKey(), entry.getValue() + 1);
        }
        return ret;
    }
    
    public static synchronized Map<String, Long> getLastendOffsetMap() {
        if (lastendOffsetMap != null) {
            return lastendOffsetMap;
        }

        String prefix = System.getProperty("catalina.base");
        if (prefix == null) {
            prefix = ".";
        }
        String fileFullPath = prefix + "/logs/offset-lastend.log";
        Map<String, Long> ret = new HashMap<String, Long>();
        List<Map<String, Object>> mapList = FileUtils.parseTextFile(fileFullPath,
                new FileUtils.TextFileParse<Map<String, Object>>() {
                    @SuppressWarnings("unchecked")
                    @Override
                    public Map<String, Object> parseOneLine(String jsonStr) {

                        return (Map<String, Object>) JSONUtils.parse(jsonStr);
                    }

                }, Const.UTF_8);

        for (Map<String, Object> item : mapList) {
            ret.put(item.get("partition").toString(), Long.valueOf(item.get("offset").toString()));
        }

        LOG.info("got lastendOffset: " + ret.toString());

        ConsumerLog.lastendOffsetMap = ret;

        return ret;
    }
    

}
