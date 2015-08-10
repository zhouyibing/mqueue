package com.manyi.mqservice.util;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.manyi.mqservice.reload.MessageProcessor;

public class MessageFilter {
    private static final long PERIOD = 60 * 1000; // 重置缓存时间
    private static final Logger logger = Logger.getLogger(MessageProcessor.class);
    private static volatile ConcurrentHashMap<String, Integer> dataMap = null;
    private static Thread monitor;

    static {
	initMap();
	initMonitor();
    }

    private static void initMap() {
	dataMap = new ConcurrentHashMap<String, Integer>(5000);
    }

    private static void initMonitor() {
	monitor = new Thread() {
	    public void run() {
		while (!this.isInterrupted()) {
		    try {
			Thread.sleep(PERIOD);
		    } catch (InterruptedException e) {
			this.interrupt();
		    }
		    try {
			initMap();
			logger.info("filterMap reinited");
		    } catch (Throwable e) {
		    }
		}
	    }
	};
	monitor.setDaemon(true);
	monitor.setPriority(10);
	monitor.start();
    }

    /**
     * @return true表示被过滤了，false表示新的消息
     * 
     */
    public static boolean filter(String key) {
	if (dataMap.containsKey(key)) {
	    return true;
	}
	dataMap.put(key, 0);
	return false;
    }
}