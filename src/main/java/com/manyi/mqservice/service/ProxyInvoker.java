package com.manyi.mqservice.service;

import java.util.Date;

import org.apache.log4j.Logger;

import com.manyi.mqservice.util.MqClassLoader;

public class ProxyInvoker {

    private static final Logger logger = Logger.getLogger(ProxyInvoker.class);
    private static final String processorClassName = "com.manyi.mqservice.reload.MessageProcessor";
    /** 状态统计类名 */
    private static final String statorClassName = "com.manyi.mqservice.reload.DataStat";
    /** 控制台处理逻辑类名 */
    private static final String consoleHandlerName = "com.manyi.mqservice.reload.ConsoleHandler";
    /** 消息处理类 */
    private static Object processor = null;
    /** 状态统计类 */
    private static Object stator = null;
    /** 控制台处理逻辑类 */
    private static Object consoleHandler = null;

    static {
	try {
	    initReloadClass();
	} catch (Exception e) {

	}

    }

    /**
     * 初始化主要reload类
     */
    public static void initReloadClass() throws Exception {
	processor = MqClassLoader.instance.loadClass(processorClassName, true).newInstance();
	stator = MqClassLoader.instance.loadClass(statorClassName, true).newInstance();
	consoleHandler = MqClassLoader.instance.loadClass(consoleHandlerName, true).newInstance();
    }

    /**
     * 统计消息接收到响应的处理时间
     */
    public static void caculateInProcTime(long startTime) {
	try {
	    stator.getClass().getMethod("caculateInProcTime", long.class).invoke(stator, startTime);
	} catch (Exception e) {
	    logger.error("reloadable datastat error", e);
	}
    }

    /**
     * 统计消息从创建到下发的处理时间
     */
    public static void caculateOutProcTime(long startTime) {
	try {
	    stator.getClass().getMethod("caculateOutProcTime", long.class).invoke(stator, startTime);
	} catch (Exception e) {
	    logger.error("reloadable datastat error", e);
	}
    }

    /**
     * 返回统计数据
     */
    public static String getCaculateData() {
	try {
	    return (String) stator.getClass().getMethod("getStatValue").invoke(stator);
	} catch (Exception e) {
	    logger.error("reloadable datastat error", e);
	    return null;
	}
    }

    /**
     * 返回类的载入时间
     */
    public static String getStatorClassStartTime() {
	try {
	    Long time = (Long) stator.getClass().getMethod("getClassStartTime").invoke(stator);
	    return new Date(time).toString();
	} catch (Exception e) {
	    logger.error("reloadable datastat error", e);
	    return null;
	}
    }

    /**
     * 返回类的载入时间
     */
    public static String getProcessorClassStartTime() {
	try {
	    Long time = (Long) processor.getClass().getMethod("getClassStartTime").invoke(processor);
	    return new Date(time).toString();
	} catch (Exception e) {
	    logger.error("reloadable datastat error", e);
	    return null;
	}
    }

    public static void proxyMsgHandle(String method, Object[] obj, Class<?>[] parameterTypes) {
	try {
	    processor.getClass().getMethod(method, parameterTypes).invoke(processor, obj);
	} catch (Exception e) {
	    logger.error("reloadable proxyMsgHandle error", e);
	}
    }

    public static void proxyConsoleHandle(String method, Object[] obj, Class<?>[] parameterTypes) {
	try {
	    consoleHandler.getClass().getMethod(method, parameterTypes).invoke(consoleHandler, obj);
	} catch (Exception e) {
	    logger.error("reloadable proxyMsgHandle error", e);
	}
    }
}