package com.manyi.mqservice.reload;

import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.manyi.mqservice.service.ControlCenter;

public class DataStat {
	private static final Logger logger = Logger.getLogger(DataStat.class);
	/** 接收消息处理时间 */
	private static AtomicLong inProcMs = new AtomicLong(0);
	/** 分发消息处理时间 */
	private static AtomicLong outProcMs = new AtomicLong(0);
	/** in消息请求次数 */
	private static AtomicLong inRequestCount = new AtomicLong(0);
	/** out消息处理次数 */
	private static AtomicLong outRequestCount = new AtomicLong(0);
	/** in请求总数 */
	private static AtomicLong inTotalTimes = new AtomicLong(0);
	/** out消息处理次数 */
	private static AtomicLong outTotalTimes = new AtomicLong(0);
	/** app的请求次数 */
	private static ConcurrentHashMap<String, AtomicInteger> appMessageCount = new ConcurrentHashMap<String, AtomicInteger>();
	/** in最大处理时间 */
	private static long inMaxProcMs = 0;
	/** out最大处理时间 */
	private static long outMaxProcMs = 0;
	/** 统计间隔 */
	private static int periodTime = 10 * 1000;
	/** 创建时间 */
	private static long classStartTime = System.currentTimeMillis();
	/** 定时处理器 */
	private static Thread monitor = null;
	/** 统计结果 */
	private static String statOutputValue = null;
	/** app统计结果 */
	private static String appStatValue = null;
	/** 网络通信异常次数 */
	private static AtomicInteger netErrorTimes = new AtomicInteger(0);
	static {
		monitor = new Thread() {
			public void run() {
				int s = 0;
				while (true) {
					s++;
					try {
						doCaculateGlobal();
						if (ControlCenter.isEnableAppStat() && s % 6 == 0) {
							appStat();
						}
						Thread.sleep(periodTime);
					} catch (InterruptedException e) {
						break;
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		};
		monitor.setDaemon(true);
		monitor.start();
	}

	/**
	 * 增加应用的请求数
	 */
	public void incRequestCount(String app) {
		AtomicInteger val = appMessageCount.get(app);
		if (val == null) {
			val = new AtomicInteger(0);
			appMessageCount.put(app, val);
		}
		val.incrementAndGet();
	}

	/**
	 * 将时间段内的统计结果输出，并清0
	 */
	public static void appStat() {
		StringBuilder sb = new StringBuilder();
		Iterator<String> it = appMessageCount.keySet().iterator();
		while (it.hasNext()) {
			String app = it.next();
			AtomicInteger count = appMessageCount.get(app);
			sb.append(app);
			sb.append(" ");
			sb.append(count == null ? "0" : count.get());
			sb.append("\n\r");
			if (count != null) {
				count.set(0);
			}
		}
		appStatValue = sb.toString();
		if (logger.isInfoEnabled()) {
			logger.info("appstat:" + appStatValue);
		}
	}

	/**
	 * 输出统计数据
	 */
	public String getStatValue() {
		return statOutputValue;
	}

	/**
	 * 获得这个类最初载入的时间戳
	 */
	public long getClassStartTime() {
		return classStartTime;
	}

	/**
	 * 进行统计并输出指定格式的数据
	 */
	private static void doCaculateGlobal() {
		long avgSpeedIn = 0, avgSpeedOut = 0;
		long inProcCount = inRequestCount.get();
		long outProcCount = outRequestCount.get();
		if (inProcCount > 0) {
			avgSpeedIn = inProcMs.get() / inProcCount;
		}
		if (outProcCount > 0) {
			avgSpeedOut = outProcMs.get() / outProcCount;
		}
		Runtime rt = Runtime.getRuntime();
		StringBuilder sb = new StringBuilder();
		sb.append("in_proc_avg ");
		sb.append(avgSpeedIn);
		sb.append("\n\r");
		sb.append("in_proc_count ");
		sb.append(inProcCount);
		sb.append("\n\r");
		sb.append("in_proc_max ");
		sb.append(inMaxProcMs);
		sb.append("\n\r");
		sb.append("in_proc_total ");
		sb.append(inTotalTimes.get());
		sb.append("\n\r");
		sb.append("out_proc_avg ");
		sb.append(avgSpeedOut);
		sb.append("\n\r");
		sb.append("out_proc_count ");
		sb.append(outProcCount);
		sb.append("\n\r");
		sb.append("out_proc_max ");
		sb.append(outMaxProcMs);
		sb.append("\n\r");
		sb.append("out_proc_total ");
		sb.append(outTotalTimes.get());
		sb.append("\n\r");
		sb.append("session_exception ");
		sb.append(netErrorTimes.get());
		sb.append("\n\r");
		sb.append("mem_used ");
		sb.append(FtoM(rt.totalMemory() - rt.freeMemory()));
		sb.append("\n\r");
		sb.append("mem_free ");
		sb.append(FtoM(rt.freeMemory()));
		sb.append("\n\r");
		sb.append("mem_total ");
		sb.append(FtoM(rt.totalMemory()));
		sb.append("\n\r");
		statOutputValue = sb.toString();
		// 初始化
		inProcMs.set(0);
		outProcMs.set(0);
		inRequestCount.set(0);
		outRequestCount.set(0);
		netErrorTimes.set(0);
		inMaxProcMs = 0;
		outMaxProcMs = 0;
	}

	/**
	 * 纳入一次接收消息统计
	 */
	public void caculateInProcTime(long startTime) {
		long pastTime = System.currentTimeMillis() - startTime;
		if (logger.isDebugEnabled()) {
			logger.debug("inProcTime : " + pastTime);
		}
		if (pastTime < 0) {
			return;
		}
		inProcMs.addAndGet(pastTime); // 接收消息处理时间增加
		inRequestCount.incrementAndGet();// 处理次数+1
		if (pastTime > inMaxProcMs) { // 最大值获取
			inMaxProcMs = pastTime;
		}
		inTotalTimes.incrementAndGet(); // 总次数+1
	}

	/**
	 * 纳入一次发送消息统计
	 */
	public void caculateOutProcTime(long startTime) {
		long pastTime = System.currentTimeMillis() - startTime;
		if (logger.isDebugEnabled()) {
			logger.debug("outProcTime : " + pastTime);
		}
		if (pastTime < 0) {
			return;
		}
		outProcMs.addAndGet(pastTime); // 接收消息处理时间增加
		outRequestCount.incrementAndGet();// 处理次数+1
		if (pastTime > outMaxProcMs) { // 最大值获取
			outMaxProcMs = pastTime;
		}
		outTotalTimes.incrementAndGet(); // 总次数+1
	}

	public void increaseSesionException() {
		netErrorTimes.incrementAndGet();
	}

	/**
	 * float bytes to M
	 */
	private static String FtoM(long v) {
		double ret_v = (double) v / 1024 / 1024;
		DecimalFormat numf = new DecimalFormat("0.##");
		return numf.format(ret_v).toString();
	}

	public static String getAppStatValue() {
		return appStatValue;
	}

	public static void setAppStatValue(String appStatValue) {
		DataStat.appStatValue = appStatValue;
	}
}
