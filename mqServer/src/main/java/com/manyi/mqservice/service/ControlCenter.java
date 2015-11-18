package com.manyi.mqservice.service;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.mina.core.session.IoSession;

import com.manyi.mqservice.model.MqMessage;
import com.manyi.mqservice.util.Configuration;
import com.manyi.mqservice.util.MqClassLoader;

public class ControlCenter {
	private static final Logger logger = Logger.getLogger(ControlCenter.class);
	/** 订阅所有TOPIC的标志 */
	public static final String TOPIC_ALL = "anything";
	/** MQ内部服务器TAG标识 */
	public static final String MQ_APP_TAG = "inner_mq";
	/** 消息队列(10w条) */
	private static ArrayBlockingQueue<MqMessage> blockQueue = new ArrayBlockingQueue<MqMessage>(300000, true);
	/** 工作线程组 */
	private static WorkThread[] runner = null;
	/** 初始化线程数 */
	private static int threadCount = Configuration.getInt("queue.thread.count", 10);
	/** 消息发布者连接容器 */
	private static ConcurrentHashMap<IoSession, Long> msgSenderSet = new ConcurrentHashMap<IoSession, Long>();
	/** 消息订阅者连接容器 */
	private static ConcurrentHashMap<IoSession, Long> msgSuberSet = new ConcurrentHashMap<IoSession, Long>();
	/** 订阅TOPIC和连接的对应关系 */
	private static ConcurrentHashMap<String, HashSet<IoSession>> msgTopicSuberMap = new ConcurrentHashMap<String, HashSet<IoSession>>();
	/** 消息接收服务端 */
	private static MessageServer msgReceiver;
	/** 集群内部链接管理器 */
	private static InnerConnector innerConn;
	/** 控制台服务端 */
	private static ConsoleServer console;
	/** 是否接收消息 */
	private static boolean acceptable = true;
	/** 是否存放至DB */
	private static boolean saveMsgToDB = true;
	/** 是否开启分应用统计 */
	private static boolean enableAppStat = true;
	/** 定时处理线程池 */
	private static ScheduledThreadPoolExecutor schedulePool = new ScheduledThreadPoolExecutor(200);

	/** 请求发送监控，用于监视服务端请求发送拥堵情况 */
	private static Thread requestSendMonitor = null;

	/**
	 * 启动服务
	 */
	public static void start() throws Exception {
		/* 初始化工作线程组 */
		initWorkThreads();
		/* 初始化内部链接 */
		initInnerConnector();
		/* 初始化控制台服务 */
		initConsoleServer();

		initRequestSendMonitor();
	}

	private static void initRequestSendMonitor() {
		requestSendMonitor = new Thread() {
			public void run() {
				while (!this.isInterrupted()) {
					try {
						Thread.sleep(60 * 1000);
					} catch (InterruptedException e) {
						this.interrupt();
					}
					monitorSuberSession();
				}
			}
		};
		requestSendMonitor.setDaemon(true);
		requestSendMonitor.start();
	}

	/**
	 * 监控处理订阅者的发送队列
	 */
	private static void monitorSuberSession() {
		for (IoSession session : getMsgSuberSet().keySet()) {
			int count = session.getScheduledWriteMessages();
			String ip = session.getRemoteAddress().toString();
			if (count > 200000) {
				logger.info("monitorSuberSession close block session " + ip + ":" + count);
				try {
					session.close(true);
				} catch (Throwable e) {
					logger.error("", e);
				}
			}
			logger.info("Suber session queue " + ip + ":" + count);
		}
	}

	/**
	 * 初始化MQ服务器间的内部订阅链接
	 */
	private static void initInnerConnector() {
		innerConn = new InnerConnector();
		innerConn.start();
	}

	/**
	 * 初始化控制台服务
	 */
	public static void initConsoleServer() {
		console = new ConsoleServer();
		console.start();
	}

	/**
	 * 初始化工作线程组
	 */
	private static void initWorkThreads() {
		runner = new WorkThread[threadCount];
		logger.info("init workThread : " + threadCount);
		for (int i = 0; i < runner.length; i++) {
			runner[i] = new WorkThread("worker-" + i);
			runner[i].start();
		}
		logger.info("init workThread finished");
	}

	/**
	 * 销毁工作线程组
	 */
	private static void destroyWorkThreads() {
		if (logger.isInfoEnabled()) {
			logger.info("try to wait queue.size() == 0");
		}
		// 等待队列任务数为0
		while (blockQueue.size() > 0) {
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
			}
		}
		if (logger.isInfoEnabled()) {
			logger.info("destroy workThread : " + threadCount);
		}
		for (int i = 0; i < runner.length; i++) {
			runner[i].aWaitInterrupt();
		}
		if (logger.isInfoEnabled()) {
			logger.info("destroy workThread finished");
		}
	}

	/**
	 * 关闭服务
	 */
	public static void shutdown() {
		// 关闭消息接收
		msgReceiver.shutdown();
		// 设置为不接收消息
		acceptable = false;

		innerConn.shutdown();

		console.shutdown();

		schedulePool.shutdown();
		try {
			schedulePool.awaitTermination(1, TimeUnit.MINUTES);
		} catch (Exception e) {
			// TODO: handle exception
		}
		destroyWorkThreads();
	}

	/**
	 * 热部署reload
	 */
	public static void hotReload() {
		try {
			MqClassLoader.reset();
			ProxyInvoker.initReloadClass();
		} catch (Exception e) {
			logger.error("", e);
		}
	}

	/**
	 * 延时执行任务
	 */
	public static ScheduledFuture<?> schedule(Runnable run, long time, TimeUnit unit) {
		return schedulePool.schedule(run, time, unit);
	}

	public static InnerConnector getInnerConnector() {
		return innerConn;
	}

	/** 工作线程类 */
	static class WorkThread extends Thread {
		private volatile boolean terminate = false;

		private volatile boolean hasTerminated = false;

		private final ReentrantLock runLock = new ReentrantLock();

		private Condition termination = runLock.newCondition();

		public WorkThread(String threadName) {
			super(threadName);
		}

		/**
		 * 等待，直至完成
		 */
		public void aWaitInterrupt() {
			try {
				runLock.lock();
				terminate = true;
				if (!hasTerminated) {
					termination.await();
				}
				this.interrupt();
			} catch (InterruptedException e) {
				logger.error("", e);
			} finally {
				runLock.unlock();
			}
		}

		public void run() {
			while (!this.isInterrupted()) {
				try {
					MqMessage msg = blockQueue.poll(1, TimeUnit.HOURS);
					if (msg == null) {
						continue;
					}
					if (logger.isDebugEnabled()) {
						logger.debug(Thread.currentThread().getName() + " got task:" + msg);
					}
					try {
						runLock.lock();
						ProxyInvoker.proxyMsgHandle("process", new Object[] { msg }, new Class[] { MqMessage.class });
						if (terminate) {
							hasTerminated = true;
							termination.signalAll();
						}
					} catch (Throwable e) {
						if (terminate) {
							termination.signalAll();
						}
					} finally {
						runLock.unlock();
					}
				} catch (InterruptedException e) {
					logger.error(Thread.currentThread().getName() + " is interrupted");
					break;
				} catch (Exception e) {
					logger.error("process msg error", e);
				} catch (Throwable e2) {
					logger.error("process msg error", e2);
				}
			}
		}
	}

	/**
	 * 将一条新的消息插入到消息队列
	 */
	public static void addMessage(MqMessage msg) {
		// 记录到内存Queue
		blockQueue.add(msg);
	}

	/**
	 * 注册消费者
	 */
	public static void registerMsgSender(IoSession session) {
		msgSenderSet.put(session, System.currentTimeMillis());
	}

	public static void unRegisterMsgSender(IoSession session) {
		msgSenderSet.remove(session);
	}

	/**
	 * 检查消息发送者的session有效性，并清理容器
	 */
	public static void checkMsgSender() {
		List<IoSession> removeList = new LinkedList<IoSession>();
		Iterator<IoSession> it = msgSenderSet.keySet().iterator();
		while (it.hasNext()) {
			IoSession sess = it.next();
			if (sess == null || sess.isClosing()) {
				removeList.add(sess);
			}
		}
		for (IoSession ioSession : removeList) {
			msgSenderSet.remove(ioSession);
		}
	}

	/**
	 * 检查消息订阅者的session有效性，并清理容器
	 */
	public static void checkMsgSuber() {
		List<IoSession> removeList = new LinkedList<IoSession>();
		Iterator<IoSession> it = msgSuberSet.keySet().iterator();
		while (it.hasNext()) {
			IoSession sess = it.next();
			if (sess == null || sess.isClosing()) {
				removeList.add(sess);
			}
		}
		for (IoSession ioSession : removeList) {
			msgSuberSet.remove(ioSession);
		}
	}

	public static ConcurrentHashMap<IoSession, Long> getMsgSenderSet() {
		return msgSenderSet;
	}

	public static ConcurrentHashMap<IoSession, Long> getMsgSuberSet() {
		return msgSuberSet;
	}

	public static ConcurrentHashMap<String, HashSet<IoSession>> getMsgTopicSuberMap() {
		return msgTopicSuberMap;
	}

	public static InnerConnector getInnerConn() {
		return innerConn;
	}

	public static ConsoleServer getConsole() {
		return console;
	}

	/**
	 * 注册订阅者
	 */
	public static void registerMsgSuber(IoSession session, String topics) {
		if (StringUtils.isEmpty(topics)) {
			throw new RuntimeException("cant register no topics subers");
		}
		msgSuberSet.put(session, System.currentTimeMillis());

		String[] topicArr = topics.trim().split(",");
		for (int i = 0; i < topicArr.length; i++) {
			HashSet<IoSession> suberSet = msgTopicSuberMap.get(topicArr[i]);
			if (suberSet == null) {
				suberSet = new HashSet<IoSession>();
				suberSet.add(session);
				msgTopicSuberMap.put(topicArr[i], suberSet);

			} else {
				suberSet.add(session);
			}
		}
	}

	/**
	 * 获取某一topic的订阅者列表
	 */
	public static List<IoSession> getTopicSubers(String topic) {
		HashSet<IoSession> ss = msgTopicSuberMap.get(topic);// 获取订阅这个topic的session列表
		List<IoSession> sessionList = new LinkedList<IoSession>();
		List<IoSession> removeList = new LinkedList<IoSession>();
		if (ss != null && ss.size() > 0) {
			for (Iterator<IoSession> iterator = ss.iterator(); iterator.hasNext();) {
				IoSession sess = (IoSession) iterator.next();
				if (sess != null && !sess.isClosing() && msgSuberSet.containsKey(sess)) {
					sessionList.add(sess);
				} else {
					removeList.add(sess);
				}
			}

			for (int i = 0; i < removeList.size(); i++) {
				IoSession dead = removeList.get(i);
				ss.remove(dead);
				msgSuberSet.remove(dead);
			}
			removeList.clear();
		}
		HashSet<IoSession> suballer = msgTopicSuberMap.get(TOPIC_ALL); // 获取订阅所有topic的session列表
		if (suballer != null && suballer.size() > 0) {
			for (Iterator<IoSession> iterator = suballer.iterator(); iterator.hasNext();) {
				IoSession sess = (IoSession) iterator.next();
				if (sess != null && !sess.isClosing() && msgSuberSet.containsKey(sess)) {
					sessionList.add(sess);
				} else {
					removeList.add(sess);
				}
			}
			for (int i = 0; i < removeList.size(); i++) {
				IoSession dead = removeList.get(i);
				suballer.remove(dead);
				msgSuberSet.remove(dead);
			}
			removeList.clear();
		}
		return sessionList;
	}

	public static void unRegisterMsgSuber(IoSession session) {
		msgSuberSet.remove(session);
	}

	/**
	 * 返回消息队列大小
	 */
	public static int getQueueSize() {
		return blockQueue.size();
	}

	public static void registerMessageServer(MessageServer receiver) {
		msgReceiver = receiver;
		msgReceiver.start();
	}

	public static boolean isAcceptable() {
		return acceptable;
	}

	public static void setAcceptable(boolean acceptable) {
		ControlCenter.acceptable = acceptable;
	}

	public static boolean isSaveMsgToDB() {
		return saveMsgToDB;
	}

	public static void setSaveMsgToDB(boolean saveMsgToDB) {
		ControlCenter.saveMsgToDB = saveMsgToDB;
	}

	public static MessageServer getMsgReceiver() {
		return msgReceiver;
	}

	public static boolean isEnableAppStat() {
		return enableAppStat;
	}

	public static void setEnableAppStat(boolean enableAppStat) {
		ControlCenter.enableAppStat = enableAppStat;
	}

}
