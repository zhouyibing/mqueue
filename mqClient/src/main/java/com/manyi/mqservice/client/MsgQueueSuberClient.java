package com.manyi.mqservice.client;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.buffer.SimpleBufferAllocator;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.SocketConnector;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

import com.manyi.mqservice.model.Command;
import com.manyi.mqservice.model.RequestMessage;
import com.manyi.mqservice.model.ServerReply;

/**
 * 消息订阅者客户端
 */
public class MsgQueueSuberClient extends BaseClient {
	private static final Logger logger = Logger.getLogger(MsgQueueSuberClient.class);
	private String subTopics; // 订阅的消息列表，逗号分隔
	private long minutesBefore; // 获取多少分钟之前的消息
	private AbstractHandler handler;// 处理器
	private IoSession session;
	private Thread monitor;
	private int tryConnectTimes;
	private long lastLoginTime;
	private long lastActiveTime = System.currentTimeMillis();;

	public IoSession getSession() {
		return session;
	}

	public void setSession(IoSession session) {
		this.session = session;
	}

	public MsgQueueSuberClient() {
		monitor = new Thread() {
			public void run() {
				try {
					Thread.sleep(10000);
					while (true) {
						if (this.isInterrupted()) {
							break;
						}
						if (isActive()) {
							lastActiveTime = System.currentTimeMillis(); // 设置最近一次连接有效时间
							Thread.sleep(5000); // 5秒检查一次
							if (logger.isDebugEnabled()) {
								logger.debug("suberclient is alive...");
							}
						} else {
							if (logger.isInfoEnabled()) {
								logger.info("suberclient isn't alive, restarting...");
							}
							newConnection();
							Thread.sleep(10000);
						}
					}
				} catch (Exception e) {
					logger.error("", e);
				}
			}
		};
		monitor.setDaemon(true);
		monitor.start();
	}

	/**
	 * 关闭这个订阅客户端
	 */
	public void shutdown() {
		if (monitor != null) {
			monitor.interrupt();
		}
		if (session != null && session.isConnected()) {
			session.close(true);
			session.getCloseFuture().awaitUninterruptibly();
		}
	}

	/**
	 * 检查连接是否有效
	 */
	public boolean isActive() {
		return session != null && session.isConnected(); 
	}

	/**
	 * 创建新连接 遍历集群服务器列表，连上其中一台后返回
	 */
	public void newConnection() {
		if (this.getLastLoginTime() > 0) { // 登陆成功过, 属于自动重连 , 重新计算要告诉服务端的消息重发时间
			long millis = System.currentTimeMillis() - this.lastActiveTime; // 当前时间减去最近一次检查存活时间
			long minutes = millis / (1000 * 60);
			if (millis % 60000 > 0) {
				minutes = minutes + 1;
			}
			minutes++; // 推前1分钟
			this.setMinutesBefore(minutes);
		}
		for (int i = 0; i < this.getServersCount(); i++) {
			ServerInfo info = this.getServerInfo(i);
			try {
				connect(info.host, info.port);
				if (this.isActive()) {
					return;
				}
			} catch (Exception e) {
				logger.error("", e);
			}
		}
	}

	public void connect(String host, int port) {
		tryConnectTimes++;
		if (StringUtils.isEmpty(getApp()) || StringUtils.isEmpty(subTopics) || StringUtils.isEmpty(host) || port == 0) {
			throw new RuntimeException(this.getClass().getName() + " can not start for the empty params!");
		}

		SocketAddress srvAddr = new InetSocketAddress(host, port);
		SocketConnector connector = new NioSocketConnector(Runtime.getRuntime().availableProcessors() + 1);

		connector.getFilterChain().addLast("codec", new ProtocolCodecFilter(new CodecFactory())); // 协议
		connector.setHandler(handler);
		IoBuffer.setUseDirectBuffer(false);
		IoBuffer.setAllocator(new SimpleBufferAllocator());
		ConnectFuture cf = connector.connect(srvAddr);
		cf.awaitUninterruptibly();
		if (!cf.isConnected()) {
			connector.dispose();
			logger.error("failed to connect to server--" + host + ": " + port);
			return;
		}
		IoSession session = cf.getSession();
		boolean succ = login(session, getTimeout());
		if (succ) {
			if (logger.isInfoEnabled()) {
				logger.info("success to connect to server--" + host + ": " + port);
			}
			this.session = session;
			lastLoginTime = System.currentTimeMillis();
			return;
		} else {
			session.close(true);
			session.getCloseFuture().awaitUninterruptibly();
			session = null;
			logger.error("failed to login to server" + host + ": " + port);
			return;
		}
	}

	/**
	 * 初次连接进行登录
	 */
	private boolean login(IoSession session, long millis) {
		RequestMessage req = new RequestMessage();
		req.setCmd(Command.CMD_SUBER_LOGIN);
		req.setBodyField("app", getApp());
		req.setBodyField("msgTopic", this.subTopics);
		req.setBodyField("minutes", this.minutesBefore);
		session.setAttribute("req", req);
		String sendData = req.toCmd();
		session.write(sendData);
		if (logger.isInfoEnabled()) {
			logger.info("send request:" + sendData);
		}
		String resp = null;
		try {
			//阻塞到服务器响应，通过handler messageReceived中的anounceResp来通知响应到达，内部是countdownlatch实现
			resp = req.getResponse(millis, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
		}
		if (resp == null) {
			logger.error("login failed for server is busy!");
			return false;
		} else {
			ServerReply reply = new ServerReply(resp);
			if (reply.getStatus() == 1) {
				return true;
			} else {
				return false;
			}
		}
	}

	public AbstractHandler getHandler() {
		return handler;
	}

	/**
	 * 连接在sethandler方法调用后进行
	 */
	public void setHandler(AbstractHandler handler) {
		this.handler = handler;
		newConnection();
	}

	public String getSubTopics() {
		return subTopics;
	}

	public void setSubTopics(String subTopics) {
		this.subTopics = subTopics;
	}

	public long getMinutesBefore() {
		return minutesBefore;
	}

	public void setMinutesBefore(long minutesBefore) {
		this.minutesBefore = minutesBefore;
	}

	public int getTryConnectTimes() {
		return tryConnectTimes;
	}

	public void setTryConnectTimes(int tryConnectTimes) {
		this.tryConnectTimes = tryConnectTimes;
	}

	public long getLastLoginTime() {
		return lastLoginTime;
	}

	public void setLastLoginTime(long lastLoginTime) {
		this.lastLoginTime = lastLoginTime;
	}

	public long getLastActiveTime() {
		return lastActiveTime;
	}

	public void setLastActiveTime(long lastActiveTime) {
		this.lastActiveTime = lastActiveTime;
	}
}
