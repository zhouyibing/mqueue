package com.manyi.mqservice.client;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.SocketConnector;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

import com.manyi.mqservice.model.Command;
import com.manyi.mqservice.model.RequestMessage;
import com.manyi.mqservice.model.ServerReply;
import com.manyi.mqservice.model.MqMessage;

/**
 * 消息发布者客户端
 */
public class MsgQueueSenderClient extends BaseClient {

	private static final Logger logger = Logger.getLogger(MsgQueueSenderClient.class);

	/** 未发送成功的消息队列(10000条) */
	private ArrayBlockingQueue<MqMessage> unsendedQueue = new ArrayBlockingQueue<MqMessage>(10000, true);
	/** 处理未发送成功的线程 */
	private Thread monitor = null;
	/** 连接对象池 */
	private GenericObjectPool connectionPool = new GenericObjectPool(new SessionPoolObjectFactory());

	private static ScheduledThreadPoolExecutor schedulePool = new ScheduledThreadPoolExecutor(10);

	/** 发送池大小 */
	private int maxActive = 8;

	public MsgQueueSenderClient() {
		connectionPool.setTestOnBorrow(true); // 借对象时候针对有效性测试
		connectionPool.setMaxWait(1000); // 从对象池获取对象最多等待1000毫秒
		connectionPool.setMaxActive(maxActive);
		connectionPool.setMinIdle(2);
		connectionPool.setMaxIdle(4);
		startUnsendedMonitor();
	}

	/**
	 * 启动处理未处理成功消息的线程
	 */
	private void startUnsendedMonitor() {
		if (monitor != null && monitor.isAlive()) {
			monitor.interrupt();
			monitor = null;
		}
		monitor = new Thread() {
			public void run() {
				while (true && !isInterrupted()) {
					try {
						MqMessage msg = unsendedQueue.poll(1, TimeUnit.HOURS);
						if (msg == null) {
							continue;
						}
						if (msg.getProcessTimes() > 5) { // 超过五次丢弃该消息
							if (logger.isInfoEnabled()) {
								logger.info("msgId:" + msg.getMsgUID() + " reaches 5 proc times, give up!");
							}
							continue;
						}
						msg.setProcessTimes(msg.getProcessTimes() + 1);
						sendMessage(msg);
					} catch (Exception e) {
						logger.error("", e);
					}
				}
			}
		};
		monitor.setDaemon(true);
		monitor.start();
	}

	/**
	 * 延时5s加入重新发送队列
	 */
	private void addUnsendedMessage(final MqMessage msg) {
		Runnable task = new Runnable() {
			public void run() {
				try {
					if (logger.isInfoEnabled()) {
						logger.info("msg:" + msg.getMsgUID() + " add to unsended queue!");
					}
					unsendedQueue.add(msg);
				} catch (Exception e) {
					logger.error("msg:" + msg.getMsgUID() + " add to unsended queue failed for queue is full!");
				}
			}
		};
		schedulePool.schedule(task, 5000, TimeUnit.MILLISECONDS);
	}

	/**
	 * 创建新连接 从服务器列表中遍历，获取有效的连接
	 */
	public Connection newConnection() {

		for (int i = 0; i < this.getServersCount(); i++) {
			ServerInfo info = this.getServerInfo(i);
			try {
				Connection con = connect(info.host, info.port);
				if (con != null) {
					return con;
				}
			} catch (Exception e) {
				logger.error("", e);
			}
		}
		return null;
	}

	public Connection connect(String ip, int port) {
		SocketAddress srvAddr = new InetSocketAddress(ip, port);
		SocketConnector connector = new NioSocketConnector(Runtime.getRuntime().availableProcessors() + 1);
		connector.getFilterChain().addLast("codec", new ProtocolCodecFilter(new CodecFactory())); // 协议
		connector.setHandler(new CHandler());
		ConnectFuture cf = connector.connect(srvAddr);
		cf.awaitUninterruptibly();
		if (!cf.isConnected()) {
			connector.dispose();
			logger.error("failed to connect to server," + ip + ": " + port);
			return null;
		}
		IoSession session = cf.getSession();
		boolean succ = login(session, this.getTimeout());
		if (succ) {
			Connection con = new Connection();
			con.session = session;
			con.connector = connector;
			return con;
		} else {
			logger.error("failed to login to server," + ip + ": " + port);
			return null;
		}
	}

	/**
	 * 发送消息，并等待返回
	 */
	public boolean sendMessage(MqMessage msg) {
		if (msg.getMsgUID() == null) {
			// 创建UUID
			String uuid = UUID.randomUUID().toString().replaceAll("-", "");
			msg.setMsgUID(uuid);
		}
		if (connectionPool.getMaxActive() <= connectionPool.getNumActive()) {
			addUnsendedMessage(msg);
			return false;
		}
		Connection con = null;
		try {
			// 从连接池获得连接
			con = (Connection) connectionPool.borrowObject();
			if (con == null) {
				addUnsendedMessage(msg); // 网络连接断开 , 加入消息未发送成功队列
				logger.error("havn't connect to server yet!");
				return false;
			}
			// 组装命令对象
			RequestMessage req = new RequestMessage();
			req.setCmd(Command.CMD_SEND_MSG);
			req.setBody(msg.toJSON());
			IoSession session = con.session;
			session.setAttribute("req", req);
			// 发送命令请求
			String sendData = req.toCmd();
			session.write(sendData);
			String resp = null;
			// 等待数据返回
			try {
				resp = req.getResponse(this.getTimeout(), TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
			}
			if (resp != null) {
				ServerReply reply = new ServerReply(resp);
				if (reply.getStatus() == 1) {
					if (logger.isInfoEnabled()) {
						logger.info("jmq data send succ:" + reply.getMessage());
					}
					return true;
				} else { // 这里的发送失败是服务端告知的，消息格式不合理等问题，不用重发
					if (logger.isInfoEnabled()) {
						logger.info("jmq data send fail:" + reply.getMessage());
					}
					return false;
				}
			} else {
				addUnsendedMessage(msg); // 超时错误 , 加入消息未发送成功队列
				logger.error("jmq data send fail for timeout:" + msg.getMsgUID());
				return false;
			}
		} catch (Exception e) {
			logger.error("sendMessage error:", e);
			addUnsendedMessage(msg); // 网络连接断开 , 加入消息未发送成功队列
			return false;
		} finally {
			try {
				connectionPool.returnObject(con);
			} catch (Exception e) {
				logger.error("connectionPool.returnObject error:", e);
			}
		}
	}

	/**
	 * 初次连接进行登录
	 */
	private boolean login(IoSession session, long millis) {
		RequestMessage req = new RequestMessage();
		req.setCmd(Command.CMD_SENDER_LOGIN);
		req.setBodyField("app", this.getApp());
		session.setAttribute("req", req);
		String sendData = req.toCmd();
		session.write(sendData);
		String resp = null;
		try {
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

	private static int ConnectObjId = 0;

	class Connection {
		int id = 0;
		IoSession session;
		SocketConnector connector;

		Connection() {
			id = ++ConnectObjId;
		}

		boolean isValidate() {
			if (session != null && connector != null) {
				return session.isConnected();
			}
			return false;
		}

		void quit() {
			// IoSession session = getSession();
			if (session != null) {
				if (session.isConnected()) {
					logger.info("closeme id=" + id + ", timeout=" + connectionPool.getMaxWait() + ", NumActive="
							+ connectionPool.getNumActive());
					session.write("closeme");
					session.getCloseFuture().awaitUninterruptibly();
				}
				session.close(true);
				session = null;
			}
			connector = null;

		}
	}

	class SessionPoolObjectFactory implements PoolableObjectFactory {

		@Override
		public void activateObject(Object obj) throws Exception {

		}

		@Override
		public void destroyObject(Object obj) throws Exception {
			Connection con = (Connection) obj;
			if (con != null) {
				con.quit();
				con = null;
			}
		}

		@Override
		public Object makeObject() throws Exception {
			Connection con = newConnection();
			return con;
		}

		@Override
		public void passivateObject(Object obj) throws Exception {
		}

		@Override
		public boolean validateObject(Object obj) {
			Connection con = (Connection) obj;
			if (con != null) {
				return con.isValidate();
			}
			return false;
		}

	}

	static class CHandler extends IoHandlerAdapter {

		public void sessionCreated(IoSession session) throws Exception {

		}

		public void sessionOpened(IoSession session) throws Exception {
			if (logger.isInfoEnabled()) {
				logger.info("session opened to:" + session.getRemoteAddress());
			}
		}

		public void sessionClosed(IoSession session) throws Exception {
			if (logger.isInfoEnabled()) {
				logger.info("session closed to:" + session.getRemoteAddress());
			}
		}

		public void sessionIdle(IoSession session, IdleStatus status) throws Exception {
		}

		public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
			if (logger.isInfoEnabled())
				logger.info(cause.getMessage(), cause);
		}

		public void messageReceived(IoSession session, Object message) throws Exception {
			if (logger.isInfoEnabled()) {
				logger.info("receive answer:" + message);
			}
			RequestMessage req = (RequestMessage) session.getAttribute("req");
			if (req != null) {
				req.anounceResp((String) message);
			}
		}

		public void messageSent(IoSession session, Object message) throws Exception {
			if (logger.isInfoEnabled()) {
				logger.info("send Request:" + message);
			}
		}
	}

	public int getMaxActive() {
		return maxActive;
	}

	public void setMaxActive(int maxActive) {
		this.maxActive = maxActive;
		if (connectionPool != null) {
			connectionPool.setMaxActive(maxActive);
		}
	}

	public void setTimeout(long timeout) {
		super.setTimeout(timeout);
		if (connectionPool != null) {
			connectionPool.setMaxWait(timeout);
		}
	}

}