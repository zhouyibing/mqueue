package com.manyi.mqservice.reload;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.dao.DataAccessException;

import com.manyi.mqservice.model.Command;
import com.manyi.mqservice.model.RequestMessage;
import com.manyi.mqservice.model.ServerReply;
import com.manyi.mqservice.model.MqMessage;
import com.manyi.mqservice.service.ControlCenter;
import com.manyi.mqservice.util.MessageFilter;

public class MessageProcessor extends IoHandlerAdapter {

	private static final Logger logger = Logger.getLogger(MessageProcessor.class);
	/** 创建时间 */
	private static long classStartTime = System.currentTimeMillis();
	/** 客户端类型，消息发布者或者消息接收者 */
	private static final String SESSION_KEY_ROLER = "roler";
	/** 消息发布者 */
	private static final String ROLER_PUBLISHER = "pulisher";
	/** 消息接收者 */
	private static final String ROLER_SUBSCRIBER = "suber";

	/**
	 * 将消息对订阅者下发
	 */
	public void process(MqMessage msg) {
		List<IoSession> subers = ControlCenter.getTopicSubers(msg.getMsgTopic());
		if (subers == null || subers.size() == 0) {
			if (logger.isDebugEnabled()) {
				logger.debug("MsgUUID:" + msg.getMsgUID() + " find no subers!");
			}
			return;
		}
		for (IoSession session : subers) {
			try {
				String app = (String) session.getAttribute("app");
				boolean itsMQSuber = ControlCenter.MQ_APP_TAG.equals(app); // 判断该session是否是MQ
																			// 内部session
				if (msg.isFromMQ() && itsMQSuber) { // 如果消息是内部发出来的，并且是内部session则不发送
					continue;
				}
				session.setAttribute(MqMessage.TIMESTAMP_KEY, msg.getInitTime());
				session.write(Command.CMD_SEND_MSG + "#" + msg.toJSON().toString());
			} catch (Exception e) {
				logger.error("", e);
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug("processed : " + msg.getMsgUID());
		}
	}

	public long getClassStartTime() {
		return classStartTime;
	}

	/**
	 * session 打开
	 */
	public void sessionOpened(IoSession session) throws Exception {
		if (logger.isDebugEnabled()) {
			logger.debug("session closed:" + session.getRemoteAddress());
		}
	}

	/**
	 * session关闭
	 */
	public void sessionClosed(IoSession session) throws Exception {
		if (logger.isDebugEnabled()) {
			logger.debug("session closed:" + session.getRemoteAddress());
		}
		String rolerName = (String) session.getAttribute(SESSION_KEY_ROLER);
		if (rolerName != null && rolerName.equals(ROLER_PUBLISHER)) { // 消息发布者 session，主要统计从接收到消息 到给出响应。
			ControlCenter.unRegisterMsgSender(session);
		} else if (rolerName != null && rolerName.equals(ROLER_SUBSCRIBER)) {// 消息订阅者sesion,统计从消息对象的产生 到 下发给具体的订阅者。
			ControlCenter.unRegisterMsgSuber(session);
		}

	}

	public void sessionIdle(IoSession session, IdleStatus status) throws Exception {
		if (logger.isDebugEnabled()) {
			logger.debug("session idle:" + session.getRemoteAddress());
		}
	}

	/**
	 * 捕获到异常
	 */
	public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
		String app = (String) session.getAttribute("app");
		logger.error("Exception caught from:" + session.getRemoteAddress() + ", app:" + app, cause);
		DataStat ds = new DataStat();
		ds.increaseSesionException();
	}

	/**
	 * 接收到消息
	 */
	public void messageReceived(IoSession session, Object msgData) throws Exception {
		ServerReply reply = new ServerReply();
		if (logger.isDebugEnabled()) {
			logger.debug(System.currentTimeMillis() + " msg received:" + msgData + " from "
					+ session.getRemoteAddress());
		}
		String msg = (String) msgData;
		int pos = msg.indexOf("#");
		String cmd = null;
		JSONObject body = null;
		if (pos >= 0) {
			cmd = msg.substring(0, pos).trim();
			body = (JSONObject) JSONValue.parse(msg.substring(pos + 1).trim());
		} else {
			cmd = msg;
		}
		if (Command.CMD_SEND_MSG.equals(cmd)) {// 接收到消息发布者发布的消息
			reply.setCmd("msgReceived");
			String rolerName = (String) session.getAttribute(SESSION_KEY_ROLER);
			if (rolerName == null) { // 连接者角色名称检查
				reply.setMessage("havn't login yet!");
				session.write(reply);
				return;
			}
			String app = (String) session.getAttribute("app");
			if (app == null) { // 验证app
				reply.setMessage("no such app");
				session.write(reply);
				return;
			}
			if (body == null) {// 验证数据包
				reply.setMessage("the message body can't be null!");
				session.write(reply);
				return;
			}

			MqMessage message = ControlCenter.getMsgReceiver().packMessage(body); // 验证消息内容打包成对象

			if (message == null) {
				reply.setMessage("the message body doesn't contain msgTopic and msgType!");
				session.write(reply);
				return;
			}

			if (MessageFilter.filter(message.getMsgUID())) {// 如果是被过滤的消息
				logger.info("filtered message:" + message.getMsgUID());
				reply.setStatus(1);
				reply.setMessage(message.getMsgUID());
				session.write(reply);
				return;
			}
			/*
			 * 统计一次app消息请求
			 */
			if (ControlCenter.isEnableAppStat()) {
				DataStat ds = new DataStat();
				ds.incRequestCount(app);
			}

			try {
				logger.info(session.getRemoteAddress() + "SEND:" + body);
				ControlCenter.getMsgReceiver().receiveMessage(message);
				reply.setStatus(1);
				reply.setMessage(message.getMsgUID());
			} catch (DataAccessException e) { // 保存至数据库失败，但是还是给予反馈说收到了数据
				DataStat ds = new DataStat();
				ds.increaseSesionException(); // 告知监控出现一次数据库操作异常
				reply.setStatus(1);
				reply.setMessage(message.getMsgUID());
				logger.error("error for add message to db", e);
			} catch (Exception e) {
				reply.setStatus(0);
				reply.setMessage(e.toString());
				logger.error("error receiving messasge", e);
			}
			session.write(reply);

		} else if (Command.CMD_SENDER_LOGIN.equals(cmd)) { // 消息发布者连接
			reply.setCmd("result");
			boolean canLogin = false;
			if (body != null) {
				String app = (String) body.get("app");
				if (app != null) {
					session.setAttribute("app", app);
					canLogin = true;
				}
			}
			if (!canLogin) {
				reply.setStatus(0);
				reply.setMessage("app not set correctly");
				session.write(reply);
				session.close(true); // 主动关闭非法请求连接
			} else {
				session.setAttribute(SESSION_KEY_ROLER, ROLER_PUBLISHER); // 标识该session为消息发布者
				reply.setStatus(1);
				reply.setMessage("login success");
				session.write(reply);
				ControlCenter.registerMsgSender(session);
			}
			return;
		} else if (Command.CMD_SUBER_LOGIN.equals(cmd)) { // 消息订阅者登录
			reply.setCmd("result");
			boolean canLogin = false;
			String topics = null;
			if (body != null) {
				String app = (String) body.get("app");
				topics = (String) body.get("msgTopic");
				if (app != null && topics != null) {
					session.setAttribute("app", app);
					canLogin = true;
				}
			}
			final int minutes = (body == null || body.get("minutes") == null) ? 0 : ((Long) body.get("minutes"))
					.intValue();
			if (canLogin) {
				final String subbedTopics = topics;
				session.setAttribute(SESSION_KEY_ROLER, ROLER_SUBSCRIBER);
				// 如果minutes > 0， 从数据库获取最新的消息对其进行单独下发
				// 为了加强消息接收能力，将数据库持久化这一操作异步处理。但是这样会导致要求重发的时候，数据可能尚未持久到数据库
				// 采用延迟3秒钟后进行获取要重发的数据，降低因上面原因带来的漏发概率。
				// 系统无法做到100%不丢消息，只能尽可能地降低逻辑问题引起的丢失。
				if (minutes > 0) {
					final IoSession sess = session;
					Runnable run = new Runnable() {
						public void run() {
							final Iterator<MqMessage> msgList = ControlCenter.getMsgReceiver()
									.getMessagesBeforeByTopic(minutes, subbedTopics);
							if (logger.isInfoEnabled()) {
								logger.info("start to send history data to sess:" + sess.getRemoteAddress());
							}
							while (msgList.hasNext()) {
								MqMessage tdMessage = msgList.next();
								// 组装命令对象
								RequestMessage req = new RequestMessage();
								req.setCmd(Command.CMD_SEND_MSG);
								req.setBody(tdMessage.toJSON());
								String sendData = req.toCmd();
								sess.write(sendData);
							}
							if (logger.isInfoEnabled()) {
								logger.info("finished to send history data to sess:" + sess.getRemoteAddress());
							}
						}
					};
					ControlCenter.schedule(run, 3000, TimeUnit.MILLISECONDS);
				}
				reply.setStatus(1);
				reply.setMessage("login success");
				session.write(reply);
				ControlCenter.registerMsgSuber(session, topics);
			} else {
				reply.setStatus(0);
				reply.setMessage("suber params not set correctly");
				session.write(reply);
				session.close(true); // 主动关闭非法请求连接
			}

		} else if (Command.CMD_CLOSE.equals(cmd)) {
			String app = (String) session.getAttribute("app");
			String ip = session.getRemoteAddress().toString();
			session.close(true);
			session.getCloseFuture().awaitUninterruptibly();
			logger.info("app:" + app + " from :" + ip + " close connection!");
		} else {
			session.write("i dont know what's ur saying! closing now!!");
			session.close(true);
			session.getCloseFuture().awaitUninterruptibly();
		}

	}

	/**
	 * 消息下发完成后被调用的方法
	 */
	public void messageSent(IoSession session, Object message) throws Exception {
		if (logger.isDebugEnabled()) {
			logger.debug("msg sent:" + message);
		}
		String rolerName = (String) session.getAttribute(SESSION_KEY_ROLER);
		if (rolerName != null && rolerName.equals(ROLER_PUBLISHER)) { // 消息发布者 session，主要统计从接收到消息 到给出响应。
			if (message instanceof ServerReply) {
				ServerReply reply = (ServerReply) message;
				DataStat ds = new DataStat();
				ds.caculateInProcTime(reply.getProcStartTime());
			}
		} else if (rolerName != null && rolerName.equals(ROLER_SUBSCRIBER)) {// 消息订阅者sesion,统计从消息对象的产生 到 下发给具体的订阅者。
			Long startTime = (Long) session.getAttribute(MqMessage.TIMESTAMP_KEY);
			if (startTime != null) {
				DataStat ds = new DataStat();
				ds.caculateOutProcTime(startTime);
			}
		}
	}

}
