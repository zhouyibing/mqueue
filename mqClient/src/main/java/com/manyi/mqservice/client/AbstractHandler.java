package com.manyi.mqservice.client;

import java.net.SocketAddress;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.manyi.mqservice.model.Command;
import com.manyi.mqservice.model.RequestMessage;
import com.manyi.mqservice.model.MqMessage;

/**
 * 客户端订阅者需要实现这一抽象类的抽象方法
 */
public abstract class AbstractHandler extends IoHandlerAdapter {

	private static final Logger logger = Logger.getLogger(AbstractHandler.class);
	private static ThreadPoolExecutor schedulePool = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);

	public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
		logger.error("connection:" + session.getRemoteAddress().toString(), cause);
	}

	/**
	 * 改方法已经封装好，不允许重载
	 */
	public final void messageReceived(final IoSession session, Object message) throws Exception {
		RequestMessage req = (RequestMessage) session.getAttribute("req");
		String msg = (String) message;
		if (req != null) {
			req.anounceResp(msg); // 通知接收到了回复
		}
		if (logger.isInfoEnabled()) {
			logger.info(session.getRemoteAddress() + " receive:" + msg);
		}
		int pos = msg.indexOf("#");
		if (pos == -1) {
			return;
		}
		String cmd = msg.substring(0, pos);
		String body = msg.substring(pos + 1);
		if ("result".equals(cmd)) {
			return;
		} else if (Command.CMD_SEND_MSG.equals(cmd)) {
			final MqMessage mqMessage = packMessage(body);
			Runnable command = new Runnable() {
				public void run() {
					processMessage(mqMessage, session.getServiceAddress());
				}
			};
			schedulePool.execute(command);
		}
	}

	/**
	 * 处理消息的方法,这里的消息是按照消息顺序处理，为了提高效率，建议在方法中使用线程池的方式处理业务 确保每条消息都能很快地消化掉
	 */
	public abstract void processMessage(MqMessage message, SocketAddress host);

	/**
	 * 将字符串打包成MqMessage对象
	 */
	private MqMessage packMessage(String s) {
		if (StringUtils.isEmpty(s)) {
			return null;
		}
		try {
			// 组装成消息对象
			MqMessage message = new MqMessage();

			JSONObject msg = (JSONObject) JSONValue.parse(s);
			String msgTopic = (String) msg.get("msgTopic");
			String msgType = (String) msg.get("msgType");
			long objectId = Long.valueOf(msg.get("objectId") + "");
			int userId = Integer.valueOf(msg.get("userId") + "");
			Object aTime = msg.get("addTime");
			if (aTime != null) {
				message.setAddTime(new Date(Long.valueOf(aTime + "")));
			}
			String memo = (String) msg.get("memo");
			String ip = msg.get("ip") + "";
			String msgUID = (String) msg.get("msgUID");

			message.setMsgTopic(msgTopic);
			message.setMsgType(msgType);
			message.setObjectId(objectId);
			message.setUserId(userId);
			message.setMemo(memo);
			message.setIp(Integer.valueOf(ip));
			message.setMsgUID(msgUID);
			return message;
		} catch (Exception e) {
			logger.error("error packing for the body content:" + s, e);
			return null;
		}
	}

	public void messageSent(IoSession session, Object message) throws Exception {
		if (logger.isInfoEnabled()) {
			logger.info("send:" + message);
		}
	}

}
