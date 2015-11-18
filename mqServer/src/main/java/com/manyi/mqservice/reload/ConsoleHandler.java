package com.manyi.mqservice.reload;

import java.security.MessageDigest;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;

import com.manyi.mqservice.service.ControlCenter;
import com.manyi.mqservice.service.ProxyInvoker;
import com.manyi.mqservice.util.Configuration;

public class ConsoleHandler extends IoHandlerAdapter {
	/** log4j logger */
	private static final Logger logger = Logger.getLogger(ConsoleHandler.class);

	public void messageReceived(IoSession session, Object msgData) throws Exception {
		if (logger.isInfoEnabled()) {
			logger.info("console cmd received:" + msgData + " from " + session.getRemoteAddress());
		}
		String msg = (String) msgData;
		msg = msg.trim();
		int step = (Integer) session.getAttribute("step");
		if (step == 1) { // 尚未登录
			if (md5(msg).equals(Configuration.getString("server.console.password"))) {
				session.write("login success, welcome administrator!\r");
				session.setAttribute("step", 2);
				if (logger.isInfoEnabled()) {
					logger.info(session.getRemoteAddress() + "  console login success!");
				}
				return;
			} else {
				session.write("plz input the password!");
				return;
			}
		}
		// 解析命令行
		if (StringUtils.isEmpty(msg)) {
			session.write("please input command, [stat subers senders ...]\r");
			return;
		}
		int pos = msg.indexOf(" ");
		String cmd = null;
		String body = null;
		if (pos > 0) {
			cmd = msg.substring(0, pos).trim();
			body = msg.substring(pos + 1).trim();
		} else {
			cmd = msg;
		}
		if ("connect".equals(cmd)) { // 手工连接MQ内部服务器
			int p = body.indexOf(":");
			String ip = body.substring(0, p).trim();
			int port = Integer.valueOf(body.substring(p + 1).trim());
			if (ControlCenter.getInnerConnector().isConnected(ip, port)) {// 看是否已经连接过
				session.write("ip:" + ip + ", port:" + port + " already connected in:"
						+ ControlCenter.getInnerConnector().getLastConnectedTime(ip, port));
			} else {
				ControlCenter.getInnerConnector().disConnect(ip, port); // 尝试关闭连接
				ControlCenter.getInnerConnector().connect(ip, port, 200);// 重新连接
				if (ControlCenter.getInnerConnector().isConnected(ip, port)) {
					session.write("connected to ip:" + ip + " successfully!");
				} else {
					ControlCenter.getInnerConnector().disConnect(ip, port);
					session.write("connected to ip:" + ip + " failed, will disconnected!");
				}
			}
		} else if ("disconnect".equals(cmd)) {// 手工断开MQ内部服务器
			int p = body.indexOf(":");
			String ip = body.substring(0, p).trim();
			int port = Integer.valueOf(body.substring(p + 1).trim());
			session.write("try to disconnect:" + ip + " : " + port);
			ControlCenter.getInnerConnector().disConnect(ip, port); // 尝试关闭连接
			if (!ControlCenter.getInnerConnector().isConnected(ip, port)) {
				session.write("disconnect success!");
			}
		} else if ("stat".equals(cmd)) {// 输出统计信息
			String data = ProxyInvoker.getCaculateData();
			if (data == null) {
				session.write("there's no data yet!");
			} else {
				session.write(data);
			}
		} else if ("reload".equals(cmd)) {// 重新加载业务逻辑类, reload包下的程序
			String statorDate = ProxyInvoker.getStatorClassStartTime();
			String procorDate = ProxyInvoker.getProcessorClassStartTime();
			session.write("Before Date: \n\rstator:" + statorDate + "\n\r" + "processor:" + procorDate);
			ControlCenter.hotReload();
			statorDate = ProxyInvoker.getStatorClassStartTime();
			procorDate = ProxyInvoker.getProcessorClassStartTime();
			session.write("\n\rAfter Date:\n\rstator:" + statorDate + "\n\r" + "processor:" + procorDate);
		} else if ("dbsave".equals(cmd)) {// 是否保存数据到数据库
			session.write("current:" + ControlCenter.isSaveMsgToDB());
			if (body != null) {
				if ("true".equals(body)) {
					ControlCenter.setSaveMsgToDB(true);
				} else if ("false".equals(body)) {
					ControlCenter.setSaveMsgToDB(false);
				}

			}
			session.write("now:" + ControlCenter.isSaveMsgToDB() + "\n\r");
		} else if ("enableappstat".equals(cmd)) {// 是否允许分应用统计消息量
			session.write("current:" + ControlCenter.isEnableAppStat());
			if (body != null) {
				if ("true".equals(body)) {
					ControlCenter.setEnableAppStat(true);
				} else if ("false".equals(body)) {
					ControlCenter.setEnableAppStat(false);
				}

			}
			session.write("now:" + ControlCenter.isEnableAppStat() + "\n\r");
		} else if ("senders".equals(cmd)) {// 消息发送者列表
			StringBuilder sb = new StringBuilder();
			ConcurrentHashMap<IoSession, Long> senders = ControlCenter.getMsgSenderSet();
			Iterator<IoSession> it = senders.keySet().iterator();
			while (it.hasNext()) {
				IoSession sess = it.next();
				if (sess != null) {
					String app = (String) sess.getAttribute("app");
					sb.append(app);
					sb.append(", ip :");
					sb.append(sess.getRemoteAddress().toString());
					sb.append(" connected in :");
					sb.append(new Date(senders.get(sess)).toString());
					sb.append("\n\r");
				}
			}
			if (sb.length() > 0) {
				session.write(sb.toString());
			} else {
				session.write("there's no senders!\n\r");
			}
		}

		else if ("subers".equals(cmd)) { // 消息接收者列表
			StringBuilder sb = new StringBuilder();
			ConcurrentHashMap<IoSession, Long> subers = ControlCenter.getMsgSuberSet();
			Iterator<IoSession> it = subers.keySet().iterator();
			while (it.hasNext()) {
				IoSession sess = it.next();
				if (sess != null) {
					String app = (String) sess.getAttribute("app");
					sb.append(app);
					sb.append(", ip :");
					sb.append(sess.getRemoteAddress().toString());
					sb.append(" connected in :");
					sb.append(new Date(subers.get(sess)).toString());
					sb.append("\n\r");
				}
			}
			if (sb.length() > 0) {
				session.write(sb.toString());
			} else {
				session.write("there's no subers!\n\r");
			}
		} else if ("appstat".equals(cmd)) { // 输出分应用统计数据
			String data = DataStat.getAppStatValue();
			if (data == null) {
				session.write("there's no app info \n\r");
			} else {
				session.write(data + "\n\r");
			}
		} else if ("quit".equals(cmd)) { // 退出
			session.write("closing session!\n\r");
			session.close(true);
			session.getCloseFuture().awaitUninterruptibly();
			if (logger.isInfoEnabled()) {
				logger.info(session.getRemoteAddress() + " quit console login!");
			}
		} else {
			session.write("unknown command");
		}
	}

	/** 16位字符数组 */
	private static char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	/**
	 * md5加密
	 */
	private static String md5(String str) {
		try {
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			StringBuilder newstr = new StringBuilder();
			md5.update(str.getBytes("utf-8"));
			byte[] bytes = md5.digest();
			for (byte b : bytes) {
				char ch1 = hexDigits[b >>> 4 & 0xf];
				char ch2 = hexDigits[b & 0xf];
				newstr.append(ch1);
				newstr.append(ch2);
			}
			return newstr.toString();
		} catch (Exception e) {
			return null;
		}
	}

	public static void main(String[] argv) {
		System.out.println(md5("123456"));
	}
}
