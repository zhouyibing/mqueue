package com.manyi.mqservice.service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.buffer.SimpleBufferAllocator;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.transport.socket.SocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.json.simple.JSONObject;
import org.springframework.stereotype.Service;

import com.manyi.mqservice.client.CodecFactory;
import com.manyi.mqservice.dao.MsgDAO;
import com.manyi.mqservice.model.MqMessage;
import com.manyi.mqservice.util.Configuration;

@Service
public class MessageServer {

    private static final Logger logger = Logger.getLogger(MessageServer.class);

    /** 操作队列数据库的dao */
    @Resource
    private MsgDAO msgDAO;
    /** 连接侦听器 */
    private SocketAcceptor acceptor;
    /** 处理器 */
    private ExecutorService excutors;

    @Resource
    private IoHandlerAdapter MessageHandler;

    /**
     * 开始启动控制中心服务等
     */
    @PostConstruct
    public void register2Global() {
	try {
	    ControlCenter.start();
	    ControlCenter.registerMessageServer(this);
	} catch (Exception e) {
	    throw new RuntimeException("ControlCenter start failed...", e);
	}
    }

    /**
     * 开始启动服务
     */
    public void start() {
	int ioThreads = Configuration.getInt("server.service.corepoolsize", 10);
	int port = Configuration.getInt("server.service.port", 10001);
	if (logger.isInfoEnabled()) {
	    logger.info("msg receiver starting server...");
	}
	try {
	    acceptor = new NioSocketAcceptor(ioThreads);
	    excutors = Executors.newCachedThreadPool();
	    acceptor.setReuseAddress(false);
	    acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(new CodecFactory()));
	    acceptor.getFilterChain().addLast("threadPool", new ExecutorFilter(excutors));
	    IoBuffer.setUseDirectBuffer(false);
	    IoBuffer.setAllocator(new SimpleBufferAllocator());
	    acceptor.setHandler(MessageHandler);
	    acceptor.bind(new InetSocketAddress(port));
	    if (logger.isInfoEnabled()) {
		logger.info("The Msg Receiver Listening on port " + port);
	    }
	} catch (IOException e) {
	    throw new RuntimeException("foreign server start failed!", e);
	}
	if (logger.isInfoEnabled()) {
	    logger.info("msg receiver finish starting...");
	}
    }

    /**
     * 关闭
     */
    public void shutdown() {
	acceptor.unbind();
	excutors.shutdown();
	try {
	    excutors.awaitTermination(1, TimeUnit.MINUTES);
	} catch (Exception e) {
	    // TODO: handle exception
	}
    }

    /**
     * 接收消息 , 入库， 入队列
     */
    public String receiveMessage(final MqMessage msg) throws Exception {
	ControlCenter.addMessage(msg); // 加入消息队列
	if (ControlCenter.isSaveMsgToDB()) {
	    Runnable run = new Runnable() {
		public void run() {
		    try {
			msgDAO.addMessage(msg);
		    } catch (Exception e) {
			logger.error("", e);
		    }
		}
	    };
	    ControlCenter.schedule(run, 1, TimeUnit.MILLISECONDS);
	}
	return msg.getMsgUID();
    }

    /**
     * 根据topic列表来获得几分钟前的消息
     * 
     * @param minutes
     *            ,几分钟前
     * @param topics
     *            , 消息Topic列表，逗号分隔
     */
    public Iterator<MqMessage> getMessagesBeforeByTopic(int minutes, String topics) {
	if (ControlCenter.isSaveMsgToDB()) {
	    try {
		List<MqMessage> dbReturn = msgDAO.getMessagesBeforeByTopic(minutes, topics);
		if (dbReturn != null) {
		    return dbReturn.iterator();
		}
	    } catch (Exception e) {
		logger.error("", e);
	    }
	}
	return null;
    }

    /**
     * 将json消息对象组装成JO
     */
    public MqMessage packMessage(JSONObject msg) {
	MqMessage message = new MqMessage(); // 为了统计消息产生到发送完成的时间，这一对象提前初始化。
	if (msg == null) {
	    return null;
	}
	try {
	    String msgTopic = (String) msg.get("msgTopic");
	    String msgType = (String) msg.get("msgType");
	    long objectId = Long.valueOf(msg.get("objectId") + "");
	    int userId = Integer.valueOf(msg.get("userId") + "");
	    String memo = (String) msg.get("memo");
	    String ip = msg.get("ip") + "";
	    String msgUID = (String) msg.get("msgUID");
	    if (StringUtils.isEmpty(msgTopic) || StringUtils.isEmpty(msgType)) {
		return null;
	    }
	    message.setMsgTopic(msgTopic);// 组装成消息对象
	    message.setMsgType(msgType);
	    message.setObjectId(objectId);
	    message.setUserId(userId);
	    message.setMemo(memo);
	    message.setAddTime(new Date());
	    if (ip != null) {
		try {
		    message.setIp(Integer.valueOf(ip));
		} catch (Exception e) {
		}
	    }
	    if (StringUtils.isNotEmpty(msgUID)) {
		message.setMsgUID(msgUID);
	    } else {
		String uuid = UUID.randomUUID().toString().replaceAll("-", "");
		message.setMsgUID(uuid);
	    }
	    return message;
	} catch (Exception e) {
	    logger.error("error packing for the body content:" + msg, e);
	    return null;
	}
    }

}
