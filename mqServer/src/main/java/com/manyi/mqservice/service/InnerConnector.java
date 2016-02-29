package com.manyi.mqservice.service;

import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.manyi.mqservice.client.AbstractHandler;
import com.manyi.mqservice.client.MsgQueueSuberClient;
import com.manyi.mqservice.model.MqMessage;
import com.manyi.mqservice.util.Configuration;
import com.manyi.utils.WebTool;

/**
 * 这个类用于服务器集群间的内部连接管理,处理集群间的消息同步,定于集群内所有服务器的消息
 */
public class InnerConnector {

    private static final Logger logger = Logger.getLogger(InnerConnector.class);
    /** 根据ip和port作为key,存放内部链接 */
    private static ConcurrentHashMap<String, MsgQueueSuberClient> subClients = new ConcurrentHashMap<String, MsgQueueSuberClient>();

    /**
     * 根据配置文件，计算当前服务器连接到集群的哪几台服务器
     */
    public void start() {
	String servers = Configuration.getString("server.group.list");
	if (StringUtils.isEmpty(servers)) {
	    return;
	}
	String[] serverIpList = servers.trim().split(",");
	String hostname = WebTool.getHostName();
	// int currentPosition = Configuration.getInt("server.group.current",
	// 0);
	int timeout = Configuration.getInt("server.group.timeout", 200);

	for (int i = 0; i < serverIpList.length; i++) {
	    String hostAddr = serverIpList[i];
	    int pos = hostAddr.indexOf(":");
	    String ip = hostAddr.substring(0, pos).trim();
	    int port = Integer.valueOf(hostAddr.substring(pos + 1).trim());
	    logger.info("hostname=" + hostname + ",ip=" + ip);
	    if (ip.equals(hostname)) { // 该服务器是当前服务器
		logger.info("contine;hostname=" + hostname);
		continue;
	    }
	    connect(ip, port, timeout);
	}
    }

    /**
     * 创建一个连接
     */
    public void connect(String ip, int port, int timeout) {
	if (logger.isInfoEnabled()) {
	    logger.info("start to connect to inner server:" + ip + ":" + port);
	}
	MsgQueueSuberClient suber = new MsgQueueSuberClient();
	suber.setApp(ControlCenter.MQ_APP_TAG);
	suber.setServAddr(ip + ":" + port);
	suber.setTimeout(timeout);
	suber.setSubTopics(ControlCenter.TOPIC_ALL);
	suber.setMinutesBefore(0);
	suber.setHandler(new InnerHandler());
	subClients.put(ip + ":" + port, suber);
    }

    /**
     * 断开连接
     */
    public boolean disConnect(String ip, int port) {
	String key = ip + ":" + port;
	MsgQueueSuberClient client = subClients.get(key);
	if (client == null) {
	    return false;
	}
	client.shutdown();
	subClients.remove(key);
	return true;
    }

    public void shutdown() {
	Iterator<String> it = subClients.keySet().iterator();
	while (it.hasNext()) {
	    String srvKey = it.next();
	    MsgQueueSuberClient client = subClients.get(srvKey);
	    if (client != null) {
		client.shutdown();
	    }
	}
	subClients.clear();
    }

    public boolean isConnected(String ip, int port) {
	String key = ip + ":" + port;
	MsgQueueSuberClient client = subClients.get(key);
	if (client == null) {
	    return false;
	}
	return client.isActive();
    }

    public String getLastConnectedTime(String ip, int port) {
	String key = ip + ":" + port;
	MsgQueueSuberClient client = subClients.get(key);
	if (client == null) {
	    return "CLIENT IS NULL";
	}
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	return sdf.format(new Date(client.getLastLoginTime()));
    }

    class InnerHandler extends AbstractHandler {
	@Override
	public void processMessage(MqMessage message, SocketAddress host) {
	    message.setFromMQ(true); // 将消息设置为来自MQ内部服务器
	    // if (logger.isInfoEnabled()) {
	    // logger.info("received inner data:" + message.toString() +
	    // " from " + host.toString());
	    // }
	    ControlCenter.addMessage(message);
	}
    }
}
