package com.manyi.mqservice.service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.buffer.SimpleBufferAllocator;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.transport.socket.SocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

import com.manyi.mqservice.client.CodecFactory;
import com.manyi.mqservice.util.Configuration;

public class ConsoleServer {

    private static final Logger logger = Logger.getLogger(ConsoleServer.class);
    /** socket侦听器 */
    private SocketAcceptor acceptor;
    /** IO处理器 */
    private ExecutorService excutors;

    /**
     * 启动侦听控制台端口
     */
    public void start() {
	int ioThreads = 1;
	int port = Configuration.getInt("server.console.port", 10002);
	if (logger.isInfoEnabled()) {
	    logger.info("starting console server...");
	}
	try {
	    acceptor = new NioSocketAcceptor(ioThreads);
	    excutors = Executors.newCachedThreadPool();
	    acceptor.setReuseAddress(true);
	    acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(new CodecFactory()));
	    acceptor.getFilterChain().addLast("threadPool", new ExecutorFilter(excutors));
	    IoBuffer.setUseDirectBuffer(false);
	    IoBuffer.setAllocator(new SimpleBufferAllocator());
	    acceptor.setHandler(new MessageHandler());
	    acceptor.bind(new InetSocketAddress(port));
	    if (logger.isInfoEnabled()) {
		logger.info("The console server Listening on port " + port);
	    }
	} catch (IOException e) {
	    throw new RuntimeException("console server start failed!", e);
	}
	if (logger.isInfoEnabled()) {
	    logger.info("console server finish starting...");
	}
    }

    /**
     * 关闭控制台服务
     */
    public void shutdown() {
	if (acceptor != null)
	    acceptor.unbind();
	if (excutors != null) {
	    excutors.shutdown();
	    try {
		excutors.awaitTermination(1, TimeUnit.MINUTES);
	    } catch (Exception e) {
		// TODO: handle exception
	    }
	}
    }

    class MessageHandler extends IoHandlerAdapter {

	public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
	    if (logger.isDebugEnabled()) {
		logger.debug("session excepiton caught:" + session.getRemoteAddress());
	    }
	    logger.error("", cause);
	}

	public void sessionOpened(IoSession session) throws Exception {
	    session.setAttribute("step", 1); // 设置登录第一步
	    session.write("plz input the password:\r");
	    if (logger.isInfoEnabled()) {
		logger.info(session.getRemoteAddress() + "  console start login");
	    }
	}

	public void messageReceived(IoSession session, Object msgData) throws Exception {
	    // 交给reload包下的程序处理
	    ProxyInvoker.proxyConsoleHandle("messageReceived", new Object[] { session, msgData }, new Class[] {
		    IoSession.class, Object.class });
	}

	public void messageSent(IoSession session, Object message) throws Exception {
	    if (logger.isDebugEnabled()) {
		logger.debug("console result:" + message);
	    }
	}
    }

}
