package com.manyi.mqservice.service;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.springframework.stereotype.Repository;

/**
 * 事件处理器
 */
@Repository
class MessageHandler extends IoHandlerAdapter {

    public MessageHandler() {
    }

    public void sessionOpened(IoSession session) throws Exception {
	ProxyInvoker.proxyMsgHandle("sessionOpened", new Object[] { session }, new Class[] { IoSession.class });
    }

    public void sessionClosed(IoSession session) throws Exception {
	ProxyInvoker.proxyMsgHandle("sessionClosed", new Object[] { session }, new Class[] { IoSession.class });
    }

    public void sessionIdle(IoSession session, IdleStatus status) throws Exception {
	ProxyInvoker.proxyMsgHandle("sessionIdle", new Object[] { session, status }, new Class[] { IoSession.class,
		IdleStatus.class });
    }

    public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
	ProxyInvoker.proxyMsgHandle("exceptionCaught", new Object[] { session, cause }, new Class[] { IoSession.class,
		Throwable.class });
    }

    public void messageReceived(IoSession session, Object msgData) throws Exception {
	ProxyInvoker.proxyMsgHandle("messageReceived", new Object[] { session, msgData }, new Class[] {
		IoSession.class, Object.class });
    }

    /**
     * 消息下发完成后被调用的方法
     */
    public void messageSent(IoSession session, Object message) throws Exception {
	ProxyInvoker.proxyMsgHandle("messageSent", new Object[] { session, message }, new Class[] { IoSession.class,
		Object.class });
    }
}