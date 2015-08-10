package com.manyi.mqservice.action;

import java.net.SocketAddress;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.manyi.mqservice.client.AbstractHandler;
import com.manyi.mqservice.client.MsgQueueSenderClient;
import com.manyi.mqservice.client.MsgQueueSuberClient;
import com.manyi.mqservice.model.MqMessage;

@Controller
public class TestAction {
	private final static Logger logger = Logger.getLogger(TestAction.class);

	@RequestMapping(value = "test")
	public void test(HttpServletRequest request, HttpServletResponse response, Integer count) throws Exception {
		init();
		for (int i = 0; i < count; i++) {
			new SendWorkers("sender_" + i).start();
		}
		response.getWriter().write("ok");
	}

	private MsgQueueSenderClient sender = null;
	private MsgQueueSuberClient subers = null;

	private void init() {
		subers = new MsgQueueSuberClient();
		sender = new MsgQueueSenderClient();

		subers.setApp("beta_test_1");
		subers.setMinutesBefore(1);
		subers.setTimeout(3000);
		subers.setSubTopics("sender_1");
		subers.setServAddr("mqueue_web1:10301");
		subers.setHandler(new MsgHandler());

		sender.setApp("beta_test_1");
		sender.setServAddr("mqueue_web1:10301");
		sender.setTimeout(3000);
		sender.setMaxActive(20);

	}

	class SendWorkers extends Thread {
		private String topic;

		public SendWorkers(String topic) {
			this.topic = topic;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void run() {
			for (int i = 0; i < 100; i++) {
				MqMessage msg = new MqMessage();
				msg.setMsgTopic(topic);
				msg.setMsgType("update");
				msg.setObjectId(100000 + i);
				msg.setUserId(100000 + i);
				msg.setIp(100000 + i);
				JSONObject memo = new JSONObject();
				memo.put("password", topic + "_password");
				memo.put("detail", topic + "_测试_" + i);
				msg.setMemo(memo.toString());
				sender.sendMessage(msg);
			}
			logger.info("sender ok; " + topic);
		}
	}

	class MsgHandler extends AbstractHandler {
		@Override
		public void processMessage(MqMessage mqMessage, SocketAddress socketAddress) {
			System.out.println("houseId=" + mqMessage.toJSON().toJSONString());
		}
	}
}