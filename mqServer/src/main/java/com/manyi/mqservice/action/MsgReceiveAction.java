package com.manyi.mqservice.action;

import java.io.IOException;
import java.io.PrintWriter;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.manyi.mqservice.model.MqMessage;
import com.manyi.mqservice.service.ControlCenter;
import com.manyi.mqservice.service.MessageServer;
import com.manyi.mqservice.util.RequestUtil;

@Controller
public class MsgReceiveAction {
	private static final Logger logger = Logger.getLogger(MsgReceiveAction.class);
	@Resource
	private MessageServer server;

	@RequestMapping(value = "receiveMsg")
	public void receiveMsg(HttpServletRequest request, HttpServletResponse response) throws Exception {
		writeResponse(response, handle(request, response));
	}

	private String handle(HttpServletRequest request, HttpServletResponse response) {
		String app = request.getParameter("app");
		if (StringUtils.isEmpty(app)) {
			return "error:app is isEmpty";
		}
		// 内容检查
		String jMsg = request.getParameter("msg");
		if (StringUtils.isEmpty(jMsg)) {
			return "error:empty msg body";
		}
		// 日志记录
		if (logger.isInfoEnabled()) {
			logger.info(RequestUtil.getIp(request) + "|" + app + "|" + jMsg);
		}
		JSONObject msg = null;
		try {
			msg = (JSONObject) JSONValue.parse(jMsg);
		} catch (Exception e) {
		}
		if (msg == null) {
			return "error:msg body isn't json";
		}
		// 打包成JO对象
		MqMessage message = server.packMessage(msg);
		if (message == null) {
			return "error:body params miss";
		}
		String errMsg = null;
		String UUID = null;
		try {
			if (ControlCenter.isAcceptable()) {
				UUID = server.receiveMessage(message);
			} else {
				errMsg = "server is shuting down, refuse to receive message";
			}
		} catch (DataAccessException e) {
			logger.error("add message into db error", e);
		} catch (Exception e) {
			logger.error("process msg error:" + message.toString(), e);
			errMsg = e.toString();
		}
		if (errMsg == null) {
			return "succ:" + UUID;
		} else {
			return "error:" + errMsg;
		}
	}

	private void writeResponse(HttpServletResponse resp, String content) {
		PrintWriter writer = null;
		try {
			resp.setHeader("P3P", "CP=CAO PSA OUR");
			resp.setContentType("text/plain");
			writer = resp.getWriter();
			writer.write(content);
		} catch (IOException e) {
			logger.error("writeResponse error:", e);
		} finally {
			IOUtils.closeQuietly(writer);
		}
	}
}