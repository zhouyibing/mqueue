package com.manyi.mqservice.model;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONObject;

@SuppressWarnings("unchecked")
public class RequestMessage implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1584569397331664762L;
	/** 倒数器，用于检查数据是否到位 */
	private transient CountDownLatch respReady = new CountDownLatch(1);
	/** 指令 */
	private String cmd;
	/** 内容 */
	private JSONObject body = new JSONObject();
	/** 数据 */
	private String response;

	public void setBody(JSONObject body) {
		this.body = body;
	}

	public String getCmd() {
		return cmd;
	}

	public void setCmd(String cmd) {
		this.cmd = cmd;
	}

	public JSONObject getBody() {
		return body;
	}

	public void setBodyField(String key, Object val) {
		body.put(key, val);
	}

	public Object getBodyField(String key) {
		return body.get(key);
	}

	public String toCmd() {
		return cmd + "#" + body.toString();
	}

	public String getResponse(long timeout, TimeUnit unit) throws InterruptedException {
		try {
			respReady.await(timeout, unit);
		} catch (InterruptedException ie) {
			respReady.countDown(); // non-interrupted thread
			throw ie;
		}
		return response;
	}

	public void anounceResp(String response) {
		this.response = response;
		this.respReady.countDown();
	}
}
