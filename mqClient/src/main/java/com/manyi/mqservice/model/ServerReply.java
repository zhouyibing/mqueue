package com.manyi.mqservice.model;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class ServerReply {
	/** 指令 */
	private String cmd;
	/** 状态 */
	private int status;
	/** 数据 */
	private String message;
	/** 回复开始时间 */
	private long procStartTime = System.currentTimeMillis();

	public ServerReply() {
	}

	public ServerReply(String resp) {
		int pos = resp.indexOf("#");
		if (pos == -1) {
			cmd = resp;
		} else {
			cmd = resp.substring(0, pos);
			String jbody = resp.substring(pos + 1);
			JSONObject jo = (JSONObject) JSONValue.parse(jbody);
			status = ((Long) jo.get("status")).intValue();
			message = (String) jo.get("message");
		}
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@SuppressWarnings("unchecked")
	public String toString() {
		JSONObject ret = new JSONObject();
		ret.put("status", status);
		ret.put("message", message);
		return cmd + "#" + ret.toString();
	}

	public String getCmd() {
		return cmd;
	}

	public void setCmd(String cmd) {
		this.cmd = cmd;
	}

	public long getProcStartTime() {
		return procStartTime;
	}

	public void setProcStartTime(long procStartTime) {
		this.procStartTime = procStartTime;
	}
}
