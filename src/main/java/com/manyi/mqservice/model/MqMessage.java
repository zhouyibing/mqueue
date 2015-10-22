package com.manyi.mqservice.model;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

import org.json.simple.JSONObject;

public class MqMessage extends BaseModel implements Serializable {

	private static final long serialVersionUID = 1L;
	/** 消息创建的Key */
	public static final String TIMESTAMP_KEY = "MSG-Created-Time";
	/** 消息ID,由数据库赋予值 */
	private int id;
	/** 消息msgUID，自动赋予 */
	private String msgUID;
	/** 消息的Topic，用于消费和订阅的消息种类区分 */
	private String msgTopic;
	/** 消息的处理类型，无限制，由消息生产者定义 */
	private String msgType;
	/** 消息对象所针对的对象ID,和具体业务有关，具体含义由消息生产者定义 */
	private long objectId;
	/** 消息对象所针对的用户ID,和具体业务有关，具体含义由消息生产者定义 */
	private int userId;
	/** 消息对象扩展信息，具体含义由消息生产者定义 */
	private String memo;
	/** 消息对象的创建时间, 由消息中心赋予 */
	private Date addTime;
	/** 用户ip的字符串to int */
	private int ip;
	/** 消息是否来自MQ内部服务器，用于集群间消息的定义 */
	private boolean isFromMQ;
	/** 消息被处理次数，客户端辨识用，超过5次后丢弃 */
	private transient int processTimes;
	/** 消息对象创建时间 */
	private long initTime = System.currentTimeMillis();

	public boolean isFromMQ() {
		return isFromMQ;
	}

	public void setFromMQ(boolean isFromMQ) {
		this.isFromMQ = isFromMQ;
	}

	public int getIp() {
		return ip;
	}

	public void setIp(int ip) {
		this.ip = ip;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getMsgUID() {
		return msgUID;
	}

	public void setMsgUID(String msgUID) {
		this.msgUID = msgUID;
	}

	public String getMsgTopic() {
		return msgTopic;
	}

	public void setMsgTopic(String msgTopic) {
		this.msgTopic = msgTopic;
	}

	public String getMsgType() {
		return msgType;
	}

	public void setMsgType(String msgType) {
		this.msgType = msgType;
	}

	public long getObjectId() {
		return objectId;
	}

	public void setObjectId(long objectId) {
		this.objectId = objectId;
	}

	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public String getMemo() {
		return memo;
	}

	public void setMemo(String memo) {
		this.memo = memo;
	}

	public Date getAddTime() {
		if (addTime == null) {
			addTime = new Date();
		}
		return addTime;
	}

	public void setAddTime(Date addTime) {
		this.addTime = addTime;
	}

	@SuppressWarnings("unchecked")
	public JSONObject toJSON() {
		JSONObject jo = new JSONObject();
		jo.put("msgUID", msgUID);
		jo.put("msgTopic", msgTopic);
		jo.put("msgType", msgType);
		jo.put("objectId", objectId);
		jo.put("userId", userId);
		jo.put("isFromMQ", isFromMQ);
		if (getAddTime() != null) {
			jo.put("addTime", addTime.getTime());
		}
		jo.put("memo", memo);
		jo.put("ip", ip);
		return jo;
	}

	/**
	 * 根据json对象获取MqMessage业务对象
	 * */
	public static MqMessage parseJSON(JSONObject jo) throws Throwable {
		MqMessage ret = new MqMessage();
		ret.setMsgUID((String) jo.get("msgUID"));
		ret.setMsgTopic((String) jo.get("msgTopic"));
		ret.setMsgType((String) jo.get("msgType"));
		ret.setObjectId(((Long) jo.get("objectId")).intValue());
		ret.setUserId(((Long) jo.get("userId")).intValue());
		ret.setFromMQ(Boolean.valueOf(jo.get("isFromMQ") + ""));
		Long time = (Long) jo.get("addTime");
		if (time != null) {
			ret.setAddTime(new Date(time));
		}
		ret.setMemo((String) jo.get("memo"));
		ret.setIp(((Long) jo.get("ip")).intValue());
		return ret;

	}

	public long getInitTime() {
		return initTime;
	}

	public int getProcessTimes() {
		return processTimes;
	}

	public void setProcessTimes(int processTimes) {
		this.processTimes = processTimes;
	}

	public void renewUUID() {
		this.msgUID = UUID.randomUUID().toString().replaceAll("-", "");
	}
}
