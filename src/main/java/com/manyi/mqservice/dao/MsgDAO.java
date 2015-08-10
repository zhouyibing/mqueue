package com.manyi.mqservice.dao;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.log4j.Logger;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.stereotype.Repository;

import com.manyi.mqservice.model.MqMessage;
import com.manyi.mqservice.service.ControlCenter;

@Repository
public class MsgDAO {
	@Resource
	private SqlSessionTemplate mqWriteSqlMap;
	@Resource
	private SqlSessionTemplate mqReadSqlMap;

	private static final Logger logger = Logger.getLogger(MsgDAO.class);

	/**
	 * 创建一个线程，每小时尝试创建新表
	 */
	@PostConstruct
	public void initTable() {
		Thread th = new Thread() {
			public void run() {
				while (true) {
					try {
						logger.info("start to create table");
						createTable(new Date());
						createTable(new Date(System.currentTimeMillis() + 3600 * 1000 * 24));
						createTable(new Date(System.currentTimeMillis() + 3600 * 1000 * 48));
						Thread.sleep(3600 * 1000 * 24);
					} catch (Exception e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
		};
		th.setDaemon(true);
		th.start();
		try {
			createTable(new Date());
		} catch (Exception e) {
		}
	}

	/**
	 * 根据日期创建一个日表
	 */
	public void createTable(Date day) {
		try {
			String tableName = getTableName(day);
			Map<String, String> params = new HashMap<String, String>();
			params.put("tableName", tableName);
			mqWriteSqlMap.update("MQDAO.createTable", params);
			logger.info("create table " + tableName + " success!");
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * 增加一条消息到数据库
	 */
	public void addMessage(MqMessage msg) {
		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("msgUID", msg.getMsgUID());
		params.put("msgTopic", msg.getMsgTopic());
		params.put("msgType", msg.getMsgType());
		params.put("objectId", msg.getObjectId());
		params.put("userId", msg.getUserId());
		params.put("memo", msg.getMemo());
		params.put("tableName", getTableName(new Date()));
		params.put("ip", msg.getIp());
		mqWriteSqlMap.insert("MQDAO.addMessage", params);
	}

	/**
	 * 获取一条消息
	 */
	public MqMessage getMessageById(int msgId) {
		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("msgId", msgId);
		params.put("tableName", getTableName(new Date()));
		return (MqMessage) mqReadSqlMap.selectOne("MQDAO.getMessageById", params);
	}

	/**
	 * 获取一条消息
	 */
	public int getMessageByUID(String msgId) {
		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("uuid", msgId);
		params.put("tableName", getTableName(new Date()));
		return (Integer) mqReadSqlMap.selectOne("MQDAO.getMessageByUID", params);
	}

	/**
	 * 根据topics获取几分钟之前的数据库内的数据
	 */
	public List<MqMessage> getMessagesBeforeByTopic(int minutes, String topics) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date datePoint = new Date(System.currentTimeMillis() - minutes * 60000);
		String dateStr = sdf.format(datePoint);
		String tableName1 = getTableName(datePoint);
		String tableName2 = getTableName(new Date());
		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("dateStr", dateStr);
		params.put("tableName", tableName1);
		if (topics != null && !ControlCenter.TOPIC_ALL.equals(topics)) {
			StringBuilder sb = new StringBuilder();
			String[] tps = topics.split(",");
			for (String string : tps) {
				sb.append("'");
				sb.append(string);
				sb.append("'");
				sb.append(",");
			}
			sb.deleteCharAt(sb.length() - 1);
			params.put("topics", sb.toString());
		}
		List<MqMessage> ret = mqReadSqlMap.selectList("MQDAO.getMessageBefore", params);
		if (!tableName1.equals(tableName2)) {
			params.put("tableName", tableName2);
			List<MqMessage> tmp = mqReadSqlMap.selectList("MQDAO.getMessageBefore", params);
			ret.addAll(tmp);
		}
		return ret;
	}

	/**
	 * 
	 * 根据日期获取表名
	 */
	public String getTableName(Date date) {
		if (date == null) {
			date = new Date();
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		return "iwjw_message_" + sdf.format(date);
	}
}
