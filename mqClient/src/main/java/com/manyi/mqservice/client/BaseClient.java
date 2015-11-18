package com.manyi.mqservice.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;

/**
 * @author chentong
 * @date 2014-9-15
 */
public class BaseClient
{

	/** 集群服务器随机列表 */
	private List<String> servers;
	// /** 字符串格式的地址 */
	// private String servAddr;
	/** 应用名称 */
	private String app;
	/** 超时时间(ms) */
	private long timeout;

	public String getApp()
	{
		return app;
	}

	public void setApp(String app)
	{
		this.app = app;
	}

	public long getTimeout()
	{
		return timeout;
	}

	public void setTimeout(long timeout)
	{
		this.timeout = timeout;
	}

	/**
	 * 接收集群地址配置，解析并校验
	 */
	public void setServAddr(String servAddr)
	{
		if (StringUtils.isEmpty(servAddr))
		{
			throw new NullPointerException("servAddr can't be null");
		}
		String[] arr = servAddr.trim().split(",");
		List<String> list = new ArrayList<String>();
		for (String string : arr)
		{
			if (!validAddrFormat(string))
			{
				throw new RuntimeException("servAddr format is error:" + string);
			}
			list.add(string);
		}
		servers = getRandomList(list);
	}

	/**
	 * 返回集群服务器总数
	 */
	public int getServersCount()
	{
		return servers.size();
	}

	/**
	 * 获得远程服务器地址
	 */
	public ServerInfo getServerInfo(int pos)
	{
		if (pos > servers.size() - 1)
		{
			return null;
		}
		if (pos < 0)
		{
			pos = 0;
		}
		String addr = servers.get(pos);
		int p = addr.indexOf(":");
		ServerInfo info = new ServerInfo();
		info.host = addr.substring(0, p);
		info.port = Integer.valueOf(addr.substring(p + 1));
		return info;
	}

	/**
	 * 服务器地址对象
	 */
	public static class ServerInfo
	{
		public String host;
		public int port;
	}

	/**
	 * 随机打乱list中的元素
	 */
	protected List<String> getRandomList(List<String> src)
	{
		if (src.size() == 1)
		{
			return src;
		}
		for (int i = 0; i < src.size(); i++)
		{
			int random = new Random().nextInt(src.size());
			String tmp = src.get(i);
			String dest = src.get(random);
			src.set(i, dest);
			src.set(random, tmp);
		}
		return src;
	}

	/**
	 * 校验服务器地址格式是否正确
	 */
	protected boolean validAddrFormat(String addr)
	{
		String tmp = addr.trim();
		int pos = tmp.indexOf(":");
		if (pos == -1)
		{
			return false;
		}
		String port = tmp.substring(pos + 1);
		if (!StringUtils.isNumeric(port))
		{
			return false;
		}
		return true;
	}
}
