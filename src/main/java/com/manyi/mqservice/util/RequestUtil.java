package com.manyi.mqservice.util;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;

public class RequestUtil {
    private static final Logger logger = Logger.getLogger(RequestUtil.class);

    /**
     * 一个网通用户访问www，真实IP是218.60.0.3. 他的IP相关变量如下： HTTP_CLIENT_IP = 220.181.45.67<br />
     * HTTP_X_CLUSTER_CLIENT_IP = nil<br />
     * HTTP_X_FORWARDED_FOR = 218.60.0.3, 192.168.2.1<br />
     * HTTP_X_FORWARD_FOR = nil<br />
     * REMOTE_ADDR = 10.24.21.249<br />
     * 使用LoadBalance时，获取IP地址
     * 
     * @param request
     * @return
     */

    // public static String getIP(HttpServletRequest req) {
    // String xForwordIp = req.getHeader("x-forwarded-for");
    // if (null != xForwordIp) {
    // xForwordIp = xForwordIp.trim();
    // }
    // String clientIp = req.getHeader("client_ip");
    // if (null != clientIp) {
    // clientIp = clientIp.trim();
    // }
    // String remoteAddr = req.getRemoteAddr();
    // return getIP(xForwordIp, clientIp, remoteAddr);
    // }

    /**
     * 获取请求主机IP地址,如果通过代理进来，则透过防火墙获取真实IP地址;
     * 
     * @param request
     * @return
     * @throws IOException
     */
    public final static String getIp(HttpServletRequest request) {
	// 获取请求主机IP地址,如果通过代理进来，则透过防火墙获取真实IP地址

	String ip = request.getHeader("x-forwarded-for");
	if (logger.isInfoEnabled()) {
	    logger.info("getIpAddress(HttpServletRequest) - X-Forwarded-For - String ip=" + ip);
	}

	if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
	    ip = request.getHeader("Proxy-Client-IP");
	    if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
		ip = request.getHeader("WL-Proxy-Client-IP");
	    }
	    if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
		ip = request.getHeader("HTTP_CLIENT_IP");
	    }
	    if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
		ip = request.getHeader("HTTP_X_FORWARDED_FOR");
	    }
	    if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
		ip = request.getRemoteAddr();
		if (logger.isInfoEnabled()) {
		    logger.info("getIpAddress(HttpServletRequest) - getRemoteAddr - String ip=" + ip);
		}
	    }
	} else if (ip.length() > 15) {
	    String[] ips = ip.split(",");
	    for (int index = 0; index < ips.length; index++) {
		String strIp = (String) ips[index];
		if (!("unknown".equalsIgnoreCase(strIp))) {
		    ip = strIp;
		    break;
		}
	    }
	}
	return ip;
    }
}