package com.manyi.mqservice.action;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.manyi.mqservice.service.ProxyInvoker;

/**
 * 本类用于输出统计数据
 */
@Controller
public class DataAction {
	@RequestMapping(value = "statInfo")
	public String getStatInfo(HttpServletRequest request, HttpServletResponse response) throws Exception {
		String outData = ProxyInvoker.getCaculateData();
		if (outData == null) {
			outData = "";
		}
		response.getWriter().write(outData);
		return null;
	}
}