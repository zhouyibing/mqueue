package com.manyi.mqservice;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.manyi.mqservice.service.ControlCenter;

public class ShutDownListener implements ServletContextListener {

	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		ControlCenter.shutdown();
		System.out.println("mqservice shut down!");
	}

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		// do nothing
	}
}