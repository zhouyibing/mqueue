package com.manyi.mqservice.util;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class Configuration {

    private static Properties props;

    static {
	load();
    }

    public Configuration() {
	this("config.properties");
    }

    public Configuration(String fileName) {
	load(fileName);
    }

    private static void load(String fileName) {
	try {
	    props = new Properties();
	    File f = new File(Configuration.class.getResource("/").getPath() + fileName);
	    System.out.println("conf path:" + f.getAbsolutePath());
	    FileInputStream fis = new FileInputStream(f);
	    props.load(fis);
	    fis.close();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    private static void load() {
	load("config.properties");
    }

    public static String getString(String name) {
	if (props == null) {
	    load();
	}
	return props.getProperty(name);
    }

    public static String getString(String name, String defVal) {
	String val = getString(name);
	if (val == null) {
	    return defVal;
	}
	return val;
    }

    public static int getInt(String name, int defVal) {
	String val = getString(name);
	try {
	    return Integer.parseInt(val);
	} catch (Exception e) {
	    return defVal;
	}
    }

    public static long getLong(String name, long defVal) {
	String val = getString(name);
	try {
	    return Long.parseLong(val);
	} catch (Exception e) {
	    return defVal;
	}
    }
}
