package com.manyi.mqservice.client;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Utility {
	/**
	 * ip string to int
	 */
	public static int ipToInt(String ip) {
		try {
			InetAddress address = InetAddress.getByName(ip);
			byte[] bytes = address.getAddress();
			int a, b, c, d;
			a = byte2int(bytes[0]);
			b = byte2int(bytes[1]);
			c = byte2int(bytes[2]);
			d = byte2int(bytes[3]);
			int result = (a << 24) | (b << 16) | (c << 8) | d;
			return result;
		} catch (UnknownHostException e) {
			return 0;
		}
	}

	/**
	 * byte to int
	 */
	public static int byte2int(byte b) {
		int l = b & 0x07f;
		if (b < 0) {
			l |= 0x80;
		}
		return l;
	}

	/**
	 * ip to long
	 */
	public static long ip2long(String ip) {
		int ipNum = ipToInt(ip);
		return int2long(ipNum);
	}

	/**
	 * int to long
	 */
	public static long int2long(int i) {
		long l = i & 0x7fffffffL;
		if (i < 0) {
			l |= 0x080000000L;
		}
		return l;
	}

	/**
	 * long to ip string
	 */
	public static String long2ip(long ip) {
		int[] b = new int[4];
		b[0] = (int) ((ip >> 24) & 0xff);
		b[1] = (int) ((ip >> 16) & 0xff);
		b[2] = (int) ((ip >> 8) & 0xff);
		b[3] = (int) (ip & 0xff);
		String x = Integer.toString(b[0]) + "." + Integer.toString(b[1]) + "." + Integer.toString(b[2]) + "."
				+ Integer.toString(b[3]);
		return x;
	}
}
