package com.manyi.mqservice.client;

import java.nio.charset.Charset;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.textline.LineDelimiter;
import org.apache.mina.filter.codec.textline.TextLineDecoder;
import org.apache.mina.filter.codec.textline.TextLineEncoder;

public class CodecFactory implements ProtocolCodecFactory {

	@Override
	public ProtocolDecoder getDecoder(IoSession iosession) throws Exception {
		TextLineDecoder coder = new TextLineDecoder(Charset.forName("utf-8"), new LineDelimiter("\n"));
		coder.setMaxLineLength(Integer.MAX_VALUE);
		return coder;
	}

	@Override
	public ProtocolEncoder getEncoder(IoSession iosession) throws Exception {
		TextLineEncoder coder = new TextLineEncoder(Charset.forName("utf-8"), new LineDelimiter("\n"));
		coder.setMaxLineLength(Integer.MAX_VALUE);
		return coder;
	}
}
