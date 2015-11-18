package com.manyi.mqservice.model;

public class BaseModel {
	// 输出类的详细信息
	public String toString() {
		return new ObjectAnalyzer().toString(this);
	}
}
