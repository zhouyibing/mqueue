package com.manyi.mqservice.model;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;

public class ObjectAnalyzer {
	private ArrayList<Object> visited = new ArrayList<Object>();

	// toString
	public String toString(Object obj) {
		if (obj == null) {
			return "null";
		}
		if (visited.contains(obj)) {
			return "...";
		}
		Class<?> cl = obj.getClass();
		if (cl == String.class) // 如果是字符串，直接输出
			return (String) obj;
		if (cl.isArray()) { // 如果是数组
			String r = cl.getComponentType() + "[]{";
			for (int i = 0; i < Array.getLength(obj); i++) {
				if (i > 0) {
					r += ",";
					Object val = Array.get(obj, i);
					if (cl.getComponentType().isPrimitive()) {
						r += val;
					} else {
						r += toString(val);
					}
				}
			}
			return r;
		}
		String r = cl.getName();
		do {
			Field[] fields = cl.getDeclaredFields();
			if (fields.length > 0) {
				r += "[";
				AccessibleObject.setAccessible(fields, true);
				for (Field field : fields) {
					if (!Modifier.isStatic(field.getModifiers())) { // 非静态变量
						if (!r.endsWith("[")) {
							r += ",";
						}
						r += field.getName() + "=";
						try {
							Class<?> t = field.getType();
							Object val = field.get(obj);
							if (t.isPrimitive()) {
								r += val;
							} else {
								r += toString(val);
							}
						} catch (Exception e) {
							// e.printStackTrace();
						}

					}
				}
				r += "]";
			}
			cl = cl.getSuperclass();
		} while (cl != null);
		return r;
	}

}
