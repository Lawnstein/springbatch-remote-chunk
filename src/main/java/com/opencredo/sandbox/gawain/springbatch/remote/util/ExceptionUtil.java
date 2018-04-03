package com.opencredo.sandbox.gawain.springbatch.remote.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ExceptionUtil {
	public static String getStackTrace(Throwable e) {
		try {
			ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
			e.printStackTrace(new java.io.PrintWriter(buf, true));
			String stack = buf.toString();
			buf.close();
			return stack;
		} catch (IOException e1) {
		}
		return null;
	}
}
