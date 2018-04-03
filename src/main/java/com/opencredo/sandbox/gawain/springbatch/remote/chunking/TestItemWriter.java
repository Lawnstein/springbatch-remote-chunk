package com.opencredo.sandbox.gawain.springbatch.remote.chunking;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemWriter;

import com.opencredo.sandbox.gawain.springbatch.remote.util.ExceptionUtil;

public class TestItemWriter implements ItemWriter<Map> {

	private static final Log logger = LogFactory.getLog(TestItemWriter.class);
	public volatile static int count = 0;
	private int chunkCount = 0;
	private boolean exceptionThrown = false;
	private String exceptionText;
	private String writerName ="unknown-writer";
	
	@Override
	public void write(final List<? extends Map> items) throws Exception {

		if (count < 2) {
			logger.info("Writer调用堆栈：" + ExceptionUtil.getStackTrace(new Exception("call stack.")));
		}
		for (Map item : items) {
			count++;
			logger.info("********** [" + writerName + "] writing item: " + item);
			
		}
		chunkCount++;
	}
	
	public void setExceptionText(final String exceptionText) {
		this.exceptionText = exceptionText;
	}

	public void setWriterName(final String writerName) {
		this.writerName = writerName;
	}

	public int getChunkCount() {
		return chunkCount;
	}

}
