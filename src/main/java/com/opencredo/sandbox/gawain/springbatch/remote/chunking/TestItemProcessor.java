package com.opencredo.sandbox.gawain.springbatch.remote.chunking;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemProcessor;

import com.opencredo.sandbox.gawain.springbatch.remote.util.ExceptionUtil;

public class TestItemProcessor implements ItemProcessor<Map, Map> {

	private static final Log logger = LogFactory.getLog(TestItemProcessor.class);
	public volatile static int count = 0;
	private int chunkCount = 0;
	private boolean exceptionThrown = false;
	private String exceptionText;
	private String processorName = "unknown-processor";

	/*
	 * @Override public void write(final List<? extends String> items) throws
	 * Exception { for (String item : items) { count++; if (!exceptionThrown &&
	 * exceptionText != null && item.contains(exceptionText)) { exceptionThrown
	 * = true; // throw checked exception so master can deal with it throw new
	 * RuntimeException(
	 * "A contrived exception thrown on the remote writer to demonstrate error handling over message queues for item "
	 * + item); } logger.info("********** [" + writerName + "] writing item: " +
	 * item);
	 * 
	 * } chunkCount++; }
	 */
	public void setExceptionText(final String exceptionText) {
		this.exceptionText = exceptionText;
	}

	public String getProcessorName() {
		return processorName;
	}

	public void setProcessorName(String processorName) {
		this.processorName = processorName;
	}

	public int getChunkCount() {
		return chunkCount;
	}

	@Override
	public Map process(Map item) throws Exception {

		if (count < 2) {
			logger.info("Processor调用堆栈：" + ExceptionUtil.getStackTrace(new Exception("call stack.")));
		}

		logger.info(">>>>>>>>>>>> [" + processorName + "] process item: " + item);
		// Thread.sleep(1200);
		logger.info("<<<<<<<<<<<< [" + processorName + "] process item: " + item);
		count++;
		return item;
	}


}
