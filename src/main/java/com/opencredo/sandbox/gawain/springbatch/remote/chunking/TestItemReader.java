package com.opencredo.sandbox.gawain.springbatch.remote.chunking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.integration.chunk.ChunkParameterListener;
import org.springframework.batch.integration.chunk.ChunkProcessBean;
import org.springframework.batch.integration.chunk.ChunkWriteBean;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import com.opencredo.sandbox.gawain.springbatch.remote.util.ExceptionUtil;

public class TestItemReader implements ItemReader<Map>, ChunkProcessBean, ChunkWriteBean {

	private static final Log logger = LogFactory.getLog(TestItemReader.class);
	private final List<String> items;
	private final List<Map> source = new ArrayList<Map>();
	public volatile int count = 0;
	private String processBeanId;
	private String writeBeanId;
	
	TestItemReader() {
		items = new ArrayList<String>();
	}

	public void setProcessBeanId(String processBeanId) {
		this.processBeanId = processBeanId;
	}

	@Override
	public String getProcessBeanId() {
		return processBeanId;
	}
	
	@Override
	public Map read() throws Exception, UnexpectedInputException, ParseException,
			NonTransientResourceException {
		if (source.size() == 0) {
			for (String s : items) {
				Map<String, Object> m = new HashMap<String, Object>();
				m.put("actno", s);
				m.putAll(ChunkParameterListener.getAllPropertiesValue(this));
				m.putAll(ChunkParameterListener.getJobParameters());
				source.add(m);
			}
		}
		
		if (count < 2) {
			logger.info("Reader调用堆栈：" + ExceptionUtil.getStackTrace(new Exception("call stack.")));
		}
		
		if (source.size() > count) {
			final Map item = source.get(count++);
			logger.info("********** for count " + count + ", reading item: " + item);
			return item;
		}else {
			logger.info("********** returning null for count " + count);
			return null;
		}
		
	}

	public List<String> getItems() {
		return items;
	}

	public void setItems(List<String> items) {
		this.items.clear();
		this.items.addAll(items);
	}

	public String getWriteBeanId() {
		return writeBeanId;
	}

	public void setWriteBeanId(String writeBeanId) {
		this.writeBeanId = writeBeanId;
	}
	
	
	

}
