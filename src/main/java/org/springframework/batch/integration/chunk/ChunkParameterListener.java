package org.springframework.batch.integration.chunk;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.factory.InitializingBean;

public class ChunkParameterListener implements ChunkListener, InitializingBean {
	private static final Logger logger = LoggerFactory.getLogger(ChunkParameterListener.class);

	private static final String DEFUALT_KEY_JOB_PARAMS = "DEFUALT_KEY_JOB_PARAMS";
	private static final ThreadLocal<Map> _JOB_PARAMS = new ThreadLocal<Map>();

	private String transactionIdNames = "trcode,TRCODE";
	private String[] transIds = null;

	public static final String PROCESSBEANID = "processBeanId";
	public static final String WRITEBEANID = "writeBeanId";

	public static Map getJobParameters() {
		return _JOB_PARAMS.get();
	}

	public String getTransactionIdNames() {
		return transactionIdNames;
	}

	public void setTransactionIdNames(String transactionIdNames) {
		this.transactionIdNames = transactionIdNames;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (transactionIdNames == null || transactionIdNames.length() == 0) {
			transIds = new String[0];
		} else {
			transIds = transactionIdNames.split(",;");
			for (int i = 0; i < transIds.length; i++) {
				if (transIds[i] == null)
					continue;
				transIds[i] = transIds[i].trim();
			}
		}
	}

	public static Map<String, Object> getAllPropertiesValue(Object instance) {
		LinkedHashMap<String, Object> list = new LinkedHashMap<String, Object>();
		Class clazz = instance.getClass();
		while (clazz != null) {
			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {
				int mo = field.getModifiers();
				if (Modifier.isFinal(mo))
					continue;
				if (Modifier.isStatic(mo))
					continue;

				field.setAccessible(true);

				Object value = null;
				try {
					value = field.get(instance);
				} catch (IllegalArgumentException e) {
					throw new FatalBeanException("parse bean failed.", e);
				} catch (IllegalAccessException e) {
					throw new FatalBeanException("parse bean failed.", e);
				}
				list.put(field.getName(), value);
			}
			clazz = clazz.getSuperclass();
		}
		if (instance instanceof ChunkProcessBean) {
			list.put(PROCESSBEANID, ((ChunkProcessBean) instance).getProcessBeanId());
		}
		if (instance instanceof ChunkWriteBean) {
			list.put(WRITEBEANID, ((ChunkWriteBean) instance).getWriteBeanId());
		}

		return list;
	}

	@Override
	public void afterChunk(ChunkContext chunkContext) {
		_JOB_PARAMS.set(null);
	}

	@Override
	public void afterChunkError(ChunkContext chunkContext) {
		_JOB_PARAMS.set(null);
	}

	@Override
	public void beforeChunk(ChunkContext chunkContext) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.putAll(chunkContext.getStepContext().getJobParameters());
		
		m.put("SPRINGBATCH_JOB_NAME", chunkContext.getStepContext().getJobName());
		m.put("SPRINGBATCH_JOB_ID", chunkContext.getStepContext().getStepExecution().getJobExecution().getJobId());
		m.put("SPRINGBATCH_STEP_NAME", chunkContext.getStepContext().getStepName());
		m.put("SPRINGBATCH_STEP_ID", chunkContext.getStepContext().getStepExecution().getId());
		m.put("SPRINGBATCH_STEP_EXECUTION_ID", chunkContext.getStepContext().getStepExecution().getJobExecutionId());
		
		for (String trid : transIds) {
			if (trid == null || trid.length() == 0) continue;
			m.remove(trid);
		}

		// TranContext.putUsrData(DEFUALT_KEY_JOB_PARAMS, m);
		_JOB_PARAMS.set(m);
	}

}
