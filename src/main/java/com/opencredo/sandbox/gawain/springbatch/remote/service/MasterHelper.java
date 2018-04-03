package com.opencredo.sandbox.gawain.springbatch.remote.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class MasterHelper implements ApplicationContextAware {
	private static final Logger logger = LoggerFactory.getLogger(MasterHelper.class);
	private ApplicationContext applicationContext;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public void start() {
		MasterService master = applicationContext.getBean(MasterService.class);
		BatchStatus batchStatus = master.getBatchStatus();
		boolean running = true;
		while (running) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException("exception during Thread.sleep()", e);
			}
			// System.out.println("Waiting... Batch Job Status:" +
			// batchContext.getBatchStatus() );
			if (isJobFinished(batchStatus)) {
				logger.info("Job Status:" + batchStatus + ", EXIT");
				running = false;
			}
		}
	}

	public boolean isJobFinished(BatchStatus batchStatus) {
		if (batchStatus == BatchStatus.STARTED || batchStatus == BatchStatus.STARTING
				|| batchStatus == BatchStatus.STOPPING || batchStatus == null) {
			return false;
		} else {
			return true;
		}
	}
}
