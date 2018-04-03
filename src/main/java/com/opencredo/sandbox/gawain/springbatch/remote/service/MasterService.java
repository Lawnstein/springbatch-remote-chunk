/*
 * Copyright 2005-2017 Client Service International, Inc. All rights reserved.
 * CSII PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * project: SpringBatchIntegrationExample
 * create: 2017年12月26日 下午1:29:24
 * vc: $Id: $
 */
package com.opencredo.sandbox.gawain.springbatch.remote.service;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.opencredo.sandbox.gawain.springbatch.remote.manage.AbstractStartable;

/**
 * TODO 请填写注释.
 * 
 * @author Lawnstein.Chan
 * @version $Revision:$
 */
public class MasterService extends AbstractStartable implements ApplicationContextAware {
	private static final Logger logger = LoggerFactory.getLogger(MasterService.class);
	private ApplicationContext applicationContext;
	private String jobName = "testjob";
	private BatchStatus batchStatus;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public BatchStatus getBatchStatus() {
		return batchStatus;
	}

	public void setBatchStatus(BatchStatus batchStatus) {
		this.batchStatus = batchStatus;
	}

	@Override
	public Object call() throws Exception {
		Thread.sleep(3000);
		logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		long s = 0;
		try {
			final JobLauncher jobLauncher = (JobLauncher) applicationContext.getBean("jobLauncher");
			final JobParameter jobParam = new JobParameter(UUID.randomUUID().toString());
			final Map params = Collections.singletonMap("param1", jobParam);
			final Job job = (Job) applicationContext.getBean(jobName);
			logger.debug("batch context running job " + jobName + " : " + job);
			s = System.currentTimeMillis();
			final JobExecution jobExecution = jobLauncher.run(job, new JobParameters(params));
			batchStatus = jobExecution.getStatus();
		} catch (Exception e) {
			logger.error("Exception thrown in batch context", e);
			throw new RuntimeException(e);
		}

		logger.debug("batch context finished running job {}, elapsed {}", batchStatus, System.currentTimeMillis() - s);
		return batchStatus;
	}
}
