/*
 * Copyright 2005-2017 Client Service International, Inc. All rights reserved.
 * CSII PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * project: SpringBatchIntegrationExample
 * create: 2017年12月26日 下午1:29:24
 * vc: $Id: $
 */
package com.opencredo.sandbox.gawain.springbatch.remote.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO 请填写注释.
 * 
 * @author Lawnstein.Chan
 * @version $Revision:$
 */
public class SlaveService {
	private static final Logger logger = LoggerFactory.getLogger(SlaveService.class);

	public void start() throws Exception {
		Thread.sleep(10 * 60 * 1000);

	}
}
