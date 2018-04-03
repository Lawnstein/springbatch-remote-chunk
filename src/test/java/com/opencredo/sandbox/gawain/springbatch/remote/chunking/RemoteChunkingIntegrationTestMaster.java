package com.opencredo.sandbox.gawain.springbatch.remote.chunking;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.test.annotation.DirtiesContext;

import com.opencredo.sandbox.gawain.springbatch.remote.manage.BatchJobTestHelper;
import com.opencredo.sandbox.gawain.springbatch.remote.manage.MasterBatchContext;

@DirtiesContext
public class RemoteChunkingIntegrationTestMaster {

	private static final Log logger = LogFactory.getLog(RemoteChunkingIntegrationTestMaster.class);

	@Test
	public void testRemoteChunkingOnMultipleSlavesShouldLoadBalanceAndComplete() throws Exception {

		final MasterBatchContext masterBatchContext = new MasterBatchContext("testjob",
				"classpath:/master/master-batch-context.xml");
		masterBatchContext.start();

		BatchJobTestHelper.waitForJobTopComplete(masterBatchContext);

		final BatchStatus batchStatus = masterBatchContext.getBatchStatus();
		logger.info("job finished with status: " + batchStatus);
		Assert.assertEquals("Batch Job Status", BatchStatus.COMPLETED, batchStatus);

	}

}
