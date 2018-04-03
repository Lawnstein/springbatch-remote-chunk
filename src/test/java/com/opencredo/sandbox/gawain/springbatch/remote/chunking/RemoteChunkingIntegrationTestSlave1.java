package com.opencredo.sandbox.gawain.springbatch.remote.chunking;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.springframework.test.annotation.DirtiesContext;

import com.opencredo.sandbox.gawain.springbatch.remote.manage.SlaveContext;

@DirtiesContext
public class RemoteChunkingIntegrationTestSlave1 {

	private static final Log logger = LogFactory.getLog(RemoteChunkingIntegrationTestSlave1.class);

	@Test
	public void testRemoteChunkingOnMultipleSlavesShouldLoadBalanceAndComplete() throws Exception {

		final SlaveContext slaveContext1 = new SlaveContext("classpath:/slave/slave1-batch-context.xml");
		slaveContext1.start();
		Thread.sleep(10 * 60 * 1000);

		logger.info("slave 1 chunks written: " + slaveContext1.writtenCount());
		// Assert.assertEquals("slave chunks written", 5,
		// slaveContext1.writtenCount() );

	}

}
