package org.springframework.batch.integration.chunk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobInterruptedException;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.step.item.Chunk;
import org.springframework.batch.core.step.item.ChunkProcessor;
import org.springframework.batch.core.step.item.FaultTolerantChunkProcessor;
import org.springframework.batch.core.step.item.SimpleChunkProcessor;
import org.springframework.batch.core.step.skip.NonSkippableReadException;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipListenerFailedException;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.message.ChunkMessageGateway;
import org.springframework.batch.message.ChunkMessageHandler;
import org.springframework.batch.message.ChunkMessagePackage;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.task.TaskExecutor;
import org.springframework.retry.RetryException;
import org.springframework.util.Assert;

public class ChunkItemMessageSlaveHandler<S>
		implements ChunkMessageHandler<S>, InitializingBean, ApplicationContextAware {

	protected final static Logger logger = LoggerFactory.getLogger(ChunkItemMessageSlaveHandler.class);

	private ApplicationContext applicationContext;

	private ChunkMessageGateway messagingGateway;

	private TaskExecutor taskExecutor = null;

	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.messagingGateway, "A ChunkMessageMasterGateway must be provided");
	}

	public ChunkMessageGateway getMessagingGateway() {
		return messagingGateway;
	}

	public void setMessagingGateway(ChunkMessageGateway messagingGateway) {
		this.messagingGateway = messagingGateway;
	}

	public TaskExecutor getTaskExecutor() {
		return taskExecutor;
	}

	public void setTaskExecutor(TaskExecutor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	public ChunkResponse doHandle(ChunkProcessor chunkProcessor, ChunkRequest<S> chunkRequest) throws Exception {
		logger.debug("Handling chunk: {}", chunkRequest);

		StepContribution stepContribution = chunkRequest.getStepContribution();

		Throwable failure = doProcess(chunkProcessor, chunkRequest, stepContribution);
		if (failure != null) {
			logger.debug("Failed chunk", failure);
			return new ChunkResponse(false, chunkRequest.getSequence(), Long.valueOf(chunkRequest.getJobId()),
					stepContribution, failure.getClass().getName() + ": " + failure.getMessage());
		}

		logger.debug("Completed chunk handling with {}", stepContribution);
		return new ChunkResponse(true, chunkRequest.getSequence(), Long.valueOf(chunkRequest.getJobId()),
				stepContribution);
	}

	private Throwable doProcess(ChunkProcessor chunkProcessor, ChunkRequest<S> chunkRequest,
			StepContribution stepContribution) throws Exception {
		Chunk chunk = new Chunk(chunkRequest.getItems());
		Throwable failure = null;
		try {
			chunkProcessor.process(stepContribution, chunk);
		} catch (SkipLimitExceededException e) {
			failure = e;
		} catch (NonSkippableReadException e) {
			failure = e;
		} catch (SkipListenerFailedException e) {
			failure = e;
		} catch (RetryException e) {
			failure = e;
		} catch (JobInterruptedException e) {
			failure = e;
		} catch (Exception e) {
			if (chunkProcessor instanceof FaultTolerantChunkProcessor) {
				throw e;
			}

			failure = e;
		}

		return failure;
	}

	public void innerHandle(ChunkMessagePackage reqMessagePackage) {

		ChunkRequest chunkRequest = (ChunkRequest) reqMessagePackage.getPayLoad();
		ChunkResponse chunkResponse = null;

		if (reqMessagePackage.getProcessBeanId() == null || reqMessagePackage.getProcessBeanId().length() == 0
				|| reqMessagePackage.getWriteBeanId() == null || reqMessagePackage.getWriteBeanId().length() == 0) {

			String failMessage = "Slave handle chunk failed. No " + ChunkParameterListener.PROCESSBEANID + " or "
					+ ChunkParameterListener.PROCESSBEANID + " assigned.";
			logger.warn("{} {}", failMessage, reqMessagePackage);

			// public ChunkResponse(boolean status, int sequence, Long jobId,
			// StepContribution stepContribution, String message)
			chunkResponse = new ChunkResponse(false, chunkRequest.getSequence(), Long.valueOf(chunkRequest.getJobId()),
					chunkRequest.getStepContribution(), failMessage);
		} else {

			try {
				ItemProcessor itemProcessor = applicationContext.getBean(reqMessagePackage.getProcessBeanId(),
						ItemProcessor.class);

				ItemWriter itemWriter = applicationContext.getBean(reqMessagePackage.getWriteBeanId(),
						ItemWriter.class);

				SimpleChunkProcessor chunkProcessor = new SimpleChunkProcessor(itemProcessor, itemWriter);

				chunkResponse = doHandle(chunkProcessor, chunkRequest);
			} catch (Throwable e) {
				chunkResponse = new ChunkResponse(false, chunkRequest.getSequence(),
						Long.valueOf(chunkRequest.getJobId()), chunkRequest.getStepContribution(),
						e.getClass().getName() + ": " + e.getMessage());
			}
		}

		ChunkMessagePackage resMessagePackage = reqMessagePackage.createResponse(chunkResponse);
		messagingGateway.send(resMessagePackage);
		return;
	}

	@Override
	public void handle(S request) throws Exception {
		logger.info("Slave recv chunk request : {}", request);
		if (this.taskExecutor == null) {
			innerHandle((ChunkMessagePackage) request);
		} else {
			final ChunkMessagePackage finalRequest = (ChunkMessagePackage) request;
			this.taskExecutor.execute(new Runnable() {

				@Override
				public void run() {
					innerHandle(finalRequest);
				}
			});
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
}
