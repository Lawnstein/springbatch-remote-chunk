/*
 * Copyright 2006-2007 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.batch.integration.chunk;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.message.ChunkMessageGateway;
import org.springframework.batch.message.ChunkMessagePackage;
import org.springframework.util.Assert;

public class ChunkItemMessageMasterWriter<T> extends StepExecutionListenerSupport
		implements ItemWriter<T>, ItemStream, StepContributionSource {

	protected final static Logger logger = LoggerFactory.getLogger(ChunkItemMessageMasterWriter.class);

	static final String ACTUAL = ChunkItemMessageMasterWriter.class.getName() + ".ACTUAL";

	static final String EXPECTED = ChunkItemMessageMasterWriter.class.getName() + ".EXPECTED";

	private static final long DEFAULT_THROTTLE_LIMIT = 6;

	private ChunkMessageGateway messagingGateway;

	private LocalState localState = new LocalState();

	private long throttleLimit = DEFAULT_THROTTLE_LIMIT;

	private int DEFAULT_MAX_WAIT_TIMEOUTS = 40;

	private int maxWaitTimeouts = DEFAULT_MAX_WAIT_TIMEOUTS;

	/**
	 * The maximum number of times to wait at the end of a step for a non-null
	 * result from the remote workers. This is a multiplier on the receive
	 * timeout set separately on the gateway. The ideal value is a compromise
	 * between allowing slow workers time to finish, and responsiveness if there
	 * is a dead worker. Defaults to 40.
	 * 
	 * @param maxWaitTimeouts
	 *            the maximum number of wait timeouts
	 */
	public void setMaxWaitTimeouts(int maxWaitTimeouts) {
		this.maxWaitTimeouts = maxWaitTimeouts;
	}

	/**
	 * Public setter for the throttle limit. This limits the number of pending
	 * requests for chunk processing to avoid overwhelming the receivers.
	 * 
	 * @param throttleLimit
	 *            the throttle limit to set
	 */
	public void setThrottleLimit(long throttleLimit) {
		this.throttleLimit = throttleLimit;
	}

	public ChunkMessageGateway getMessagingGateway() {
		return messagingGateway;
	}

	public void setMessagingGateway(ChunkMessageGateway messagingGateway) {
		this.messagingGateway = messagingGateway;
	}

	public void setMessagingOperations(ChunkMessageGateway messagingGateway) {
		this.messagingGateway = messagingGateway;
	}

	public void write(List<? extends T> items) throws Exception {

		// Block until expecting <= throttle limit
		while (localState.getExpecting() > throttleLimit) {
			getNextResult();
		}

		if (!items.isEmpty()) {
			ChunkMessagePackage requestBody = localState.createRequest(items);
			if (logger.isDebugEnabled()) {
				logger.debug("Dispatching chunk: {}", requestBody);
			}

			messagingGateway.send(requestBody);
			localState.incrementExpected();
			logger.info("Master send chunk request : {}", requestBody);
		}

	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
		localState.setStepExecution(stepExecution);
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		if (!(stepExecution.getStatus() == BatchStatus.COMPLETED)) {
			return ExitStatus.EXECUTING;
		}
		long expecting = localState.getExpecting();
		boolean timedOut;
		try {
			logger.debug("Waiting for results in step listener...");
			timedOut = !waitForResults();
			logger.debug("Finished waiting for results in step listener.");
		} catch (RuntimeException e) {
			logger.debug("Detected failure waiting for results in step listener.", e);
			stepExecution.setStatus(BatchStatus.FAILED);
			return ExitStatus.FAILED.addExitDescription(e.getClass().getName() + ": " + e.getMessage());
		} finally {

			if (logger.isDebugEnabled()) {
				logger.debug("Finished waiting for results in step listener.  Still expecting: {}",
						localState.getExpecting());
			}

			for (StepContribution contribution : getStepContributions()) {
				stepExecution.apply(contribution);
			}
		}
		if (timedOut) {
			stepExecution.setStatus(BatchStatus.FAILED);
			return ExitStatus.FAILED.addExitDescription(
					"Timed out waiting for " + localState.getExpecting() + " backlog at end of step");
		}
		return ExitStatus.COMPLETED.addExitDescription("Waited for " + expecting + " results.");
	}

	public void close() throws ItemStreamException {
		localState.reset();
	}

	public void open(ExecutionContext executionContext) throws ItemStreamException {
		if (executionContext.containsKey(EXPECTED)) {
			localState.open(executionContext.getInt(EXPECTED), executionContext.getInt(ACTUAL));
			if (!waitForResults()) {
				throw new ItemStreamException("Timed out waiting for back log on open");
			}
		}
	}

	public void update(ExecutionContext executionContext) throws ItemStreamException {
		executionContext.putInt(EXPECTED, localState.expected.intValue());
		executionContext.putInt(ACTUAL, localState.actual.intValue());
	}

	public Collection<StepContribution> getStepContributions() {
		List<StepContribution> contributions = new ArrayList<StepContribution>();
		for (ChunkResponse response : localState.pollChunkResponses()) {
			StepContribution contribution = response.getStepContribution();
			if (logger.isDebugEnabled()) {
				logger.debug("Applying: {}", response);
			}
			contributions.add(contribution);
		}
		return contributions;
	}

	/**
	 * Wait until all the results that are in the pipeline come back to the
	 * reply channel.
	 * 
	 * @return true if successfully received a result, false if timed out
	 */
	private boolean waitForResults() throws AsynchronousFailureException {
		int count = 0;
		int maxCount = maxWaitTimeouts;
		Throwable failure = null;
		logger.info("Waiting for {} results", localState.getExpecting());
		while (localState.getExpecting() > 0 && count++ < maxCount) {
			try {
				getNextResult();
			} catch (Throwable t) {
				logger.error(
						"Detected error in remote result. Trying to recover {} outstanding results before completing.",
						localState.getExpecting(), t);
				failure = t;
			}
		}
		if (failure != null) {
			throw wrapIfNecessary(failure);
		}
		return count < maxCount;
	}

	/**
	 * Get the next result if it is available (within the timeout specified in
	 * the gateway), otherwise do nothing.
	 * 
	 * @throws AsynchronousFailureException
	 *             If there is a response and it contains a failed chunk
	 *             response.
	 * 
	 * @throws IllegalStateException
	 *             if the result contains the wrong job instance id (maybe we
	 *             are sharing a channel and we shouldn't be)
	 */
	private void getNextResult() throws AsynchronousFailureException {
		int eched = 0;
		for (ChunkMessagePackage requestBody : localState.getRequestList()) {
			ChunkMessagePackage responseBody = messagingGateway.recv(requestBody);
			if (responseBody == null)
				continue;

			logger.info("Master recv chunk response : {}", responseBody);
			localState.removeRequest(requestBody);
			eched++;

			ChunkResponse payload = (ChunkResponse) responseBody.getPayLoad();
			if (logger.isDebugEnabled()) {
				logger.debug("Found result: {}", payload);
			}
			Long jobInstanceId = payload.getJobId();
			Assert.state(jobInstanceId != null, "Message did not contain job instance id.");
			Assert.state(jobInstanceId.equals(localState.getJobId()), "Message contained wrong job instance id ["
					+ jobInstanceId + "] should have been [" + localState.getJobId() + "].");
			if (payload.isRedelivered()) {
				logger.warn(
						"Redelivered result detected, which may indicate stale state. In the best case, we just picked up a timed out message "
								+ "from a previous failed execution. In the worst case (and if this is not a restart), "
								+ "the step may now timeout.  In that case if you believe that all messages "
								+ "from workers have been sent, the business state "
								+ "is probably inconsistent, and the step will fail.");
				localState.incrementRedelivered();
			}
			localState.pushResponse(payload);
			localState.incrementActual();
			if (!payload.isSuccessful()) {
				throw new AsynchronousFailureException(
						"Failure or interrupt detected in handler: " + payload.getMessage());
			}
		}

		if (eched == 0) {
			try {
				Thread.sleep(this.maxWaitTimeouts);
			} catch (InterruptedException e) {
			}
		} else {
			localState.cleanRequestList();
		}
	}

	/**
	 * Re-throws the original throwable if it is unchecked, wraps checked
	 * exceptions into {@link AsynchronousFailureException}.
	 */
	private static AsynchronousFailureException wrapIfNecessary(Throwable throwable) {
		if (throwable instanceof Error) {
			throw (Error) throwable;
		} else if (throwable instanceof AsynchronousFailureException) {
			return (AsynchronousFailureException) throwable;
		} else {
			return new AsynchronousFailureException("Exception in remote process", throwable);
		}
	}

	private static class LocalState {

		private AtomicInteger current = new AtomicInteger(-1);

		private AtomicInteger actual = new AtomicInteger();

		private AtomicInteger expected = new AtomicInteger();

		private AtomicInteger redelivered = new AtomicInteger();

		private StepExecution stepExecution;

		private Queue<ChunkResponse> contributions = new LinkedBlockingQueue<ChunkResponse>();

		private List<ChunkMessagePackage> deliverList = new LinkedList<ChunkMessagePackage>();

		private List<ChunkMessagePackage> deletingList = new LinkedList<ChunkMessagePackage>();

		public int getExpecting() {
			return expected.get() - actual.get();
		}

		public <T> ChunkRequest<T> getRequest(List<? extends T> items) {
			return new ChunkRequest<T>(current.incrementAndGet(), items, getJobId(), createStepContribution());
		}

		public ChunkMessagePackage createRequest(List items) {
			ChunkRequest chunkRequest = new ChunkRequest(current.incrementAndGet(), items, getJobId(),
					createStepContribution());
			ChunkMessagePackage chunkPackage = new ChunkMessagePackage(chunkRequest);

			String processBeanId = null;
			String writeBeanId = null;
			Object firstItem = items.get(0);
			if (firstItem instanceof Map) {
				processBeanId = (String) ((Map) firstItem).get(ChunkParameterListener.PROCESSBEANID);
				writeBeanId = (String) ((Map) firstItem).get(ChunkParameterListener.WRITEBEANID);
			} else {
				if (firstItem instanceof ChunkProcessBean) {
					processBeanId = ((ChunkProcessBean) firstItem).getProcessBeanId();
				}
				if (firstItem instanceof ChunkWriteBean) {
					writeBeanId = ((ChunkWriteBean) firstItem).getWriteBeanId();
				}
			}
			if (processBeanId == null || processBeanId.length() == 0 || writeBeanId == null
					|| writeBeanId.length() == 0) {
				throw new RuntimeException("Maybe no " + ChunkParameterListener.PROCESSBEANID + " or "
						+ ChunkParameterListener.WRITEBEANID + " assigned for remote chunk.");
			}

			chunkPackage.setProcessBeanId(processBeanId);
			chunkPackage.setWriteBeanId(writeBeanId);

			synchronized (deliverList) {
				deliverList.add(chunkPackage);
			}
			return chunkPackage;
		}

		public void removeRequest(ChunkMessagePackage request) {
			synchronized (deliverList) {
				deletingList.add(request);
			}
		}

		public List<ChunkMessagePackage> getRequestList() {
			return deliverList;
		}

		public void cleanRequestList() {
			synchronized (deliverList) {
				for (ChunkMessagePackage p : this.deletingList) {
					deliverList.remove(p);
				}
				deletingList.clear();
			}

		}

		public void open(int expectedValue, int actualValue) {
			actual.set(actualValue);
			expected.set(expectedValue);
		}

		public Collection<ChunkResponse> pollChunkResponses() {
			Collection<ChunkResponse> set = new ArrayList<ChunkResponse>();
			synchronized (contributions) {
				ChunkResponse item = contributions.poll();
				while (item != null) {
					set.add(item);
					item = contributions.poll();
				}
			}
			return set;
		}

		public void pushResponse(ChunkResponse stepContribution) {
			synchronized (contributions) {
				contributions.add(stepContribution);
			}
		}

		public void incrementRedelivered() {
			redelivered.incrementAndGet();
		}

		public void incrementActual() {
			actual.incrementAndGet();
		}

		public void incrementExpected() {
			expected.incrementAndGet();
		}

		public StepContribution createStepContribution() {
			return stepExecution.createStepContribution();
		}

		public Long getJobId() {
			return stepExecution.getJobExecution().getJobId();
		}

		public void setStepExecution(StepExecution stepExecution) {
			this.stepExecution = stepExecution;
		}

		public void reset() {
			expected.set(0);
			actual.set(0);
		}
	}

}
