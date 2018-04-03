package org.springframework.batch.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

public abstract class ChunkMessageGateway {

	protected final static Logger logger = LoggerFactory.getLogger(ChunkMessageGateway.class);

	/**
	 * 消息队列URL，"/192.168.15.101:9876/系统代码/子系统代码"格式.<br>
	 * 例如: <br>
	 * RMQ：/$NamesrvAddr/$group/$topic".<br>
	 * AMQ：/$NamesrvAddr/$queue/$topic".<br>
	 */
	protected String mqURL;

	/**
	 * 异步服务执行端调用.
	 */
	protected ChunkMessageHandler handler;

	/**
	 * 消费服务线程数.
	 */
	protected int defaultConsumerThreads = 2;

	final static ConcurrentSkipListSet concurrentConsumeMesgID = new ConcurrentSkipListSet();

	private final static int MAX_CONNECT_RETRY = 3;

	protected String addr;

	protected String group;

	protected String topic;

	protected int clientSize = 1;

	protected int clientThreads = 0;

	protected List<DefaultMQProducer> producers = new ArrayList<DefaultMQProducer>();

	protected Random producerRandom = new Random(System.currentTimeMillis());

	protected ConcurrentHashMap<String, MQConsumer> consumerMap;

	public int getDefaultConsumerThreads() {
		return defaultConsumerThreads;
	}

	public void setDefaultConsumerThreads(int defaultConsumerThreads) {
		this.defaultConsumerThreads = defaultConsumerThreads;
	}

	public Random getProducerRandom() {
		return producerRandom;
	}

	public void setProducerRandom(Random producerBase) {
		this.producerRandom = producerBase;
	}

	public ConcurrentHashMap<String, MQConsumer> getConsumerMap() {
		return consumerMap;
	}

	public void setConsumerMap(ConcurrentHashMap<String, MQConsumer> consumerMap) {
		this.consumerMap = consumerMap;
	}

	public String getMqURL() {
		return mqURL;
	}

	public static ConcurrentSkipListSet getConcurrentconsumemesgid() {
		return concurrentConsumeMesgID;
	}

	public ChunkMessageHandler getHandler() {
		return handler;
	}

	public void setHandler(ChunkMessageHandler handler) {
		this.handler = handler;
	}

	class MQConsumer {
		public DefaultMQPushConsumer c = null;

		public String trcode = null;

		public AtomicLong r = new AtomicLong(0L);

		public boolean alive = true;

		public MQConsumer(DefaultMQPushConsumer consumer) {
			this.c = consumer;
		}

		public void shutdown() {
			if (c != null)
				c.shutdown();
			c = null;
		}

		public String toString() {
			return "MQConsumer [trcode=" + trcode + ", r=" + r.get() + ", c=" + c + "]";
		}

	}

	public ChunkMessageGateway() {
		super();
		consumerMap = new ConcurrentHashMap<String, MQConsumer>();
	}

	public void setMqURL(String mqURL) {
		this.mqURL = mqURL;
		String[] elems = mqURL.split("/");
		if (elems.length >= 1)
			addr = elems[1];
		if (elems.length >= 2)
			group = elems[2];
		if (elems.length >= 3)
			topic = elems[3];
		if (addr == null || addr.length() == 0)
			addr = "127.0.0.1:9876";
		if (group == null || group.length() == 0)
			group = "CCBS";
		if (topic == null || topic.length() == 0)
			topic = "ccbspe";
		logger.debug("MqURL: addr=" + addr + ", group=" + group + ", topic=" + topic);
	}

	public int getClientThreads() {
		return clientThreads;
	}

	public void setClientThreads(int clientThreads) {
		this.clientThreads = clientThreads;
	}

	public int getClientSize() {
		return clientSize;
	}

	public void setClientSize(int clientSize) {
		this.clientSize = clientSize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ChunkMessageMasterGateway [addr=" + addr + ", group=" + group + ", topic=" + topic + ", producers="
				+ producers + ", clientThreads=" + clientThreads + "]";
	}

	public void startProducer() {
		synchronized (mqURL) {
			if (producers.size() > 0)
				return;

			for (int n = 0; n < clientSize; n++) {
				DefaultMQProducer producer = null;
				logger.debug("try to start " + (n + 1) + "/" + clientSize + " producer ...");
				for (int ii = 0; ii < MAX_CONNECT_RETRY; ii++) {
					try {
						producer = new DefaultMQProducer("PRODUCER_" + group + "_" + topic + "_" + n);
						producer.setNamesrvAddr(addr);
						producer.setInstanceName("PRODUCER_" + this.group + "_"
								+ UUID.randomUUID().toString().replaceAll("-", "") + "_" + n);
						if (clientThreads > 0)
							producer.setClientCallbackExecutorThreads(clientThreads);
						producer.setRetryAnotherBrokerWhenNotStoreOK(true);
						producer.setRetryTimesWhenSendFailed(3);
						logger.debug(producer.toString());
						producer.start();

						while (true) {
							ServiceState serviceState = producer.getDefaultMQProducerImpl().getServiceState();
							logger.info("ChunkMessageMasterGateway.Producer " + producer + " current state : "
									+ serviceState);
							if (serviceState.equals(ServiceState.RUNNING)) {
								logger.debug(
										"ChunkMessageMasterGateway.Producer " + producer + " has start completely. ");
								break;
							} else if (serviceState.equals(ServiceState.SHUTDOWN_ALREADY)) {
								logger.warn(
										"ChunkMessageMasterGateway.Producer " + producer + " was SHUTDOWN_ALREADY. ");
								producer.shutdown();
								producer = null;
								break;
							} else if (serviceState.equals(ServiceState.START_FAILED)) {
								logger.warn("ChunkMessageMasterGateway.Producer " + producer + " has start FAILED. ");
								producer.shutdown();
								producer = null;
								break;
							} else {
								try {
									Thread.currentThread().sleep(10);
								} catch (InterruptedException e) {
								}
							}
						}
						logger.info("ChunkMessageMasterGateway.Producer " + producer + " started, sendMsgTimeout:"
								+ producer.getSendMsgTimeout() + ", ");
						break;
					} catch (MQClientException e) {
						if (producer != null)
							producer.shutdown();
						producer = null;

						if (ii < MAX_CONNECT_RETRY - 1)
							logger.warn("启动Producer异常:" + mqURL);
						else
							throw new RuntimeException("start Producer " + this.mqURL + " exception", e);
					}
				}

				if (producer != null)
					producers.add(producer);
			}
		}
	}

	private void stopProducer() {
		if (producers.size() > 0) {
			for (DefaultMQProducer producer : producers) {
				producer.shutdown();
				logger.debug("ChunkMessageMasterGateway producer " + producer + " was shutdown.");
			}
			producers.clear();
			producers = null;
		}
	}

	private DefaultMQProducer getProducer() {
		if (producers == null || producers.size() == 0)
			startProducer();

		if (producers.size() > 0)
			return producers.get(producerRandom.nextInt(producers.size()));
		return null;
	}

	private void closeProducer(DefaultMQProducer producer) {
		if (producer == null)
			return;
		if (producers != null && producers.contains(producer)) {
			producers.remove(producer);
			try {
				producer.shutdown();
			} catch (Exception e) {
				logger.warn("close the producer " + producer + " exception.", e);
			}
		}
	}

	public void startConsumer(ChunkMessageQue que, int threads, final ChunkMessageHandler handler) {
		synchronized (consumerMap) {
			String tags = que.getValue();
			if (threads < 0)
				threads = defaultConsumerThreads;
			MQConsumer consumer1 = (MQConsumer) consumerMap.get(tags);
			if (consumer1 != null) {
				logger.warn(
						"ChunkMessageMasterGateway consumer " + tags + " has started, please check it : " + consumer1);
				return;
			}
			final String Tags = tags;
			final String Topic = topic;
			for (int ii = 0; ii < MAX_CONNECT_RETRY; ii++) {
				final MQConsumer consumer = new MQConsumer(
						new DefaultMQPushConsumer("CONSUMER_" + group + "_" + Topic + "_" + Tags));
				consumer.trcode = Tags;
				consumer.c.setNamesrvAddr(addr);
				consumer.c.setInstanceName(
						"CONSUMER_" + this.group + "_" + UUID.randomUUID().toString().replaceAll("-", ""));
				try {
					consumer.c.subscribe(Topic, Tags);
					logger.debug("ChunkMessageMasterGateway subscribe Topic:" + Topic + ", Tags:" + Tags
							+ ", ConsumeThreads:" + threads);
					consumer.c.setPullBatchSize(1);
					consumer.c.setConsumeThreadMin(threads);
					consumer.c.setConsumeThreadMax(threads);
					consumer.c.setClientCallbackExecutorThreads(threads < 4 ? 1
							: (threads / 2 > Runtime.getRuntime().availableProcessors()
									? Runtime.getRuntime().availableProcessors() : threads / 2));
					consumer.c.registerMessageListener(new MessageListenerConcurrently() {

						@Override
						public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
								ConsumeConcurrentlyContext arg1) {
							MessageExt msg = (msgs == null || msgs.size() == 0) ? null : msgs.get(0);
							if (msg == null)
								return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

							synchronized (consumer) {
								if (!consumer.alive) {
									return ConsumeConcurrentlyStatus.RECONSUME_LATER;
								}

								if (concurrentConsumeMesgID.contains(msg.getMsgId())) {
									logger.warn("Received MesgID " + msg.getMsgId() + " is on proccessing, ignore it.");
									return ConsumeConcurrentlyStatus.RECONSUME_LATER;
								} else {
									concurrentConsumeMesgID.add(msg.getMsgId());
								}

								consumer.r.getAndIncrement();
							}

							logger.trace("ChunkMessageMasterGateway.Consumer recved : " + msg);
							logger.debug(
									"ChunkMessageMasterGateway.Consumer recved body : " + new String(msg.getBody()));

							/**
							 * proccessing the message.
							 */
							Object o = deserialize(msg.getBody());
							logger.debug("ChunkMessageMasterGateway.Consumer recved object : " + o);
							try {
								handler.handle(o);
							} catch (Throwable th) {
								logger.trace("ChunkMessageMasterGateway.Consumer handle exception ", th);
							}

							consumer.r.getAndDecrement();
							concurrentConsumeMesgID.remove(msg.getMsgId());
							return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
						}

					});
					consumer.c.start();
					logger.info("Listened on Consumer(" + Topic + "," + Tags + ") " + consumer);
					consumerMap.put(Tags, consumer);
					break;
				} catch (MQClientException e) {
					if (consumer.c != null)
						consumer.c.shutdown();

					if (ii < MAX_CONNECT_RETRY - 1)
						logger.warn("start Consumer " + this.mqURL + " exception.", e);
					else
						throw new RuntimeException("start Consumer " + this.mqURL + " exception.", e);
				}
			}
		}
	}

	/**
	 * 
	 * @param t
	 * @return
	 */
	public static byte[] serialize(Object t) {
		ObjectOutputStream out = null;
		try {
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			out = new ObjectOutputStream(bout);
			out.writeObject(t);
			out.flush();
			byte[] target = bout.toByteArray();
			return target;
		} catch (IOException e) {
			throw new RuntimeException(
					"JDKMsgSerializer serialize " + (t == null ? "nvl" : t.getClass()) + " exception", e);
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
				}
			}
		}
	}

	/**
	 * 
	 * @param bytes
	 * @return
	 */
	public static <T> T deserialize(byte[] bytes) {
		try {
			ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));
			return (T) in.readObject();
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException("JDKMsgSerializer deserialize exception", e);
		}
	}

	public void send(ChunkMessageQue que, Object body) {
		Message msg = null;
		String mesgID = UUID.randomUUID().toString().replace("-", "").replace("_", "");
		try {
			byte[] sendBytes = serialize(body);
			msg = new Message(topic, que.getValue(), mesgID, sendBytes);
		} catch (Throwable e) {
			throw new RuntimeException("Generate message for " + this.mqURL + " failed.", e);
		}

		DefaultMQProducer producer = null;
		SendResult sendResult = null;
		Exception cte = null;
		for (int i = 0; i < 3; i++) {
			producer = getProducer();
			if (producer == null)
				throw new RuntimeException("Get connection for " + this.mqURL + " failed.", null);

			try {
				sendResult = producer.send(msg);
				// SendStatus sendStatus = sendResult.getSendStatus();
				// sendStatus.SEND_OK;
				// sendStatus.SLAVE_NOT_AVAILABLE;
				// sendStatus.FLUSH_DISK_TIMEOUT;
				// sendStatus.FLUSH_SLAVE_TIMEOUT;
				logger.debug("[" + mesgID + ":" + i + "] Send Message Result " + sendResult);
				cte = null;
				break;
			} catch (MQClientException e) {
				logger.error("[" + mesgID + ":" + i + "] Send Message Result " + sendResult
						+ ", catch MQClientException: RocketMQ.ResponseCode:" + e.getResponseCode() + ", ErrorMessage:"
						+ e.getErrorMessage());
				closeProducer(producer);
				cte = e;
			} catch (RemotingException e) {
				logger.error("[" + mesgID + ":" + i + "] Send Message Result " + sendResult
						+ ", catch RemotingException" + e);
				closeProducer(producer);
				cte = e;
			} catch (MQBrokerException e) {
				logger.error("[" + mesgID + ":" + i + "] Send Message Result " + sendResult
						+ ", catch MQBrokerException: RocketMQ.ResponseCode:" + e.getResponseCode() + ", ErrorMessage:"
						+ e.getErrorMessage());
				closeProducer(producer);
				cte = e;
			} catch (InterruptedException e) {
				logger.error("[" + mesgID + ":" + i + "] Send Message Result " + sendResult
						+ ", catch InterruptedException.");
				cte = e;
				break;
			}
		}
		if (cte != null)
			throw new RuntimeException("send message to " + this.mqURL + " failed.", cte);
	}

	public void stopConsumer() {
		while (consumerMap != null && consumerMap.size() > 0) {
			Iterator<String> iter = consumerMap.keySet().iterator();
			while (iter.hasNext()) {
				String k = (String) iter.next();
				MQConsumer consumer = consumerMap.get(k);
				consumer.alive = false;
				if (consumer.r.get() == 0) {
					synchronized (consumer) {
						if (consumer.r.get() == 0) {
							consumer.shutdown();
							consumerMap.remove(k);
							logger.debug("ChunkMessageMasterGateway consumer " + consumer + " was shutdown.");
						} else {
							logger.warn(
									"ChunkMessageMasterGateway consumer " + consumer + " is running, waiting for it.");
						}
					}
				} else {
					logger.warn("ChunkMessageMasterGateway consumer " + consumer + " is running, waiting for it.");
				}
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
		logger.debug("All the consumer closed : " + consumerMap.size());
	}

	public void onInit() {
		startProducer();
		startConsumer();
	}

	public void onClose() {
		stopProducer();
		stopConsumer();
	}

	abstract public void startConsumer();
	
	abstract public void send(ChunkMessagePackage requestBody);

	abstract public ChunkMessagePackage recv(ChunkMessagePackage requestBody);
}
