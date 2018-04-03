package org.springframework.batch.message;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkMessageMasterGateway extends ChunkMessageGateway {

	protected final static Logger logger = LoggerFactory.getLogger(ChunkMessageMasterGateway.class);
	
	private final static ConcurrentHashMap<ChunkMessagePackage, ChunkMessagePackage> responses = new ConcurrentHashMap<ChunkMessagePackage, ChunkMessagePackage>();

	private final ChunkMessagePackage DEFAULT_RESPONSE = new ChunkMessagePackage(null);
	
	public void startConsumer() {
		ChunkMessageHandler messageHandler = getHandler();
		if (messageHandler == null) {
			messageHandler = new ChunkMessageHandler<ChunkMessagePackage>() {

				@Override
				public void handle(ChunkMessagePackage responseBody) throws Exception {

					ChunkMessagePackage requestBody = null;					
					for (ChunkMessagePackage o : responses.keySet()) {
						if (o.messageID.equals(responseBody.getMessageID())) {
							requestBody = o;
							break;
						}							
					}
					
					if (requestBody == null) {
						logger.warn("No matched request found for current response {}, ignore it.", responseBody);
					} else {
						responses.put(requestBody, responseBody);
						logger.debug("Recved slave reponse {}", responseBody);
					}
				}
			};

			startConsumer(ChunkMessageQue.SLAVE, this.getDefaultConsumerThreads(), messageHandler);
		}
	}

	public void send(ChunkMessagePackage requestBody) {
		responses.put(requestBody, DEFAULT_RESPONSE);
		send(ChunkMessageQue.MASTER, requestBody);
	}

	public ChunkMessagePackage recv(ChunkMessagePackage requestBody) {
		ChunkMessagePackage responseBody = responses.get(requestBody);
		if (responseBody == DEFAULT_RESPONSE) {
			return null;
		}
		return responses.remove(requestBody);
	}


}
