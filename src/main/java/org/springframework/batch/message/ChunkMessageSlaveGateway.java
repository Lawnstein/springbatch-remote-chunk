package org.springframework.batch.message;

public class ChunkMessageSlaveGateway extends ChunkMessageGateway {

	public void startConsumer() {
		startConsumer(ChunkMessageQue.MASTER, getDefaultConsumerThreads(), getHandler());
	}

	public void send(ChunkMessagePackage responseBody) {
		send(ChunkMessageQue.SLAVE, responseBody);
	}

	public ChunkMessagePackage recv(ChunkMessagePackage requestBody) {
		return null;
	}

}
