package org.springframework.batch.message;

public abstract interface ChunkMessageHandler<T> {
	public abstract void handle(T input) throws Exception;
}