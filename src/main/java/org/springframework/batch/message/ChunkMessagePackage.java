package org.springframework.batch.message;

import java.io.Serializable;
import java.util.UUID;

public class ChunkMessagePackage implements Serializable {
	private static final long serialVersionUID = 577687108225047580L;
	public String messageID;
	public String processBeanId = null;
	public String writeBeanId = null;
	public Object payLoad;

	public ChunkMessagePackage(Object body) {
		this.messageID = UUID.randomUUID().toString().replaceAll("-", "");
		this.payLoad = body;
	}

	public ChunkMessagePackage(String messageID, Object body) {
		this.messageID = messageID;
		this.payLoad = body;
	}

	public String getMessageID() {
		return messageID;
	}

	public void setMessageID(String messageID) {
		this.messageID = messageID;
	}

	public String getProcessBeanId() {
		return processBeanId;
	}

	public void setProcessBeanId(String processBeanId) {
		this.processBeanId = processBeanId;
	}

	public String getWriteBeanId() {
		return writeBeanId;
	}

	public void setWriteBeanId(String writeBeanId) {
		this.writeBeanId = writeBeanId;
	}

	public Object getPayLoad() {
		return payLoad;
	}

	public void setPayLoad(Object body) {
		this.payLoad = body;
	}

	public ChunkMessagePackage createResponse(Object body) {
		ChunkMessagePackage p = new ChunkMessagePackage(this.getMessageID(), body);
		p.setProcessBeanId(this.getProcessBeanId());
		p.setWriteBeanId(this.getWriteBeanId());
		return p;
	}

	@Override
	public String toString() {
		return "ChunkMessagePackage [messageID=" + messageID + ", processBeanId=" + processBeanId + ", writeBeanId="
				+ writeBeanId + ", payLoad=" + payLoad + "]";
	}

}
