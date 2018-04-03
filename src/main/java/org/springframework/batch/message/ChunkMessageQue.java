package org.springframework.batch.message;

public enum ChunkMessageQue {
	MASTER("SPRINGBATCH_REQUESTS", "master request"), SLAVE("SPRINGBATCH_REPLIES", "slave reply");

	private String value;
	private String desc;

	ChunkMessageQue(String value, String desc) {
		this.value = value;
		this.desc = desc;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

}
