package the.floow.challenge.enums;

public enum MessageQueueStatus {
	
	AVAILABLE("available"), 
	PROCESSING("processing"), 
	PROCESSED("processed"); 
	
	private final String value;

	MessageQueueStatus(final String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
