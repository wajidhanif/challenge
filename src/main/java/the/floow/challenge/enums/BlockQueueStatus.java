package the.floow.challenge.enums;

public enum BlockQueueStatus {
	
	AVAILABLE("available"), 
	PROCESSED("processed"); 
	
	private final String value;

	BlockQueueStatus(final String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
