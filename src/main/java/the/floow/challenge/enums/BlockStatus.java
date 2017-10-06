package the.floow.challenge.enums;

public enum BlockStatus {
	
	AVAILABLE("available"), 
	PROCESSING("processing"), 
	PROCESSED("processed"), 
	WRITTEN("written"),
	QUEUED("queued");

	private final String value;

	BlockStatus(final String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
