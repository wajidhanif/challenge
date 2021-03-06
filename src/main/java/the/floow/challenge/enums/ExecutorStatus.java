package the.floow.challenge.enums;

public enum ExecutorStatus {
	LIVE("live"), 
	STANDBY("standby"), 
	STOP("stop");

	private final String value;

	ExecutorStatus(final String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
