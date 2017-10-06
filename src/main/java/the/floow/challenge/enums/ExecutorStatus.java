package the.floow.challenge.enums;

public enum ExecutorStatus {
	LIVE("live"), 
	EXECUTING("executing"), 
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
