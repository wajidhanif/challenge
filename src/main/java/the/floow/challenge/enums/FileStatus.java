package the.floow.challenge.enums;

public enum FileStatus {

	CREATED("created"), 
	PROCESSING("processing"), 
	PROCESSED("processed");
	
    private final String value;

    FileStatus(final String value) {
        this.value = value;
    }
    
    public String getValue() {
        return value;
    }
}
