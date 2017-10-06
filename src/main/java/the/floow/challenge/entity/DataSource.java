package the.floow.challenge.entity;

public class DataSource {

	private String ip;
	private int port;
	private String dbName;

	public DataSource(String ip, int port) {
		this.ip = ip;
		this.port = port;
		this.dbName = "challenge";
	}
	
	public DataSource(String ip, int port, String dbName) {
		this.ip = ip;
		this.port = port;
		this.dbName = dbName;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getDbName() {
		return dbName;
	}
}
