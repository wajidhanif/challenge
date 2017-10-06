package the.floow.challenge.entity;

import the.floow.challenge.utils.Util;

public class InputParameter {
	public String fileName;
	public String filePath;
	public DataSource dataSouce;
	
	public InputParameter(String filePath, DataSource ds) {
		this.filePath = filePath;
		this.fileName = Util.getFileName(filePath);
		this.dataSouce = ds;
	}
}

