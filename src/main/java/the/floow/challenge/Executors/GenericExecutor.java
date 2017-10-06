package the.floow.challenge.Executors;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

//TODO: Gernice imp
public interface GenericExecutor {
	
	public void register() throws UnknownHostException;
	public List<String> readFileByBlock(int blockNum) throws IOException;
	public void countWords(int blockNum) throws IOException;
	public void mergeCounts();
	public void writeCounts(boolean isAvailable);
	public void execute();
}
