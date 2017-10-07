package the.floow.challenge.Executors;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import the.floow.challenge.entity.DataSource;
import the.floow.challenge.entity.InputParameter;
import the.floow.challenge.processor.WordCountProcessor;
import the.floow.challenge.service.ExecutorService;

public class WordsCountExecutor implements GenericExecutor {

	public ObjectId executorID;
	public DataSource ds;
	public String fileName;
	public String filePath;
	public InputParameter inParams;
	public ExecutorService executorService;
	final static Logger logger = Logger.getLogger(WordsCountExecutor.class);

	public WordsCountExecutor(InputParameter inParams) {
		this.ds = inParams.dataSouce;
		this.filePath = inParams.filePath;
		this.fileName = inParams.filePath;
		this.inParams = inParams;
		this.executorService = new ExecutorService(inParams);
	}

	public void execute() {
		try {
			// register executor and create file if no present
			ObjectId executorID = executorService.register();
			ObjectId fileID = executorService.createFile(executorID);

			if (fileID != null) {
				WorkerController controller = new WorkerController(this.inParams, executorID, fileID);
				Thread controllerThread = new Thread(controller);
				controllerThread.start();

				Worker worker = new Worker(this.inParams, executorID, fileID,new WordCountProcessor());
				Thread workerThread = new Thread(worker);
				workerThread.start();
			}
		} catch (Exception Ex) {
			logger.error("Exception Occurs (Please see logs for more details:" + Ex.getMessage());
		} 
	}
}
