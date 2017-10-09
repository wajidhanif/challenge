package the.floow.challenge.Executors;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import the.floow.challenge.entity.DataSource;
import the.floow.challenge.entity.InputParameter;
import the.floow.challenge.processor.CountProcessor;
import the.floow.challenge.processor.WordCountProcessor;
import the.floow.challenge.service.ExecutorService;

/** 
This is the count executor class using as central class for counting 
Words/Lines/Number etc. By default this class behaves as a word count executor. 
@author Wajid; */
public class CountExecutor implements GenericExecutor {

	public ObjectId executorID;
	public DataSource ds;
	public String fileName;
	public String filePath;
	public InputParameter inParams;
	public ExecutorService executorService;
	final static Logger logger = Logger.getLogger(CountExecutor.class);
	private CountProcessor countProcessor;

	/**
	This is the constructor of count executor with default word count processor 
    @param inParams the input parameters object */
	public CountExecutor(InputParameter inParams) {
		this.ds = inParams.dataSouce;
		this.filePath = inParams.filePath;
		this.fileName = inParams.filePath;
		this.inParams = inParams;
		this.executorService = new ExecutorService(inParams);
		this.countProcessor = new WordCountProcessor();
	}
	/**
	This is the constructor of count executor where you can pass the count processor
    @param inParams the input parameters object 
    @param cProcessor the count processor object (Word/Number/Line) */
	public CountExecutor(InputParameter inParams, CountProcessor cProcessor) {
		this.ds = inParams.dataSouce;
		this.filePath = inParams.filePath;
		this.fileName = inParams.filePath;
		this.inParams = inParams;
		this.executorService = new ExecutorService(inParams);
		this.countProcessor = cProcessor;
	}
	/**
	This is the main function of executor which is performing following functionalities
	1. Register the executor in database
	2. If the source parameter exists, then create the file and file-blocks object in database.
	3. If the source parameter exists, then this executor can be a candidate for controlling 
	   the Workload distribution.	   
	4. If source  parameter is missing, then this executor can only work as 
	   a worker (getting data from database and process it)
	4. Run the WorkerController(workload distributor) and Worker threads. 
    @param inParams the input parameters object 
    @param cProcessor the count processor object (Word/Number/Line) */
	@Override	
	public void execute() {
		try {
			// register executor and create file if no present
			ObjectId executorID = executorService.register();
			ObjectId fileID = executorService.createFile(executorID);
			
			if (fileID != null) {
				/*If executor has file, then it can be a controller*/
				boolean isController = this.executorService.isExecutorAsController();
				if (isController) {
					WorkerController controller = new WorkerController(this.inParams, executorID, fileID);
					Thread controllerThread = new Thread(controller);
					controllerThread.start();
				}
				/*Initiate the Worker thread with count processor */
				Worker worker = new Worker(this.inParams, executorID, fileID, this.countProcessor);
				Thread workerThread = new Thread(worker);
				workerThread.start();
			}
		} catch (Exception Ex) {
			logger.error("Exception Occurs - Please see logs for more details:" + Ex.getMessage());
		}
	}
}
