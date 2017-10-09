package the.floow.challenge.Executors;

import java.util.List;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import the.floow.challenge.dao.MongoMessageQueue;
import the.floow.challenge.entity.InputParameter;
import the.floow.challenge.entity.QueueMessage;
import the.floow.challenge.enums.BlockStatus;
import the.floow.challenge.enums.ExecutorStatus;
import the.floow.challenge.enums.FileStatus;
import the.floow.challenge.service.WorkerControllerService;

/**
This is the controller runnable class which controls the distribution of 
workload between executors 
@author Wajid */
public class WorkerController implements Runnable {

	public InputParameter inputParams;
	public ObjectId executorID;
	public ObjectId fileID;
	public WorkerControllerService wControllerService;
	final static Logger logger = Logger.getLogger(WorkerController.class);
	private boolean isRunning; 

	public WorkerController(InputParameter inputParams, ObjectId executorID, ObjectId fileID) {
		this.inputParams = inputParams;
		this.executorID = executorID;
		this.fileID = fileID;
		this.wControllerService = new WorkerControllerService(inputParams, executorID, fileID);
		this.isRunning = true;
	}
	/** 
	This function continuously checks for possible failure of executor which is currently 
	distributing the workload. This function is performing following functionalities
	
	1. If there is a running executor as controller, then sleep for default server wait time.
	2. If the running executor is not responding for default wait and file is not processed yet, 
	   then make the current executor as controller. 
	*/
	
	public void waitForFailover() throws InterruptedException{
		boolean isFailover = false;
		/*Check server failure case */
		while(!isFailover){
			// check whether current executor is a server or not. 
			if(this.wControllerService.isExecutorAsServer()){
				isFailover = true;
			}else{
				Thread.sleep(this.wControllerService.serverDefaultWait);
				/*update the running time*/
				this.wControllerService.updateExectorRunningTime();
				
				boolean isServerRunning = this.wControllerService.isServerRunning(); 
				if(!isServerRunning){
					// check file processed or not
					boolean isFileProcessed =this.wControllerService.isFileProcessed();
					if(isFileProcessed){
						this.isRunning = false; // no need to run the below code
						isFailover = true; 						
					}	
				}
			}				
		}
	}
	
	/** 
	This function is distributing the workload. This function is performing following functionalities
	
	1.Gets all the available file blocks from database
	2.If the message queue is empty or max queue size is not reached yet, then it reads the file block 
	  from file and puts it in the message queue for executors to process it
	3.If all blocks have been processed by executors then it calls mongodb mapreduce to calculate the counts
	4.Checks the executor failure: if the executor is not responding for default max wait time, then
	  it makes the blocks available for other executors which are processed by this executor   
	*/
	@Override
	public void run() {		
		
		try {
			// controller will wait for failover if any
			this.waitForFailover();
	
			while (this.isRunning) {
				
				List<Integer> availBlocks = this.wControllerService.getFileAvailableBlocks(this.fileID);
				MongoMessageQueue queue = this.wControllerService.getMessageQueue();

				// if blocks available for processing
				if (availBlocks.size() > 0) {
					long queueSize = queue.size();
					// Default max size of queue is twice the no of executors,
					// but Configurable by Setting collection
					long maxQueueSize = this.wControllerService.getMaxQueueSize();
					int availBlockCount = availBlocks.size();
					if (maxQueueSize > queueSize) {
						int enqBlockCount = Math.toIntExact(maxQueueSize - queueSize);
						if (enqBlockCount > availBlockCount) {
							enqBlockCount = availBlockCount;
						}
						for (int i = 0; i < enqBlockCount; i++) {
							Integer blockNo = availBlocks.get(i);
							// make the block status as queued
							this.wControllerService.updateBlockStatus(blockNo, BlockStatus.QUEUED);
							QueueMessage message = this.wControllerService.getQueueMessage(blockNo);
							queue.enqueue(message);
						}
					}
				}
				/* stop the server if all block processed and written to database*/
				isRunning = this.wControllerService.isProcessFinished();
				
				// update file status and count words in db using map reduce
				if(!isRunning){
					this.wControllerService.mapReduceWords();
					this.wControllerService.updateFileStaus(FileStatus.PROCESSED);
					this.wControllerService.updateExectorServerInfo();
					
				}else{
					this.wControllerService.checkWorkerFailOver();
				}
				// update running timestamp of executor
				this.wControllerService.updateExectorRunningTime();
			}
		} catch (Exception ex) {
			logger.error("Exception Occurs (Please see logs for more details:" + ex.getMessage());
			logger.error(ex);			
		}finally{
			/*if any exception come, then update the executor isServer parameter*/
			if(isRunning){
				this.wControllerService.updateExectorServerInfo();
			}
			this.wControllerService.updateExectorStatus(ExecutorStatus.STOP);
		}
		logger.debug("File has been processed!");
		System.out.println("File has been processed!");
	}
}
