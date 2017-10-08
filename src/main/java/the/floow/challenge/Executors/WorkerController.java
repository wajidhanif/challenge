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
	
	public void waitForFailover() throws InterruptedException{
		boolean isFailover = false;
		/*Check server failure case */
		while(!isFailover){
			// check whether current executor is a server or not. 
			logger.debug("WaitForFailover: " + this.wControllerService.executorID);
			if(this.wControllerService.isExecutorAsServer()){
				isFailover = true;
			}else{
				Thread.sleep(this.wControllerService.serverDefaultWait);
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
	
	@Override
	public void run() {		
		
		try {
			logger.debug("Check  Failover : ");
			// controller will wait for failover if any
			this.waitForFailover();
			logger.debug("Controller Running: ");
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
							logger.debug("Enqueue: " + blockNo);
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
