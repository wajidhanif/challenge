package the.floow.challenge.Executors;

import java.util.List;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import the.floow.challenge.dao.MongoMessageQueue;
import the.floow.challenge.entity.InputParameter;
import the.floow.challenge.entity.QueueMessage;
import the.floow.challenge.enums.BlockStatus;
import the.floow.challenge.enums.FileStatus;
import the.floow.challenge.service.WorkerControllerService;

public class WorkerController implements Runnable {

	public InputParameter inputParams;
	public ObjectId executorID;
	public ObjectId fileID;
	public WorkerControllerService wControllerService;
	final static Logger logger = Logger.getLogger(WorkerController.class);

	public WorkerController(InputParameter inputParams, ObjectId executorID, ObjectId fileID) {
		this.inputParams = inputParams;
		this.executorID = executorID;
		this.fileID = fileID;
		this.wControllerService = new WorkerControllerService(inputParams, executorID, fileID);
	}

	@Override
	public void run() {
		try {
			
			boolean isRunning = true;
			while (isRunning) {

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
				// stop the server if all block processed and written to
				// database
				isRunning = this.wControllerService.isProcessFinished();
			
				// update file status
				if(!isRunning){
					this.wControllerService.updateFileStaus(FileStatus.PROCESSED);
				}
			}
		} catch (Exception ex) {
			logger.error("Exception Occurs (Please see logs for more details:" + ex.getMessage());
		}
		logger.debug("File has been processed!");
	}
}
