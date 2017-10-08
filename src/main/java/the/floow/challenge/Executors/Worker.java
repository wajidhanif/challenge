package the.floow.challenge.Executors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import the.floow.challenge.dao.MongoMessageQueue;
import the.floow.challenge.entity.InputParameter;
import the.floow.challenge.entity.QueueMessage;
import the.floow.challenge.enums.BlockStatus;
import the.floow.challenge.enums.ExecutorStatus;
import the.floow.challenge.processor.CountProcessor;
import the.floow.challenge.service.WorkerService;

public class Worker implements Runnable {

	public InputParameter inputParams;
	public ObjectId executorID;
	public ObjectId fileID;
	public WorkerService workerService;
	private ConcurrentHashMap<String, Long> wordCounts;
	private CountProcessor countProcessor;
	
	final static Logger logger = Logger.getLogger(Worker.class);

	public Worker(InputParameter inputParams, ObjectId executorID, ObjectId fileID,CountProcessor countProcessor) {
		this.inputParams = inputParams;
		this.executorID = executorID;
		this.fileID = fileID;
		this.workerService = new WorkerService(inputParams, executorID, fileID);
		this.countProcessor = countProcessor;
		this.wordCounts = new ConcurrentHashMap<String, Long>();
	}

	public void merge(ConcurrentHashMap<String, LongAdder> newCounts) throws IOException {

		newCounts.forEach((key,value) -> {
					if (!wordCounts.containsKey(key))
						wordCounts.put(key, value.longValue());
					else
						wordCounts.put(key, Long.valueOf(wordCounts.get(key).longValue() + value.longValue()));
				});
	}

	@Override
	public void run() {
		boolean isRunning = true;
		boolean isProcessed = false;
		List<Integer> processedBlocks = new ArrayList<>();
		
		logger.debug("Worker Started: "+this.workerService.executorID);
		
		try {				
			while (isRunning) {	
				// acknowledge current controller: I am running. 
				this.workerService.updateExectorRunningTime();
				
				MongoMessageQueue queue = this.workerService.getMessageQueue();
				if (!queue.empty()) {
					QueueMessage message = queue.dequeue();
					
					if (message != null) {
						logger.debug("dequeue: " + message.blockNo);
						this.workerService.updateBlockStatus(message.blockNo, BlockStatus.PROCESSING);
						this.merge(this.countProcessor.counts(message.data)); /*count + merge*/
						this.workerService.updateBlockStatus(message.blockNo, BlockStatus.PROCESSED);
						processedBlocks.add(message.blockNo);
					}
				}

				isProcessed = this.workerService.isBlockProcessed();
				/*
				 * write words to database if all blocks processed or  
				 * memory utilization > 90% (Configurable by Setting collection)
				 * 
				 */
				if ((isProcessed || this.workerService.isMemoryFull()) && this.wordCounts.size() > 0) {
					/* if this worker hang/timeout, then server stop  it.  No need to write to db.*/
					boolean isStop = this.workerService.isStopByServer();
					if(!isStop){
						this.workerService.writeCountsToDB(this.wordCounts);
						this.workerService.updateBlockStatus(processedBlocks, BlockStatus.WRITTEN);						
					}
					this.wordCounts = new ConcurrentHashMap<String, Long>();;
					processedBlocks = new ArrayList<>();
				}
				
				if(isProcessed){
					isRunning = this.workerService.isProcessFinished();
					
					/* Worker finished it's processing but all blocks are not written 
					 * Failure case may occur - Wait for 2 Minutes (default wait time - configurable by setting collection) */
					if(isRunning){
						Thread.sleep(this.workerService.workDefaultWait); 
					}
				}
			}
		} catch (Exception ex) {
			logger.error("Exception Occurs (Please see logs for more details:" + ex.getMessage());
		}
		finally{	
			/*if any exception come, then make all block available which are not written yet*/
			if(isRunning && processedBlocks.size() > 0){
				this.workerService.updateBlockStatus(processedBlocks, BlockStatus.AVAILABLE);
				this.workerService.updateExectorStatus(ExecutorStatus.STOP);				
			}
		}
		logger.info("Worker process finished: Worker ID:"+this.executorID);
	}
}
