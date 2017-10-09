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
/**
This is the worker runnable class which processes 
the file-blocks and counts words
@author Wajid */
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
	/** 
	This function merges the words count processed by CountProcessor into worker's wordCounts HashMap  
	*/
	public void merge(ConcurrentHashMap<String, LongAdder> newCounts) throws IOException {

		newCounts.forEach((key,value) -> {
					if (!wordCounts.containsKey(key))
						wordCounts.put(key, value.longValue());
					else
						wordCounts.put(key, Long.valueOf(wordCounts.get(key).longValue() + value.longValue()));
				});
	}
	/** 
		This function is processing file-blocks and counting the words
			
		Functionalities includes:		
		1.Gets file-block data from message queue
		2.Calls the assigned CountProcessor (by default word count processor) to count words
		3.Merges the data with worker hashmap
		4.Writes the word counts to database, if all blocks processed or memory is full 
		5.Waits for the controller-executor, if all blocks are not processed yet
	*/
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
			logger.error(ex);	
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
