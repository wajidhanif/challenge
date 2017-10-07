package the.floow.challenge.service;

import java.io.IOException;
import java.util.List;

import org.bson.types.ObjectId;

import the.floow.challenge.entity.Executor;
import the.floow.challenge.entity.InputParameter;
import the.floow.challenge.entity.QueueMessage;
import the.floow.challenge.enums.BlockStatus;
import the.floow.challenge.enums.ExecutorStatus;
import the.floow.challenge.enums.FileStatus;
import the.floow.challenge.utils.FileUtil;

public class WorkerControllerService extends GenericService {

	public long serverDefaultWait;
	
	public WorkerControllerService(InputParameter inputParams, ObjectId executorID, ObjectId fileID) {
		super(inputParams, executorID, fileID);
		this.serverDefaultWait = Long.parseLong(this.SettingsDao.getSetting("SERVER_MAX_WAIT_TIME"));
	}

	public QueueMessage getQueueMessage(Integer blockNo) throws IOException {
		QueueMessage message = new QueueMessage(blockNo);
		message.data = FileUtil.readFileByBlock(blockNo, this.inputParams.filePath, this.blockSize);
		return message;
	}

	public long getMaxQueueSize() {
		return this.DefaultQueueSizeMultipler * this.getExecutorCount();
	}

	public void updateFileStaus(FileStatus status) {
		this.fileDao.updateFileStaus(this.fileID, status);
	}

	public void checkWorkerFailOver() {
		List<Executor> executors = this.executorDao.getAllExecutor();
		for (Executor executor : executors) {
			long now = System.currentTimeMillis();
			long lastRunTime = executor.runningTimestamp.getTime();
			long diff = now - lastRunTime;
			if (diff > this.workerMaxWaitTime) {
				this.blockDao.updateBlockStatus(this.fileID, this.executorID, BlockStatus.AVAILABLE);
				this.executorDao.updateExectorStatus(this.executorID,ExecutorStatus.STOP);
			}
		}
	}
	public void updateExectorServerInfo(){		
		this.executorDao.updateExectorServerInfo(this.executorID);
	}
	public void mapReduceWords(){
		this.wordsDao.mapReduce(this.fileID);
	}

	public boolean isServerRunning() {
		Executor executor = this.executorDao.getExecutorAsServer();
		boolean running = false;
		if(executor!=null){
			long now = System.currentTimeMillis();
			long lastRunTime = executor.runningTimestamp.getTime();
			long diff = now - lastRunTime;
			running = true;
			if (diff > this.serverDefaultWait) {
				this.executorDao.updateExectorServerInfo(this.executorID);
				running = false;
			}
			
		}
		return running;
	}
	
	public boolean isExecutorAsServer(){
		Executor executor = this.executorDao.getExecutorAsServer();
		ObjectId exeID = null;
		if(executor== null){
			exeID  = this.executorDao.updateExecutorAsServer();
		}
		return this.executorID == exeID ? true : false;
	}
	
	public boolean isFileProcessed(){
		return this.fileDao.isFileProcessed(this.fileID);
	}

}
