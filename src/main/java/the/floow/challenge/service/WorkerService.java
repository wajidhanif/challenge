package the.floow.challenge.service;

import java.util.concurrent.ConcurrentHashMap;

import org.bson.types.ObjectId;

import the.floow.challenge.entity.Executor;
import the.floow.challenge.entity.InputParameter;
import the.floow.challenge.enums.BlockStatus;
import the.floow.challenge.enums.ExecutorStatus;

public class WorkerService extends GenericService {

	public long workDefaultWait;
	
	public WorkerService(InputParameter inputParams, ObjectId executorID, ObjectId fileID) {
		super(inputParams, executorID, fileID);
		this.workDefaultWait = Long.parseLong(this.SettingsDao.getSetting("WORKER_DEFAULT_WAIT"));
	}

	public boolean isBlockProcessed() {
		long availBlockCount = this.getBlockCountByStatus(BlockStatus.AVAILABLE);
		boolean isEmpty = this.getMessageQueue().empty();
		return availBlockCount == 0 && isEmpty ? true : false;
	}

	public void writeCountsToDB(ConcurrentHashMap<String, Long> words) {
		this.wordsDao.create(this.fileID, this.executorID, words);
	}
	public boolean isStopByServer(){
		Executor executor = this.getExecutor();
		if(executor.status.equals(ExecutorStatus.STOP)){
			return true;
		}
		return false;
	}
	
}
