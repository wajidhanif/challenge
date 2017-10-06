package the.floow.challenge.service;

import java.io.IOException;

import org.bson.types.ObjectId;

import the.floow.challenge.entity.InputParameter;
import the.floow.challenge.entity.QueueMessage;
import the.floow.challenge.enums.FileStatus;
import the.floow.challenge.utils.FileUtil;

public class WorkerControllerService extends GenericService {

	public WorkerControllerService(InputParameter inputParams, ObjectId executorID, ObjectId fileID) {
		super(inputParams, executorID, fileID);
	}

	public QueueMessage getQueueMessage(Integer blockNo) throws IOException {
		QueueMessage message = new QueueMessage(blockNo);
		message.data = FileUtil.readFileByBlock(blockNo, this.inputParams.filePath, this.blockSize);
		return message;
	}

	public long getMaxQueueSize() {
		return this.DefaultQueueSizeMultipler * this.getExecutorCount();
	}

	public boolean isProcessFinished() {
		long notWrittenCount = this.blockDao.getBlockCountNotWritten(this.fileID);
		return notWrittenCount > 0 ? true : false;
	}
	public void updateFileStaus(FileStatus status) {
		this.fileDao.updateFileStaus(this.fileID, status);
	}
}
