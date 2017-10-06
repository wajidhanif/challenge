package the.floow.challenge.entity;

import the.floow.challenge.enums.BlockQueueStatus;

public class BlockQueue {

	public int blockNo;
	public BlockQueueStatus status;
	
	public BlockQueue(int blockNo, String status) {
		this.blockNo = blockNo;
		this.status = BlockQueueStatus.valueOf(status);
	}
	public BlockQueue(int blockNo) {
		this.blockNo = blockNo;
	}
	
}
