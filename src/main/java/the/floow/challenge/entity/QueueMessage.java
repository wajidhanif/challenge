package the.floow.challenge.entity;

public class QueueMessage {

	public int blockNo;
	public byte [] data;
	
	public QueueMessage(int blockNo, byte[] data) {
		this.blockNo = blockNo;
		this.data = data;
	}

	public QueueMessage(int blockNo) {
		this.blockNo = blockNo;		
	}
	
}
