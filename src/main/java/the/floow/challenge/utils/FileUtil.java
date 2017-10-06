package the.floow.challenge.utils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class FileUtil {
	
	public static long getFileSize(String filePath) throws IOException{
		RandomAccessFile file = new RandomAccessFile(filePath, "r");
		long len = file.length();
		file.close();
		return len;
	}
	
	public static byte [] readFileByBlock(int blockNum, String filePath, long blockSize) throws IOException {
		RandomAccessFile file = null;
		FileChannel in = null;
		MappedByteBuffer buffer = null;
		byte [] data = null;
		try {
			file = new RandomAccessFile(filePath, "r");
			in = file.getChannel();

			long pos = blockSize * (blockNum-1);
			buffer = in.map(FileChannel.MapMode.READ_ONLY, pos, blockSize);
			data = new byte[buffer.limit()];
			buffer.get(data);
		} finally {
			if (in != null)
				in.close();
			if (file != null)
				file.close();
			if (buffer != null)
				buffer.clear();
		}
		return data;
	}
}
