package the.floow.challenge.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class Util {

	public static String getHostName() throws UnknownHostException {
		return InetAddress.getLocalHost().getHostAddress();
	}

	public static String getFileName(String filePath) {
		File f = new File(filePath);
		return f.getName();
	}

	public static List<String> bytesToStringList(byte[] bytes) throws IOException {
		List<String> lines = new ArrayList<String>();
		if (bytes == null) {
			return lines;
		}
		
		BufferedReader r = null;
		try {
			r = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes), "UTF-8"));
			for (String line = r.readLine(); line != null; line = r.readLine()) {
				lines.add(line);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			if (r != null) {
				r.close();
			}
		}
		return lines;
	}

	public static double getMemoryUsePercentage() {
		Runtime runtime = Runtime.getRuntime();
		long memoryInUse = runtime.totalMemory() - runtime.freeMemory();
		long maxMemory = runtime.maxMemory();
		return ((double) memoryInUse / maxMemory) * 100;
	}
	
	 public static Integer parseInt(String str, int defaultValue) {
	        Integer n = defaultValue;

	        try {
	            n = new Integer(Integer.parseInt(str));
	        } catch (NumberFormatException ex) {
	        	//TODO: log exception log4j
	        }

	        return n;
	    }

}
