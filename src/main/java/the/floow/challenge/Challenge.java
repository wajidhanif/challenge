package the.floow.challenge;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.Arrays;

import org.apache.log4j.Logger;

import the.floow.challenge.Executors.CountExecutor;
import the.floow.challenge.entity.DataSource;
import the.floow.challenge.entity.InputParameter;
import the.floow.challenge.service.ChallengeService;
import the.floow.challenge.utils.Util;

public class Challenge {
	final static Logger logger = Logger.getLogger(Challenge.class);

	public static void main(String[] args) throws UnknownHostException {
		try {
			String source = "";
			DataSource ds = null;
			/*if mongo parameter is missing then there is no need to run the program*/
			if (args.length == 0 || !Arrays.asList(args).contains("-mongo")) {
				throw new IllegalArgumentException("Please provide the required parameter(-mongo)");
			}
			/*parsing parameter for source and mongo db */
			for (int i = 0; i < args.length; i++) {
				if (args[i].equals("-mongo")) {
					String dbUrl = args[i + 1];
					if (!dbUrl.contains(":")) {
						throw new IllegalArgumentException("Please provide db params like IP:PORT");
					}
					String[] dbparams = dbUrl.split(":");
					String ip = dbparams[0];
					int port = Util.parseInt(dbparams[1], 27017);
					ds = new DataSource(ip, port);
				}
				if (args[i].equals("-source")) {
					source = args[i + 1];
				}
			}
			
			/*preparing the input parameter object for executor usage*/
			InputParameter inParams = new InputParameter(source, ds);
			
			// set the settings collection in db
			new Challenge().init(inParams);

			/*By default: CountExecutor behaves as word count executor (the.floow.challenge.processor.WordCountProcessor).*/  
			CountExecutor wcExecutor = new CountExecutor(inParams);
			wcExecutor.execute();

		} catch (IllegalArgumentException ex) {
			logger.error(ex.getMessage());
		} catch (Exception exp) {
			logger.error("Exception Occurs:" + exp.getMessage());
			logger.error(exp);
		}
	}

	private void init(InputParameter inParam) throws IOException {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

		BufferedReader br = null;
		try {
			InputStream is = classLoader.getResourceAsStream("settings.properties");
			br = new BufferedReader(new InputStreamReader(is));
			for (String line = br.readLine(); line != null; line = br.readLine()) {
				String[] arr = line.split("=");
				ChallengeService service = new ChallengeService(inParam);
				service.addSetting(arr[0], arr[1]);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			if (br != null) {
				br.close();
			}
		}
	}
}
