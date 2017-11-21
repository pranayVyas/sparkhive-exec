package org.hortonworks.com.spark_hiveexec;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.log4j.*;
import org.hortonworks.com.spark_hiveexec.sparkconnect;

/**
 * Run hivesql on spark. use spark-submit to run this jar
 *
 */
public class App {
	final static Logger logger = Logger.getLogger(App.class);

	static Path hqlpath = null;
	static String AppName = "JavaSparkSQL";
	static Boolean runspark = true;
	static int lengthofarg = 0;
	static Boolean inp_type_hql = true;

	public static void main(String[] args) throws Exception {

		validateargs(args);
		
		if (runspark) {

			//create connection
			
			sparkconnect.spark_connect(AppName);
			
			if (inp_type_hql) {
				String content = new String(Files.readAllBytes(hqlpath));
				sparkconnect.run_query(content);
			}
			
			if (inp_type_hql = false) {
				List<String> lines = Files.readAllLines(hqlpath);
				for(String line : lines){
				String content = new String(Files.readAllBytes(Paths.get(line)));
				sparkconnect.run_query(content);
				}

			}
		}
	}

	private static void validateargs(String[] args) {
		// TODO Auto-generated method stub
		
		runspark = false;
		lengthofarg = args.length;
		
		switch (lengthofarg) {
		case 0:
			print_message(args);
			break;
		case 1:
		case 2:	
		case 3:
			if (args[0].equalsIgnoreCase("?")  || args[0].equalsIgnoreCase("help")) {
				print_message(args);
			} else if (args[0].equalsIgnoreCase("s")) {
				inp_type_hql = false;
				checkfile(args);
			} else if (args[0].toLowerCase().equals("h")) {
				inp_type_hql = true;
				checkfile(args);			
			}
			break;
		default:
			print_message(args);
			break;
		}
	}

	private static void checkfile(String[] args) {
		// TODO Auto-generated method stub
		hqlpath = Paths.get(args[1]);
		runspark = true;
		if (lengthofarg > 2) {
			AppName = args[2];
		}
		if (Files.notExists(hqlpath)) {
			logger.info("hql file not found " + hqlpath);
			runspark = false;
		}
	}

	private static void print_message(String[] args) {
		logger.debug("Invalid Input argument provided " + args);
		logger.info("\nTo run this program use spark-submit with path to hive hql file as input parameter");
		logger.info("\nArgument 1 - options ?/help to print usage");
		logger.info("\n \t ? or help to print usage");
		logger.info("\n \t f Input is a hql file");
		logger.info("\n \t f Input is a sequence file with path to hql files");
		logger.info("\nArgument 2 - path to hql/sequence file");
		logger.info("\nArgument 3 Optional - ApplicationName");
		runspark = false;
	}
}
