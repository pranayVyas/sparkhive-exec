/**
 * 
 */
package org.hortonworks.com.spark_hiveexec;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * @author pranayashokvyas
 *
 */
public class sparkconnect {

	/**
	 * @return 
	 * 
	 */
	public static SparkConf sparkConf;
	public static JavaSparkContext ctx;
	public static SQLContext sqlContext;
	public static HiveContext hqlContext;
	
	public static void run_query(String content) {
		// TODO Auto-generated constructor stub
		// SQL
		DataFrame results = hqlContext.sql(content);
		results.show(1000);
	}
	
	public static void spark_connect(String appname) throws Exception{
		
		sparkConf = new SparkConf().setAppName(appname);
		ctx = new JavaSparkContext(sparkConf);
		sqlContext = new SQLContext(ctx);
		hqlContext = new org.apache.spark.sql.hive.HiveContext(ctx);
	}
}
