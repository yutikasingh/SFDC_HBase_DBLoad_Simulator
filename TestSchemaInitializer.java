

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;


public class TestSchemaInitializer {

	public static final String DEFAULT_TABLE_NAME = "usertable";
	public static final String DEFAULT_NUMBER_OF_REGIONS = "16";
	public static final String DEFAULT_ORGANIZATIONS_COUNT = "1000";
	public static final String DEFAULT_HBASE_PORT = "2181";
	public static final String DEFAULT_MAX_HTABLE_POOL_SIZE ="10";
	public static final String DEFAULT_COLUMN_FAMILY = "BDA-CF1"; // HistoryKey=E:T:Timestamp(millis)
	public static final Properties properties = new Properties();
	public static String tableName;
	public static int numberOfRegions;
	public static int organizationCount;
	private static String[] HBASE_HOST_ARRAY;
	public static String HBASE_PORT;
	public static String columnFamily;
	protected static ArrayList<Configuration> confArray = new ArrayList<Configuration>();

	/**
	 * main function
	 * 
	 * @param args, the properties file {splitTest}
	 * @throws IOException
	 */

	public static void main(String[] args) throws IOException {

		if (args.length == 0) {
			System.out.println("Pass the properties file, which has all the settings");
			System.exit(1);
		}

		// load the properties file
		loadProperties(args);

		// Reading the Properties
		readProperties();
		
		// Initialize Hbase connection manager
		initHbaseConnectionPool();

		
		// Create table split regions and history
		byte[][] splitKeys = getRegionSplits(organizationCount, numberOfRegions);
		createSplitHistoryTable(splitKeys);
		
	}
	
	//load the properties file

	private static void loadProperties(String[] args) {
		try {
			File file = new File(args[0]);
			System.out.println(args[0]);
			properties.load(new FileInputStream(file));
		} catch (Exception e) {
			System.out.println("Would be considering default values, "
					+ "as there was an exception reading the file : ["
					+ e.getMessage() + "]");
		}
	}
	
	// reading the properties file
	
	private static void readProperties(){
		tableName = properties.getProperty("TABLE_NAME",DEFAULT_TABLE_NAME);
		numberOfRegions = new Integer(properties.getProperty("NUMBER_OF_REGIONS",DEFAULT_NUMBER_OF_REGIONS));
		organizationCount = new Integer(properties.getProperty("ORGANIZATIONS_COUNT",DEFAULT_ORGANIZATIONS_COUNT));
		System.out.println(properties.getProperty("MULTI_HBASE_HOST"));
		HBASE_HOST_ARRAY = ((String) properties.getProperty("MULTI_HBASE_HOST")).split(",");
		HBASE_PORT = (String) properties.getProperty("HBASE_PORT",DEFAULT_HBASE_PORT);
		columnFamily = properties.getProperty("COLUMN_FAMILY",DEFAULT_COLUMN_FAMILY);
	}
	
	// splitting the table 

	private static byte[][] getRegionSplits(int orgCount, int splitCount) {
		byte[][] regionSplits = new byte[splitCount][];
		String orgIdPrefix = "ORG";
		int intervalSize = orgCount / splitCount;
		for (int i = 0; i < splitCount; i++) {
			String orgId = orgIdPrefix
					+ String.format("%09d", i * intervalSize);
			System.out.println(orgId);
			regionSplits[i] = orgId.getBytes();
		}
		return regionSplits;
	}
	
	// creating the split history

	public static void createSplitHistoryTable(byte[][] regionsplits)
			throws IOException {
		for (int h = 0; h < HBASE_HOST_ARRAY.length; h++) {
			HBaseAdmin admin = getAdminInterface(h);
			if (admin.tableExists(tableName)) {
				System.out.println("table already exists!");
			} else {
				HTableDescriptor tableDesc = new HTableDescriptor(tableName);
				tableDesc.addFamily(new HColumnDescriptor(columnFamily));

				long beforetime = System.currentTimeMillis();
				admin.createTable(tableDesc, regionsplits);
				long dbOptime = System.currentTimeMillis() - beforetime;

				System.out.println("HBase DB Operation : Action="
						+ "createSplitTable" + " StartTime=" + beforetime
						+ " DBOperationTime=" + dbOptime);
				System.out.println("create region split table " + tableName
						+ " ok.");
			}
		}
	}

	/**
	 * Initialize Hbase connection configuration
	 * 
	 * @param hbaseHost
	 * @param hbasePort
	 * @return
	 */
	protected static Configuration getHbaseConfig(String hbaseHost,
			String hbasePort) {

		// Initialize configuration for normal Htable pools
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", hbaseHost);
		conf.set("hbase.zookeeper.property.clientPort", hbasePort);

		// Config settings to minimize retries to limit the time
		// for failure scenarios
		conf.set("hbase.client.retries.number", "1");
		conf.set("hbase.client.rpc.maxattempts", "1");
		conf.set("zookeeper.recovery.retry", "1");

		// Few other configs to experiment with
		// zookeeper.session.timeout, hbase.rpc.timeout,
		// zookeeper.recovery.retry.intervalmill, hbase.client.pause
		return conf;
	}

	/**
	 * Get HBaseAdmin instance
	 * 
	 * @param hbaseInstanceIndex
	 * @return
	 */
	public static HBaseAdmin getAdminInterface(int hbaseInstanceIndex) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(confArray.get(hbaseInstanceIndex));
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
		return admin;
	}

		

	/**
	 * init HBase instance pools one connection per hbase instance
	 */
	public static void initHbaseConnectionPool() {
		// Get the HBase instances
		String[] hbaseHostArray = HBASE_HOST_ARRAY ;

		for (int i = 0; i < hbaseHostArray.length; i++) {
			// Init configuration
			Configuration conf = getHbaseConfig(hbaseHostArray[i],
					HBASE_PORT);
			confArray.add(conf);

			// Init Hconnection
			try {
				HConnectionManager.createConnection(conf);
			} catch (ZooKeeperConnectionException e) {
				e.printStackTrace();
			}
		}
	}
	
}
