import java.util.concurrent.atomic.AtomicInteger;


public class Constants {
	public  static int STATUS_SUCCESS=0;
	public static int STATUS_FAIL=1;
	public static final String NN_PORT="NN_PORT";
	public static final String NN_IP="NN_IP";
	public static final String JT_IP="JT_IP";
	public static final String DN_PORT="DN_PORT";
	public static final String DN_IP="DN_IP";
	public static AtomicInteger portinc= new AtomicInteger(6000);
	public static AtomicInteger idInc=new AtomicInteger(100);
	public static final String NAME_NODE_CONFIG_FILE_NAME="NN_config";
	public static final String JOB_TRACKER_CONFIG_FILE_NAME="JT_config";
	public static AtomicInteger taskTaskerIdCounter=new AtomicInteger(1000);
	
	public static final int TT_HB_INTERVAL=2000;
	static final int BLOCK_SIZE= 1024 * 10* 1024;
	
	public final static int replicationFactor = 2;
}
