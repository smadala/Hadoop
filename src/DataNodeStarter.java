import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


public class DataNodeStarter {

	private static String myip;
	private static int myport;
	private static int nnPort;
	private static String nnIp;
	private static int id;
	private static void init(String fileName){
		Properties prop=new Properties();
		try {
			prop.load(new FileInputStream(new File(fileName)));
			myip=prop.getProperty(Constants.DN_IP);
			myport=Constants.portinc.getAndIncrement();
			nnPort=Integer.parseInt(prop.getProperty(Constants.NN_PORT));
			nnIp=prop.getProperty(Constants.NN_IP);
			id=myport;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		init(args[0]);
		
		
		
	}

}
