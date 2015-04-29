import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import Client.Compute;
import Server.ComputeEngine;

import com.google.protobuf.AbstractMessage.Builder;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class DataNode implements IDataNode {

	private String ip;
	private int port=0;
	private int id;
	private Set<Integer> blockNums;
	private static String dataPath;

	public DataNode(String fileName) {
		super();
		blockNums = new HashSet<Integer>();
		Properties prop=new Properties();
		try {
			prop.load(new FileInputStream(new File(fileName)));
			ip=prop.getProperty(Constants.DN_IP);
			//port=Constants.portinc.getAndIncrement();
			//nnPort=Integer.parseInt(prop.getProperty(Constants.NN_PORT));
			id= Integer.parseInt(prop.getProperty("Id"));
			nnIp=prop.getProperty(Constants.NN_IP);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		dataPath = "_"+id+"_";
		
	}

	@Override
	public byte[] readBlock(byte[] data)  throws RemoteException{
		// TODO Auto-generated method stub
		HDFS.ReadBlockRequest.Builder builder = HDFS.ReadBlockRequest
				.newBuilder();
		HDFS.ReadBlockResponse.Builder responseBuilder = HDFS.ReadBlockResponse
				.newBuilder();
		try {
			builder.mergeFrom(data);
			HDFS.ReadBlockRequest request = builder.build();
			int blockNumber = request.getBlockNumber();
			if (blockNums.contains(blockNumber)) {
				Path path = Paths.get(dataPath + blockNumber);
				byte b[] = Files.readAllBytes(path);
			//	System.out.println(" Read:  "+new String(b));
				responseBuilder.setStatus(0);
				ByteString bs = ByteString.copyFrom(b);
				List<ByteString> blist=new ArrayList<ByteString>();
				blist.add(bs);
				responseBuilder.addAllData(blist);
				responseBuilder.setStatus(Constants.STATUS_SUCCESS);
			} else {
				responseBuilder.setStatus(Constants.STATUS_FAIL);
			}

		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return responseBuilder.build().toByteArray();
	}

	@Override
	public byte[] writeBlock(byte[] data) throws RemoteException{
		// TODO Auto-generated method stub
		HDFS.WriteBlockRequest.Builder requestBuilder=HDFS.WriteBlockRequest.newBuilder();
		HDFS.WriteBlockResponse.Builder responseBuilder=HDFS.WriteBlockResponse.newBuilder();
		try {
			requestBuilder.mergeFrom(data);
			HDFS.WriteBlockRequest request=requestBuilder.build();
			HDFS.BlockLocations  blockLocations=request.getBlockInfo();
			int blockNum=blockLocations.getBlockNumber();
			OutputStream os=new FileOutputStream(new File(dataPath+blockNum), false);
			blockNums.add(blockNum);
		//	System.out.println(new String( request.getData(0).toByteArray()).length());
			os.write(request.getData(0).toByteArray());
			os.close();
			int status=1;
			if(blockLocations.getLocationsCount() != 0){
				System.out.println("Next data Node...");
				HDFS.DataNodeLocation dataNodeLocation = blockLocations.getLocations(0);
				List<HDFS.DataNodeLocation> dataNodeLocations=new ArrayList<HDFS.DataNodeLocation>(blockLocations.getLocationsList());
				dataNodeLocations.remove(0);
				blockLocations=HDFS.BlockLocations.newBuilder().addAllLocations(dataNodeLocations).setBlockNumber(blockLocations.getBlockNumber()).build();
				requestBuilder.setBlockInfo(blockLocations);
				return sendWriteReq(dataNodeLocation, requestBuilder.build());
			}
			//responseBuilder.setStatus(status);
		} catch (InvalidProtocolBufferException | FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		responseBuilder.setStatus(Constants.STATUS_SUCCESS);
		return responseBuilder.build().toByteArray();
	}

	private byte[] sendWriteReq(HDFS.DataNodeLocation nodeLocation, HDFS.WriteBlockRequest req) throws RemoteException{
		 
		 IDataNode dataNode =null;
		 
	        try {
	            String ip=nodeLocation.getIp();
	            int port=nodeLocation.getPort();
	            Registry registry = LocateRegistry.getRegistry(ip);
	            dataNode = (IDataNode) registry.lookup("DN");
	        } catch (Exception e) {
	            System.err.println("Exception:");
	            e.printStackTrace();
	        }
	       return dataNode.writeBlock(req.toByteArray());
	}
	
	private static int nnPort;
	private  static String nnIp;
	
	
	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public Set<Integer> getBlockNums() {
		return blockNums;
	}

	public void setBlockNums(Set<Integer> blockNums) {
		this.blockNums = blockNums;
	}

	public static String getDataPath() {
		return dataPath;
	}

	public static void setDataPath(String dataPath) {
		DataNode.dataPath = dataPath;
	}

	public int getNnPort() {
		return nnPort;
	}

	public void setNnPort(int nnPort) {
		this.nnPort = nnPort;
	}

	public String getNnIp() {
		return nnIp;
	}

	public void setNnIp(String nnIp) {
		this.nnIp = nnIp;
	}
   // private static INameNode nameNode;
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//java -classpath "/home/satya/resource/ds/asg/1/protobuf-java-2.6.1.jar:." -Djava.rmi.server.hostname=10.1.131.103 -Djava.security.policy=server.policy NameNode
		//java -classpath "/home/satya/resource/ds/asg/1/protobuf-java-2.6.1.jar:." -Djava.rmi.server.hostname=10.1.131.103 -Djava.security.policy=server.policy DataNode
		//
		IDataNode dataNode=new DataNode("server_config");
		
		/*if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }*/
		try {
			String name="DN";
            IDataNode stub =
                (IDataNode) UnicastRemoteObject.exportObject(dataNode, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind(name, stub);
            System.out.println("DataNode Running ");
        } catch (Exception e) {
            System.err.println("DataNode exception:");
            e.printStackTrace();
        }
		
		String name = "NN";
		INameNode nameNode=null;
         try {
        	 Registry registry = LocateRegistry.getRegistry(nnIp);
			nameNode= (INameNode) registry.lookup(name);
		} catch (RemoteException | NotBoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
      /*  while(true){
        	heartBeat(dataNode.);
        }*/
		Thread heartBeatThread=new Thread(((DataNode)dataNode).new HeartBeat(nameNode));
		Thread blockReportThread=new Thread(((DataNode)dataNode).new BlockReport(nameNode));
		heartBeatThread.start();
		blockReportThread.start();
		try {
			heartBeatThread.join();
			blockReportThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
         
	}
/*	private static void heartBeat(int id){
		try {
            HDFS.HeartBeatRequest.Builder reqBuilder=HDFS.HeartBeatRequest.newBuilder();
            reqBuilder.setId(id);
            
            byte[] resBytes=nameNode.heartBeat(reqBuilder.build().toByteArray());
            System.out.println( "Sent Heart beat.." );
            Thread.sleep(3 * 1000);
        } catch (Exception e) {
            System.err.println("Exception:");
            e.printStackTrace();
        }
	}
	private static void blockReport(int id, String ip, int port, List<Integer> blockNums){
		 try {
	            
		        
	            HDFS.BlockReportRequest.Builder reqBuilder=HDFS.BlockReportRequest.newBuilder();
	            reqBuilder.setId(id);
	            HDFS.DataNodeLocation.Builder location=HDFS.DataNodeLocation.newBuilder();
	            location.setIp(ip);
	            location.setPort(port);
	            List<Integer> bn=new ArrayList<Integer>(blockNums);
	            reqBuilder.addAllBlockNumbers(bn);
	            System.out.println("Sending block report...");
	            byte[] resBytes=nameNode.blockReport(reqBuilder.build().toByteArray());
	            
	            Thread.sleep(2 * 1000);
	        } catch (Exception e) {
	            System.err.println("Exception:");
	            e.printStackTrace();
	        }
	}
	*/
	  class HeartBeat implements Runnable{

		 private INameNode nameNode;
		 
		
		public HeartBeat(INameNode nameNode) {

			this.nameNode = nameNode;
		}


		@Override
		public void run() {
			// TODO Auto-generated method stub
			while(true){
				try {
		            HDFS.HeartBeatRequest.Builder reqBuilder=HDFS.HeartBeatRequest.newBuilder();
		            reqBuilder.setId(id);
		            
		            byte[] resBytes=nameNode.heartBeat(reqBuilder.build().toByteArray());
		            System.out.println( "Sent Heart beat.." );
		            Thread.sleep(30 * 1000);
		        } catch (Exception e) {
		            System.err.println("Exception:");
		            e.printStackTrace();
		        }
		        
			}
		}
		
	}
	
	class BlockReport implements Runnable{

		 private INameNode nameNode;

		 public BlockReport(INameNode nameNode) {
				
				this.nameNode = nameNode;
			}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			while(true){
				 
			        try {
			            
			        
			            HDFS.BlockReportRequest.Builder reqBuilder=HDFS.BlockReportRequest.newBuilder();
			            reqBuilder.setId(id);
			            HDFS.DataNodeLocation.Builder location=HDFS.DataNodeLocation.newBuilder();
			            location.setIp(ip);
			            location.setPort(port);
			            List<Integer> bn=new ArrayList<Integer>(blockNums);
			            reqBuilder.addAllBlockNumbers(bn);
			            reqBuilder.setLocation(location);
			            System.out.println("Sending block report...");
			            byte[] resBytes=nameNode.blockReport(reqBuilder.build().toByteArray());
			            
			            Thread.sleep(2 * 1000);
			        } catch (Exception e) {
			            System.err.println("Exception:");
			            e.printStackTrace();
			        }
			        
				}
		}
		
	}
	
}
