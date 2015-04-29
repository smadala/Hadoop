import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class HDFSClient {

	static private String nameNodeIp;
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int op = 0;
		boolean run = true;
		String src = null, dest = null;
		Scanner in = new Scanner(System.in);
		//System.out.println(" Enter NameNode ip ");
		//nameNodeIp=in.next();*/
		nameNodeIp=args[0];
		nameNode=(INameNode) getNode(nameNodeIp, true);
		while (run) {
			System.out
					.println("1. List Files  2. Write to File 3.Read File 4. Exit \n Enter ur option: ");
			op = in.nextInt();
			switch (op) {

			case 1:
				List<String> files=listFiles(nameNode);
				System.out.println( " Num of Files : " + files.size());
				for(String s:files){
					System.out.println(s);
				}
				break;
			case 3:
				System.out.println("Enter source and destination file");
				src = in.next();
				dest = in.next();
				readFile(src, dest, nameNode);
				break;
			case 2:
				System.out.println(" Enter source and destination file ");
				src = in.next();
				dest = in.next();
				writefile(src, dest, nameNode);
				break;
			default:
				run = false;
			}

		}
	}

	private static INameNode nameNode=null;
	/*public enum NodeType{
		NN, DN, JT, TT
	}*/
	
	private static String getDataAsString(String fileName ) throws Exception{
		StringBuilder data=new StringBuilder();
		BufferedReader reader=new BufferedReader(new FileReader(fileName));
		String line=null;
		while( (line=reader.readLine()) != null){
			data.append(line).append('\n');
		}
		return data.toString();
	}
	public static Object getNode(String ip, boolean isNameNode) {

		Object obj = null;

		try {
			Registry registry = LocateRegistry.getRegistry(ip);
			if (isNameNode)
				obj = registry.lookup("NN");
			else
				obj = registry.lookup("DN");
		} catch (RemoteException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return obj;
	}

	public static List<String> listFiles(INameNode nameNode) {

		HDFS.ListFilesRequest.Builder requestBuilder=HDFS.ListFilesRequest.newBuilder();
		HDFS.ListFilesRequest request=requestBuilder.build();
		List<String> allFiles=new ArrayList<String>();
	//	INameNode nameNode = (INameNode)getNode(nameNodeIp, true);
		try {
			byte[] res=nameNode.list(request.toByteArray());
			HDFS.ListFilesResponse.Builder responseBuilder=HDFS.ListFilesResponse.newBuilder();
			responseBuilder=responseBuilder.mergeFrom(res);
			HDFS.ListFilesResponse response=responseBuilder.build();
			System.out.println( " Num of Files : " + response.getFileNamesCount());
			for(String fileName:response.getFileNamesList()){
				allFiles.add(fileName);
			}
			
		} catch (RemoteException | InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return allFiles;
	}

	static Random random=new Random();
	 
	public static void readFile(String source, String dest, INameNode nameNode ) throws Exception {
		
		    HDFS.OpenFileResponse response=getOpenRequest(source, true, nameNode);
		    if( response.getStatus() != Constants.STATUS_SUCCESS){
		    	System.out.println("Failed in opening file :" +source);
		    	return;
		    }
		    System.out.println("Opened File :"+source);
		    OutputStream os=new FileOutputStream(new File(dest), false);
		    for(int blockNum:response.getBlockNumsList()){
		        HDFS.BlockLocationResponse locationResponse=getBlockLocation(blockNum);
		        
		        
		        HDFS.BlockLocations blockLocation=locationResponse.getBlockLocations(0);
		        while(blockLocation.getLocationsCount() <= 0){
		        	System.out.println( "Location response: \n"+ locationResponse);
		         	 locationResponse=getBlockLocation(blockNum);
			         blockLocation=locationResponse.getBlockLocations(0);
		        }
		        int dnIdx = random.nextInt(blockLocation.getLocationsCount());
		        
		        HDFS.ReadBlockResponse readResponse=getReadBlock(blockNum, blockLocation.getLocations(0));
		        if( readResponse.getStatus() != Constants.STATUS_SUCCESS){
		        	System.out.println(" Fail to read block :" +blockNum);
		        	return;
		        }
		        System.out.println("Read block  "+ blockNum);
		        for(ByteString s:readResponse.getDataList()){
		        	os.write(s.toByteArray());
		        }
		    }
		    os.close();
		    closeFile(response.getHandle(), nameNode);
		    System.out.println("Completed Reading of File : "+ source +" to "+ dest);

	}
	
	private static HDFS.BlockLocationResponse getBlockLocation(int blockNum) throws Exception{
		HDFS.BlockLocationRequest request=HDFS.BlockLocationRequest.newBuilder().addBlockNums(blockNum).build();
		byte[] b=nameNode.getBlockLocations(request.toByteArray());
		HDFS.BlockLocationResponse response=HDFS.BlockLocationResponse.newBuilder().mergeFrom(b).build();
		return response;
	}
	
	private static HDFS.ReadBlockResponse getReadBlock(int blockNum, HDFS.DataNodeLocation dataNodeLocation) throws InvalidProtocolBufferException, RemoteException{
		HDFS.ReadBlockRequest request=HDFS.ReadBlockRequest.newBuilder().setBlockNumber(blockNum).build();
		IDataNode dataNode=(IDataNode) getNode(dataNodeLocation.getIp(), false);
	    return HDFS.ReadBlockResponse.newBuilder().mergeFrom(dataNode.readBlock(request.toByteArray())).build();
	}
	
	private static HDFS.OpenFileResponse getOpenRequest(String name, boolean forRead, INameNode nameNode) throws RemoteException, InvalidProtocolBufferException{
		HDFS.OpenFileRequest.Builder requestBuilder=HDFS.OpenFileRequest.newBuilder();
		requestBuilder.setFileName(name);
		requestBuilder.setForRead(forRead);
		HDFS.OpenFileRequest request=requestBuilder.build();
		HDFS.OpenFileResponse.Builder responseBuilder=HDFS.OpenFileResponse.newBuilder();
		byte[] res=nameNode.openFile(request.toByteArray());
		responseBuilder=responseBuilder.mergeFrom(res);
		return responseBuilder.build();
	}
	
	private static void closeFile(int handler, INameNode nameNode) throws RemoteException, InvalidProtocolBufferException{
		HDFS.CloseFileRequest request=HDFS.CloseFileRequest.newBuilder().setHandle(handler).build();
		byte[] res=nameNode.closeFile(request.toByteArray());
		HDFS.CloseFileResponse response=HDFS.CloseFileResponse.parseFrom(res);
		if(response.getStatus() == Constants.STATUS_SUCCESS)
			System.out.println(" Closed file, hadler :" + handler);
		else
			System.out.println(" fail to Close file, hadler :" + handler);
	}

	public static void writefile(String fromFile, String fileName, INameNode nameNode) throws Exception {

		 HDFS.OpenFileResponse response=getOpenRequest(fileName, false,nameNode);
		 if(response.getStatus() != Constants.STATUS_SUCCESS){
			 System.out.println( " Unable to open file: "+ fileName);
			 return ;
		 }
		 
		byte[] dataBytes=new byte[Constants.BLOCK_SIZE];
		
		FileInputStream is=new FileInputStream(fromFile);
		long totalSize=is.getChannel().size();
		long writtenBytes=0;
		while( writtenBytes < totalSize ){
			//System.out.println(new String(chunk));
			HDFS.AssignBlockResponse assignResponse=getAssignBlock(response.getHandle(), nameNode);
			HDFS.BlockLocations blockLocation=assignResponse.getNewBlock();
			HDFS.DataNodeLocation dataLocation=blockLocation.getLocations(0);
			List<HDFS.DataNodeLocation> dataNodeLocations=new ArrayList<HDFS.DataNodeLocation>(blockLocation.getLocationsList());
			dataNodeLocations.remove(0);
 			blockLocation=HDFS.BlockLocations.newBuilder().addAllLocations(dataNodeLocations).setBlockNumber(blockLocation.getBlockNumber()).build();
 			long numOfBytesToWrite = totalSize < writtenBytes+Constants.BLOCK_SIZE?totalSize-writtenBytes:Constants.BLOCK_SIZE;
 			System.out.println(" totalSize: "+ totalSize +" writtenBytes: "+writtenBytes+" numOfBytesToWrite: "+numOfBytesToWrite);
 			long numOfBytesRead=is.read(dataBytes, 0, (int)numOfBytesToWrite);
 			
			HDFS.WriteBlockResponse writeResponse=writeBlock(dataLocation, ByteString.copyFrom(dataBytes, 0, (int)numOfBytesToWrite).toByteArray(), blockLocation);
			if(writeResponse.getStatus() != Constants.STATUS_SUCCESS){
				System.out.println( " Failed to write to :"+ fileName);
			}
			writtenBytes+=Constants.BLOCK_SIZE;
		}
		closeFile(response.getHandle(), nameNode);
	}
	private static HDFS.WriteBlockResponse writeBlock(HDFS.DataNodeLocation dataLocation, 
			byte[] data, HDFS.BlockLocations blockLocations) throws InvalidProtocolBufferException, RemoteException{
		HDFS.WriteBlockRequest writeReq=HDFS.WriteBlockRequest.newBuilder().
				addData(ByteString.copyFrom(data)).setBlockInfo(blockLocations).build();
		IDataNode dataNode=(IDataNode) getNode(dataLocation.getIp(), false);
		return HDFS.WriteBlockResponse.newBuilder().mergeFrom(dataNode.writeBlock(writeReq.toByteArray())).build();
	}
	private static HDFS.AssignBlockResponse getAssignBlock(int handler, INameNode nameNode) throws Exception{
		HDFS.AssignBlockRequest request=HDFS.AssignBlockRequest.newBuilder().setHandle(handler).build();
		byte[] res = nameNode.assignBlock(request.toByteArray());
		return HDFS.AssignBlockResponse.newBuilder().mergeFrom(res).build();
	}

	
	
}
