import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;


public class TaskTracker {

	ExecutorService mapServices, reduceServices;
	private static final int MAX_MAP_WORKER=3, MAX_REDUCE_WORKER=1;
	AtomicInteger numOfMapsRunning, numOfReduceRunning;
	private static INameNode nameNode;
	private static IJobTracker jobTracker;
	int id;
	private  Queue<JobTracker.Task> completedMapTasks;
	private Queue<JobTracker.Task> completedReduceTasks;
	public TaskTracker(){
		mapServices=Executors.newFixedThreadPool(MAX_MAP_WORKER);
		reduceServices=Executors.newFixedThreadPool(MAX_REDUCE_WORKER);
		numOfMapsRunning=new AtomicInteger(0);
		numOfReduceRunning=new AtomicInteger(0);
		Properties prop=new Properties();
		try {
			prop.load(new FileInputStream(Constants.NAME_NODE_CONFIG_FILE_NAME));
			
			nameNode=(INameNode) HDFSClient.getNode(prop.getProperty(Constants.NN_IP), true);
			jobTracker=(IJobTracker) JobClient.getJobTrakcer(prop.getProperty(Constants.JT_IP));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		id=Constants.taskTaskerIdCounter.getAndIncrement();
		completedMapTasks=new ConcurrentLinkedQueue<JobTracker.Task>();
		completedReduceTasks=new ConcurrentLinkedQueue<JobTracker.Task>();
	}
	
	private void sendHeartBeat(){
		MapReduce.HeartBeatRequest.Builder reqBuilder=MapReduce.HeartBeatRequest.newBuilder().
				setTaskTrackerId(id).setNumMapSlotsFree(MAX_MAP_WORKER-numOfMapsRunning.get())
				.setNumReduceSlotsFree(MAX_REDUCE_WORKER-numOfReduceRunning.get());
		
		
		System.out.println(" ####### building mapStastus tasks ####### ");
		while(!completedMapTasks.isEmpty()){
			JobTracker.Task task=completedMapTasks.poll();
			if(task == null ) continue;
			MapReduce.MapTaskStatus mapTaskStatus=MapReduce.MapTaskStatus.newBuilder().
					setJobId(task.jobId).setTaskCompleted(true).setTaskId(task.id).
					setMapOutputFile("job_"+ task.jobId+"_map_"+task.id).build();
			reqBuilder.addMapStatus(mapTaskStatus);
			
		}
		System.out.println(" ####### building reduceStastus tasks ####### ");
		while(!completedReduceTasks.isEmpty()){
			JobTracker.Task task=completedReduceTasks.poll();
			if(task == null ) continue;
			MapReduce.ReduceTaskStatus reduceTaskStatus=MapReduce.ReduceTaskStatus.newBuilder().
					setJobId(task.jobId).setTaskCompleted(true).setTaskId(task.id).build();
			reqBuilder.addReduceStatus(reduceTaskStatus);
		}
		
		System.out.println(reqBuilder.build());
		try {
			System.out.println(" ####### getting mapinfo ####### ");
			MapReduce.HeartBeatResponse res=MapReduce.HeartBeatResponse.
					parseFrom(jobTracker.heartBeat(reqBuilder.build().toByteArray()));
			System.out.println(res);
			for( MapReduce.MapTaskInfo mapTaskInfo: res.getMapTasksList()){
				JobTracker.Task task=new JobTracker.Task(mapTaskInfo.getTaskId(),
						mapTaskInfo.getJobId(), id);
				mapServices.execute(new MapExc(mapTaskInfo.getMapName(), task, mapTaskInfo.getInputBlocksList()));
				numOfMapsRunning.getAndIncrement();
			}
			System.out.println(" ####### getting reduceinfo ####### ");
			for(MapReduce.ReducerTaskInfo reducerTaskInfo: res.getReduceTasksList() ){
				System.out.println("######### Reducer Info ##########");
				System.out.println(reducerTaskInfo);
				JobTracker.Task task=new JobTracker.Task(reducerTaskInfo.getTaskId(), reducerTaskInfo.getJobId(), id);
				reduceServices.execute(new ReduceExc(reducerTaskInfo.getReducerName(),
						task, reducerTaskInfo.getMapOutputFilesList(), reducerTaskInfo.getOutputFile()));
				numOfReduceRunning.getAndIncrement();
			}
		} catch (InvalidProtocolBufferException | RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		
	}
	
	public static void main(String args[]) throws InterruptedException{
		
		TaskTracker taskTracker=new TaskTracker();
		
		Thread thread=new Thread(new HeartBeartSender(taskTracker));
		
		thread.start();
		
		thread.join();
		
		
	}
	
	public static class HeartBeartSender implements Runnable{

		private TaskTracker taskTracker;
		public HeartBeartSender( TaskTracker taskTracker) {
			this.taskTracker=taskTracker;
			
		}
		@Override
		public void run() {
			
			while(true){
				System.out.println("######Sending Heart beat#########");
				taskTracker.sendHeartBeat();
				
				try {
					Thread.sleep(Constants.TT_HB_INTERVAL);
					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public class MapExc implements Runnable{

		private String mapName;
		private JobTracker.Task task;
		private List<MapReduce.BlockLocations> blockLocs;
		IMapper mapper;
		public MapExc(String mapName, JobTracker.Task task, List<MapReduce.BlockLocations> blockLocs){
			this.mapName=mapName;
			this.task=task;
			this.blockLocs=blockLocs;
			
			try {
				Class class1=Class.forName(mapName);
				mapper=(IMapper) class1.newInstance();
			} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		@Override
		public void run() {

			
			String lines[] = readBlock(blockLocs).split("\\n");
			StringBuilder resBlocks=new StringBuilder();
			String resLine=null;
			FileOutputStream os=null;
			File mapOutputFile=new File("job_"+ task.jobId+"_map_"+task.id);
			try {
				os=new FileOutputStream(mapOutputFile);
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			int buffLinesCount=0;
			for(String line:lines){
				
				buffLinesCount++;
				resLine=mapper.map(line);
				/*System.out.println("Line no: "+ buffLinesCount+" Input line: "+line);
				
				System.out.println("Line no: "+ buffLinesCount+ "Out line: "+resLine);*/
				if( resLine == null ) continue;
				
				/*System.out.println("Line no: "+ buffLinesCount+" Input line: "+line);
				
				System.out.println("Line no: "+ buffLinesCount+ "Out line: "+resLine);
				*/resBlocks.append(resLine).append('\n');
				
				if( buffLinesCount % 1000 == 0){
					System.out.println("Line no: "+ buffLinesCount+" Input line: "+line);
					
					System.out.println("Line no: "+ buffLinesCount+ "Out line: "+resLine);
					writeToFile(os, resBlocks.toString());
					resBlocks=new StringBuilder();
				}
			}
			
			if(resBlocks.length() != 0){
				writeToFile(os, resBlocks.toString());
			}
			try {
				os.close();
				HDFSClient.writefile(mapOutputFile.getAbsolutePath(), "job_"+ task.jobId+"_map_"+task.id, nameNode);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				
			}
			completedMapTasks.add(task);
			numOfMapsRunning.getAndDecrement();
		}
		
	}
	
	private void writeToFile(FileOutputStream os, String data){
		try {
			os.write(data.getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public  class ReduceExc implements Runnable{

		String reduceName, outputFile;
		List<String> mapOutFiles;
		JobTracker.Task task;
		IReducer reducer;
		public ReduceExc(String reduceName, JobTracker.Task task, List<String> mapOutFiles, String outputFile) {
			this.task=task;
			this.mapOutFiles=mapOutFiles;
			this.reduceName=reduceName;
			this.outputFile=outputFile;
			try {
				Class class1=Class.forName(reduceName);
				reducer=(IReducer) class1.newInstance();
			} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		@Override
		public void run() {
			String data=readFiles(mapOutFiles);
			
			String lines[]=data.split("\\n");
			StringBuffer resBlocks=new StringBuffer();
			String reduceOutput=null;
			FileOutputStream os=null;
			File reduceOutputFile=new File(outputFile+"_"+ task.jobId+"_"+task.id);
			try {
				os=new FileOutputStream(reduceOutputFile);
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			int buffLinesCount=0;
			for(String line:lines){
				//System.out.println("Input line: "+line);
				reduceOutput=reducer.reduce(line);
				//System.out.println("Out line: "+reduceOutput);
				if(reduceOutput == null) continue;
				
				resBlocks.append(reduceOutput).append('\n');
				
				buffLinesCount++;
				if( buffLinesCount % 1000 == 0){
					System.out.println("Line no: "+ buffLinesCount+" Input line: "+line);
					
					System.out.println("Line no: "+ buffLinesCount+ "Out line: "+reduceOutput);
					writeToFile(os, resBlocks.toString());
					resBlocks=new StringBuffer();
				}
			}
			if(resBlocks.length() != 0){
				writeToFile(os, resBlocks.toString());
			}
			try {
				os.close();
				HDFSClient.writefile(reduceOutputFile.getAbsolutePath(), outputFile+"_"+ task.jobId+"_"+task.id, nameNode);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			completedReduceTasks.add(task);
			numOfReduceRunning.getAndDecrement();
		}
		
	}
	private static String readFiles(List<String> mapOutFiles){
		
		StringBuffer res=new StringBuffer();
		for(String fileName:mapOutFiles){
			
			HDFS.OpenFileRequest req=HDFS.OpenFileRequest.newBuilder().
					setFileName(fileName).setForRead(true).build();
			try {
				HDFS.OpenFileResponse openFileResponse=HDFS.OpenFileResponse.parseFrom(nameNode.openFile(req.toByteArray()));
				
				HDFS.BlockLocationRequest blockLocationRequest=HDFS.BlockLocationRequest.
						newBuilder().addAllBlockNums(openFileResponse.getBlockNumsList()).build();
				HDFS.BlockLocationResponse blockLocationResponse=HDFS.BlockLocationResponse.
						parseFrom(nameNode.getBlockLocations(blockLocationRequest.toByteArray()));
				String lines =readBlock( JobTracker.JobDetails.getLocations(blockLocationResponse.getBlockLocationsList()));
				res.append(lines);
				
			} catch (InvalidProtocolBufferException | RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return res.toString();
		
	}
	
	public static String readBlock(List<MapReduce.BlockLocations> blockLocations){
		StringBuilder blockData=new StringBuilder();
		for(MapReduce.BlockLocations blockLoc:blockLocations ){
			
			for(MapReduce.DataNodeLocation dataNodeLocation: blockLoc.getLocationsList()){
				
				IDataNode dataNode=(IDataNode) HDFSClient.getNode(dataNodeLocation.getIp(), false);
				
				HDFS.ReadBlockRequest readBlockRequest=HDFS.ReadBlockRequest.newBuilder().setBlockNumber(blockLoc.getBlockNumber()).build();
				
				HDFS.ReadBlockResponse res;
				try {
					res = HDFS.ReadBlockResponse.parseFrom(dataNode.readBlock(readBlockRequest.toByteArray()));
					if(res.getStatus() == Constants.STATUS_FAIL) continue;
					
					List<ByteString> dataBytes = res.getDataList();
					
					
					
					for(ByteString dataByte: dataBytes){
						
						blockData.append(new String( dataByte.toByteArray()));
						
					}
				} catch (InvalidProtocolBufferException | RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
			}
			
		}
		return blockData.toString();
	}
}
