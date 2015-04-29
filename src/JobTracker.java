import java.io.FileInputStream;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.InvalidProtocolBufferException;

public class JobTracker implements IJobTracker {

	private AtomicInteger jobIdCounter;
	private AtomicInteger taskIdCounter;

	private Map<Integer, JobDetails> jobs;
	static INameNode nameNode;
	private Queue<JobDetails> mapQueue;
	private Queue<JobDetails> reduceQueue;

	private Map<Integer, Map<Integer, Task> > runningMapTasks; 
	private Map<Integer, Map<Integer, Task> > runningReduceTasks;
	
	public JobTracker() {
		jobIdCounter = new AtomicInteger(0);
		taskIdCounter = new AtomicInteger(0);
		jobs = new ConcurrentHashMap<Integer, JobDetails>();
		Properties prop=new Properties();
		try {
			prop.load(new FileInputStream(Constants.NAME_NODE_CONFIG_FILE_NAME));
			
			nameNode=(INameNode) HDFSClient.getNode(prop.getProperty(Constants.NN_IP), true);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		mapQueue = new ConcurrentLinkedQueue<JobTracker.JobDetails>();
		reduceQueue = new ConcurrentLinkedQueue<JobTracker.JobDetails>();
		runningMapTasks=new ConcurrentHashMap<Integer, Map<Integer, Task>>();
		runningReduceTasks=new ConcurrentHashMap<Integer, Map<Integer, Task>>();
	}

	@Override
	public byte[] jobSubmit(byte[] data) throws RemoteException{
		MapReduce.JobSubmitResponse.Builder responseBuilder = MapReduce.JobSubmitResponse
				.newBuilder();
		try {
			MapReduce.JobSubmitRequest jobSubmitRequest = MapReduce.JobSubmitRequest
					.parseFrom(data);
			System.out.println(jobSubmitRequest);
			int jobId = jobIdCounter.getAndIncrement();
			JobDetails jobDetails = new JobDetails(jobId, jobSubmitRequest);
			jobs.put(jobId, jobDetails);
			runningMapTasks.put(jobId, new ConcurrentHashMap<Integer, Task>());
			mapQueue.add(jobDetails);
			
			responseBuilder.setJobId(jobId);
			responseBuilder.setStatus(Constants.STATUS_SUCCESS);
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			responseBuilder.setStatus(Constants.STATUS_FAIL);
		}
		System.out.println(responseBuilder.build());
		return responseBuilder.build().toByteArray();
	}

	@Override
	public byte[] getJobStatus(byte[] data) throws RemoteException{

		MapReduce.JobStatusResponse.Builder res = MapReduce.JobStatusResponse
				.newBuilder();
		try {
			MapReduce.JobStatusRequest req = MapReduce.JobStatusRequest
					.parseFrom(data);
			System.out.println(req);
			if (!jobs.containsKey(req.getJobId()))
				throw new Exception(" Job Id: " + req.getJobId()
						+ " doesn't exist");
			JobDetails jobDetails = jobs.get(req.getJobId());
			res.setJobDone(jobDetails.numOfReduceTasksCompleted.get() == jobDetails.totalReduceTasks);
			res.setNumMapTasksStarted(jobDetails.numOfMapTasksStarted);
			res.setNumReduceTasksStarted(jobDetails.numOfReduceTasksStarted);
			res.setStatus(Constants.STATUS_SUCCESS);
			res.setTotalMapTasks(jobDetails.totalMapTasks);
			res.setTotalReduceTasks(jobDetails.totalReduceTasks);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			res.setStatus(Constants.STATUS_FAIL);
		}
		System.out.println(res.build());
		return res.build().toByteArray();
	}

	@Override
	public byte[] heartBeat(byte[] data) throws RemoteException{
		MapReduce.HeartBeatResponse.Builder res = MapReduce.HeartBeatResponse
				.newBuilder();

		System.out.println(" ####### got heart beat requestfrom ####### "+ data);
		try {
			MapReduce.HeartBeatRequest req = MapReduce.HeartBeatRequest
					.parseFrom(data);
			
			System.out.println(req);
			System.out.println(" ####### getting mapTaskStatus tasks ####### ");
			for(MapReduce.MapTaskStatus mapTaskStatus:req.getMapStatusList()){
				
				if( mapTaskStatus.getTaskCompleted() ){
					if(  runningMapTasks.containsKey(mapTaskStatus.getJobId()) 
							&& runningMapTasks.get(mapTaskStatus.getJobId()).containsKey(mapTaskStatus.getTaskId())){
						JobDetails jobDetails=jobs.get(mapTaskStatus.getJobId());
						jobDetails.numOfMapTasksCompleted.getAndIncrement();
						jobDetails.mapOutputFiles.add(mapTaskStatus.getMapOutputFile());
						runningMapTasks.get(mapTaskStatus.getJobId()).remove(mapTaskStatus.getTaskId());
						if( jobDetails.numOfMapTasksCompleted.get() == jobDetails.totalMapTasks){
							runningMapTasks.remove(jobDetails.id);
							runningReduceTasks.put(jobDetails.id, new ConcurrentHashMap<Integer, Task>());
							reduceQueue.add(jobDetails);
						}
					}
				}
			}
			
			System.out.println(" ####### getting reduceTaskStatus tasks ####### ");
			for(MapReduce.ReduceTaskStatus reduceTaskStatus:req.getReduceStatusList()){
				
				if(reduceTaskStatus.getTaskCompleted()){
					if(runningReduceTasks.containsKey(reduceTaskStatus.getJobId()) &&
							runningReduceTasks.get(reduceTaskStatus.getJobId()).containsKey(reduceTaskStatus.getTaskId())){
						runningReduceTasks.get(reduceTaskStatus.getJobId()).remove(reduceTaskStatus.getTaskId());
						JobDetails jobDetails=jobs.get(reduceTaskStatus.getJobId());
						jobDetails.numOfReduceTasksCompleted.getAndIncrement();
						if(jobDetails.numOfReduceTasksCompleted.get() == jobDetails.totalReduceTasks){
							jobDetails.isDone=true;
							runningReduceTasks.remove(reduceTaskStatus.getJobId());
						}
					}
				}
			}
			
			
			int taskTrackerId = req.getTaskTrackerId(), numMapSlotsFree = req
					.getNumMapSlotsFree(), numReduceSlotsFree = req
					.getNumReduceSlotsFree();

			System.out.println(" ####### building mapInfo tasks ####### ");
			while (numMapSlotsFree > 0 && !mapQueue.isEmpty()) {
				JobDetails jobDetails = mapQueue.peek();
				if (jobDetails != null) {
					synchronized (jobDetails) {

						for (; jobDetails.numOfMapTasksStarted < jobDetails.totalMapTasks
								&& numMapSlotsFree > 0; jobDetails.numOfMapTasksStarted++, numMapSlotsFree--) {
							MapReduce.MapTaskInfo.Builder mapBuilder = MapReduce.MapTaskInfo
									.newBuilder();
							mapBuilder.setJobId(jobDetails.id);
							int taskId=taskIdCounter
									.getAndIncrement();
							mapBuilder.setTaskId(taskId);
							mapBuilder.setMapName(jobDetails.mapName);
							System.out.println("#### BlockLocatin Info ####"+jobDetails.blockLoctions.get(jobDetails.numOfMapTasksStarted));
							mapBuilder.addInputBlocks(jobDetails.blockLoctions
									.get(jobDetails.numOfMapTasksStarted));
							
							res.addMapTasks(mapBuilder.build());
							Task task=new Task(taskId, jobDetails.id, taskTrackerId);
							runningMapTasks.get(jobDetails.id).put(taskId, task);
						}
						if (jobDetails.numOfMapTasksStarted == jobDetails.totalMapTasks) {
							mapQueue.remove();
						}
					}
				}

			}
			System.out.println(" ####### building reduceInfo tasks ####### ");
			while ( numReduceSlotsFree > 0 && !reduceQueue.isEmpty()) {
				
				JobDetails jobDetails = reduceQueue.peek();
				System.out.println("  #####reduce dequeue "+jobDetails+"  ");
				if (jobDetails != null) {
					synchronized (jobDetails) {

						for (; jobDetails.numOfReduceTasksStarted < jobDetails.totalMapTasks
								&& numReduceSlotsFree > 0; jobDetails.numOfReduceTasksStarted++, numReduceSlotsFree--) {
							MapReduce.ReducerTaskInfo.Builder reduceTaskInfoBuilder= MapReduce.ReducerTaskInfo.newBuilder();
							reduceTaskInfoBuilder.setReducerName(jobDetails.reducerName);
							reduceTaskInfoBuilder.setJobId(jobDetails.id);
							int taskId=taskIdCounter.getAndIncrement();
							reduceTaskInfoBuilder.setTaskId(taskId);
							int count=jobDetails.numOfMapOutFilesForReducer;
							while(count>0 && !jobDetails.mapOutputFiles.isEmpty()){
								reduceTaskInfoBuilder.addMapOutputFiles(jobDetails.mapOutputFiles.poll());
								count--;
							}
							reduceTaskInfoBuilder.setOutputFile(jobDetails.outPutFile);
							res.addReduceTasks(reduceTaskInfoBuilder.build());
							Task task=new Task(taskId, jobDetails.id, taskTrackerId);
							runningReduceTasks.get(jobDetails.id).put(taskId, task);
						}
						if (jobDetails.numOfReduceTasksStarted == jobDetails.totalReduceTasks) {
							reduceQueue.remove();
						}
						
					}
				}

			}
			System.out.println(" ####### end of try ####### ");
			
			res.setStatus(Constants.STATUS_SUCCESS);

		} catch (InvalidProtocolBufferException e) {

			e.printStackTrace();
			res.setStatus(Constants.STATUS_FAIL);
		}

		System.out.println(res.build());
		return res.build().toByteArray();
	}

	public static class JobDetails {
		int id, numOfReduceTasks, status;
		String inputFile, outPutFile, mapName, reducerName;
		boolean isDone;
		int totalMapTasks, totalReduceTasks, numOfMapTasksStarted,
				numOfReduceTasksStarted;
		AtomicInteger numOfMapTasksCompleted, numOfReduceTasksCompleted;
		List<MapReduce.BlockLocations> blockLoctions;
		Queue< String > mapOutputFiles;
		int numOfMapOutFilesForReducer;
		public JobDetails(int id, MapReduce.JobSubmitRequest req) {
			this.id = id;
			numOfReduceTasks = req.getNumReduceTasks();
			inputFile = req.getInputFile();
			outPutFile = req.getOutputFile();
			mapName = req.getMapName();
			reducerName = req.getReducerName();
			getBlockLocations();
			totalMapTasks = blockLoctions.size();
			totalReduceTasks = numOfReduceTasks;
			numOfMapTasksStarted = 0;
			numOfReduceTasksStarted = 0;
			numOfMapTasksCompleted=new AtomicInteger(0);
			numOfReduceTasksCompleted=new AtomicInteger(0);
			mapOutputFiles=new ConcurrentLinkedQueue();
			numOfMapOutFilesForReducer=(int) Math.ceil( (double)blockLoctions.size() / totalReduceTasks );
		}

		void getBlockLocations() {

			HDFS.OpenFileRequest req = HDFS.OpenFileRequest.newBuilder()
					.setFileName(inputFile).setForRead(true).build();
			try {
				HDFS.OpenFileResponse res = HDFS.OpenFileResponse
						.parseFrom(nameNode.openFile(req.toByteArray()));
				System.out.println("###### Open Response "+ res);
				HDFS.BlockLocationRequest blockLocationRequest = HDFS.BlockLocationRequest
						.newBuilder().addAllBlockNums(res.getBlockNumsList())
						.build();
				HDFS.BlockLocationResponse blockLocationResponse = HDFS.BlockLocationResponse
						.parseFrom(nameNode
								.getBlockLocations(blockLocationRequest
										.toByteArray()));
				System.out.println("###### BlockLocation Response "+ res);
				blockLoctions = getLocations(blockLocationResponse
						.getBlockLocationsList());
			} catch (InvalidProtocolBufferException | RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public static List<MapReduce.BlockLocations> getLocations(
				List<HDFS.BlockLocations> locs) {
			List<MapReduce.BlockLocations> mlocs = new ArrayList<MapReduce.BlockLocations>();
			for (HDFS.BlockLocations l : locs) {
				MapReduce.BlockLocations ml = MapReduce.BlockLocations
						.newBuilder()
						.setBlockNumber(l.getBlockNumber())
						.addAllLocations(
								getDataNodeLocations(l.getLocationsList()))
						.build();
				mlocs.add(ml);
			}
			return mlocs;
		}

		public static List<MapReduce.DataNodeLocation> getDataNodeLocations(
				List<HDFS.DataNodeLocation> locs) {

			List<MapReduce.DataNodeLocation> mlocs = new ArrayList<MapReduce.DataNodeLocation>();
			for (HDFS.DataNodeLocation l : locs) {
				System.out.println(l);
				mlocs.add(MapReduce.DataNodeLocation.newBuilder()
						.setIp(l.getIp()).setPort(l.getPort()).build());
			}
			return mlocs;
		}
	}

	public static class Task {
		int id, jobId, taskTracketId;

		public Task(int id, int jobId, int taskTracketId) {
			super();
			this.id = id;
			this.jobId = jobId;
			this.taskTracketId = taskTracketId;
		}

	}

	public static void main(String args[]) {

		try {
			String name = "JT";
			IJobTracker engine = new JobTracker();
			IJobTracker stub = (IJobTracker) UnicastRemoteObject.exportObject(
					engine, 0);
			Registry registry = LocateRegistry.getRegistry();
			registry.rebind(name, stub);
			System.out.println("JobTracker bound  " + name);
		} catch (Exception e) {
			System.err.println("JobTracker exception:");
			e.printStackTrace();
		}
	}
}
