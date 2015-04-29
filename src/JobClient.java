import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Properties;

import com.google.protobuf.InvalidProtocolBufferException;


public class JobClient {
	
	private static IJobTracker jobTracker;
	public static void main(String args[]) throws Exception{
		if(args.length != 5){
			System.out.println("mapName reducerName inputFile_in_HDFS outputFile_in_HDFS numReducers");
			return;
		}
		String mapName=args[0], reduceName=args[1], inputFile=args[2], outputFile=args[3];
		int numOfReducer=Integer.parseInt(args[4]);
		Properties prop=new Properties();
		prop.load(new FileInputStream(Constants.JOB_TRACKER_CONFIG_FILE_NAME));
		String ip=prop.getProperty("JT_IP");
		
		jobTracker=getJobTrakcer(ip);

		int jobId=submitJob(mapName, reduceName, inputFile, outputFile, numOfReducer);
		MapReduce.JobStatusRequest req=MapReduce.JobStatusRequest.newBuilder().setJobId(jobId).build();
		boolean run=true;
		while(run){
			MapReduce.JobStatusResponse res=MapReduce.JobStatusResponse.parseFrom(jobTracker.getJobStatus(req.toByteArray()));
			if(res.getJobDone()){
				System.out.println(" Job : "+jobId+" Completed");
				run = false;
			}
			System.out.println("Job :"+ jobId +"\n"+res.toString());
			System.out.println( "% of map tasks started:" + ((double) res.getNumMapTasksStarted()/ res.getTotalMapTasks())*100 );
			System.out.println( "% of reduce tasks started:" + ((double) res.getNumReduceTasksStarted()/ res.getTotalReduceTasks())*100 );
			Thread.sleep(2000);
		}
		System.out.println("######### Job : "+jobId+" done ########");
		
		/*String substr="outputfile_"+jobId+"_*";
		
		List<String> allFiles=HDFSClient.*/
		
	}
	private static int submitJob(String mapName, String reduceName, String inFile, String outFile, int numOfReducer) throws Exception{
		
		MapReduce.JobSubmitRequest req=MapReduce.JobSubmitRequest.newBuilder()
				.setMapName(mapName).setReducerName(reduceName).setInputFile(inFile).
				setOutputFile(outFile).setNumReduceTasks(numOfReducer).build();
				
		 MapReduce.JobSubmitResponse res =MapReduce.JobSubmitResponse.parseFrom(jobTracker.jobSubmit(req.toByteArray()));
		 if( res.getStatus() == Constants.STATUS_FAIL){
			 throw new Exception("unable submit job "+ mapName+ " "+reduceName+" "+inFile+" "+outFile+" "+numOfReducer);
		 }
		 return res.getJobId();
	}

	public static IJobTracker getJobTrakcer(String ip){
		IJobTracker jobTracker=null;
		try {
			Registry registry = LocateRegistry.getRegistry(ip);
			 jobTracker = (IJobTracker)registry.lookup("JT");
			 
		} catch (RemoteException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return jobTracker;
	}
}
