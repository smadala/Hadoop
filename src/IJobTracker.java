import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IJobTracker extends Remote{
	
	/* JobSubmitResponse jobSubmit(JobSubmitRequest) */
	byte[] jobSubmit(byte[] data) throws RemoteException;

	/* JobStatusResponse getJobStatus(JobStatusRequest) */
	byte[] getJobStatus(byte[] data) throws RemoteException;
	
	/* HeartBeatResponse heartBeat(HeartBeatRequest) */
	byte[] heartBeat(byte[] data) throws RemoteException;
}
