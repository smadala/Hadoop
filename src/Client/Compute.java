package Client;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Compute extends Remote {
    int computeSquare(int number) throws RemoteException;
}
