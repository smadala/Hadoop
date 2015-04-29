package Client;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;


public class ComputeClient {
    public static void main(String args[]) {
        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
        try {
            String name = "Compute";
            Registry registry = LocateRegistry.getRegistry(args[0]);
            Compute comp = (Compute) registry.lookup(name);
            int number = 99;
            System.out.println("Square of "+number+" = "+comp.computeSquare(number));
        } catch (Exception e) {
            System.err.println("Exception:");
            e.printStackTrace();
        }
    }    
}
