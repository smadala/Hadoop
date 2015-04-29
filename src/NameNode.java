import java.io.File;
import java.io.FileInputStream;
import java.nio.channels.ShutdownChannelGroupException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;

import Server.Compute;
import Server.ComputeEngine;

import com.google.protobuf.InvalidProtocolBufferException;

public class NameNode implements INameNode {

	private static class HFile {
		private String name;
		private boolean isWritting;
		private List<Integer> blockNums;

		public HFile(String name) {
			this.name = name;
			blockNums = new ArrayList<Integer>();
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public List<Integer> getBlockNums() {
			return blockNums;
		}

		public void setBlockNums(List<Integer> blockNums) {
			this.blockNums = blockNums;
		}

	}

	private static class NodeLocation {
		String ip;
		int port, id;

		public NodeLocation(int id, String ip, int port) {
			this.ip = ip;
			this.port = port;
			this.id = id;
		}

		@Override
		public String toString() {
			return "NodeLocation [ip=" + ip + ", port=" + port + ", id=" + id
					+ "]";
		}

	}

	private Map<String, HFile> fs;
	private Map<Integer, Set<Integer>> locations;
	private Map<Integer, String> handlerMap;
	private Map<Integer, NodeLocation> dataNodes;
	private Map<Integer, Long> heartMap;

	private static AtomicInteger blockCounter = new AtomicInteger(1);
	
	private static AtomicInteger handlerCount = new AtomicInteger(1);
	private static long maxIdleTime = 30 * 1000;

	public NameNode() {
		super();
		fs = new ConcurrentHashMap<String, NameNode.HFile>();
		locations = new ConcurrentHashMap<Integer, Set<Integer>>();
		handlerMap = new ConcurrentHashMap<Integer, String>();
		dataNodes = new ConcurrentHashMap<Integer, NodeLocation>();
		heartMap = new ConcurrentHashMap<Integer, Long>();
		// TODO Auto-generated constructor stub
	}

	@Override
	public byte[] openFile(byte[] data) {
		// TODO Auto-generated method stub
		HDFS.OpenFileRequest.Builder requestBuilder = HDFS.OpenFileRequest
				.newBuilder();
		HDFS.OpenFileResponse.Builder responseBuilder = HDFS.OpenFileResponse
				.newBuilder();
		try {
			requestBuilder.mergeFrom(data);
			HDFS.OpenFileRequest request = requestBuilder.build();
			String fileName = request.getFileName();
			boolean forRead = request.getForRead(), succ = false;
			HFile hfile = null;
			synchronized (this) {

				if (!fs.containsKey(fileName) && !request.getForRead()) {
					hfile = new HFile(fileName);
					fs.put(fileName, hfile);
					hfile.isWritting = true;
					succ = true;
				} else if (fs.containsKey(fileName) && request.getForRead()
						&& !fs.get(fileName).isWritting) {
					hfile = fs.get(fileName);
					responseBuilder.addAllBlockNums(hfile.getBlockNums());
					succ = true;
				}
			}
			if (succ) {
				responseBuilder.setStatus(Constants.STATUS_SUCCESS);
				int h = handlerCount.getAndIncrement();
				responseBuilder.setHandle(h);
				handlerMap.put(h, fileName);
			} else {
				responseBuilder.setStatus(Constants.STATUS_FAIL);
			}
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return responseBuilder.build().toByteArray();
	}

	@Override
	public byte[] closeFile(byte[] data) {
		// TODO Auto-generated method stub
		HDFS.CloseFileRequest.Builder requestBuilder = HDFS.CloseFileRequest
				.newBuilder();
		HDFS.CloseFileResponse.Builder responseBuilder = HDFS.CloseFileResponse
				.newBuilder();
		try {
			requestBuilder.mergeFrom(data);
			int handler = requestBuilder.getHandle();
			if (handlerMap.containsKey(handler)) {
				responseBuilder.setStatus(Constants.STATUS_SUCCESS);
				if (isHandleForWrite(handler)) {
					fs.get(handlerMap.get(handler)).isWritting = false;
				}
				handlerMap.remove(handler);
			} else {
				responseBuilder.setStatus(Constants.STATUS_FAIL);
			}
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return responseBuilder.build().toByteArray();
	}

	@Override
	public byte[] getBlockLocations(byte[] data) {
		// TODO Auto-generated method stub
		HDFS.BlockLocationRequest.Builder requestBuilder = HDFS.BlockLocationRequest
				.newBuilder();
		HDFS.BlockLocationResponse.Builder responseBuilder = HDFS.BlockLocationResponse
				.newBuilder();
		try {
			requestBuilder.mergeFrom(data);
			HDFS.BlockLocationRequest request = requestBuilder.build();
			System.out.println(" ##########Block Location request"+request);
			responseBuilder.setStatus(Constants.STATUS_SUCCESS);
			for (Integer blockNum : request.getBlockNumsList()) {

				responseBuilder.addBlockLocations(getBlockLocations(blockNum));
			}
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("##### BlockLocation response "+responseBuilder.build());
		return responseBuilder.build().toByteArray();
	}

	private HDFS.BlockLocations getBlockLocations(int blockNum) {
		HDFS.BlockLocations.Builder locationBuilder = HDFS.BlockLocations
				.newBuilder();
		locationBuilder.setBlockNumber(blockNum);
		for (Integer id : locations.get(blockNum)) {

			System.out.println("#### location "+ dataNodes.get(id));
			locationBuilder
					.addLocations(getDataNodeLocation(dataNodes.get(id)));
		}
		
		return locationBuilder.build();
	}

	private HDFS.DataNodeLocation getDataNodeLocation(NodeLocation nodeLocation) {

		HDFS.DataNodeLocation.Builder nodeLocationBuilder = HDFS.DataNodeLocation
				.newBuilder();
		nodeLocationBuilder.setIp(nodeLocation.ip);
		nodeLocationBuilder.setPort(nodeLocation.port);
		return nodeLocationBuilder.build();
	}

	@Override
	public byte[] assignBlock(byte[] data) {

		HDFS.AssignBlockRequest.Builder requestBuilder = HDFS.AssignBlockRequest
				.newBuilder();
		HDFS.AssignBlockResponse.Builder responseBuilder = HDFS.AssignBlockResponse
				.newBuilder();
		try {
			requestBuilder.mergeFrom(data);
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		HDFS.AssignBlockRequest request = requestBuilder.build();
		int h = request.getHandle();

		if (!handlerMap.containsKey(h) || dataNodes.size() < Constants.replicationFactor
				|| !isHandleForWrite(h)) {
			responseBuilder.setStatus(Constants.STATUS_FAIL);
		} else {
			HFile hfile = fs.get(handlerMap.get(h));

			responseBuilder.setStatus(Constants.STATUS_SUCCESS);
			List<Integer> nodeNumbers = new ArrayList<Integer>(
					dataNodes.keySet());

			Collections.shuffle(nodeNumbers);
			int blockNum = blockCounter.getAndIncrement();
			hfile.getBlockNums().add(blockNum);
			HDFS.BlockLocations.Builder blockLocationBuilder = HDFS.BlockLocations
					.newBuilder();
			blockLocationBuilder.setBlockNumber(blockNum);
			Set<Integer> dataNodeIds = new HashSet<Integer>();
			for (int i = 0; i < nodeNumbers.size() && i < Constants.replicationFactor; i++) {
				blockLocationBuilder.addLocations(getDataNodeLocation(dataNodes
						.get(nodeNumbers.get(i))));
				// dataNodeIds.add(nodeNumbers.get(i));
			}
			// locations.put(blockNum, dataNodeIds);
			responseBuilder.setNewBlock(blockLocationBuilder.build());
		}

		return responseBuilder.build().toByteArray();
	}

	private boolean isHandleForWrite(int handlerNum) {
		String fileName = handlerMap.get(handlerNum);
		HFile hfile = fs.get(fileName);
		return hfile.isWritting;
	}

	@Override
	public byte[] list(byte[] data) {
		// TODO Auto-generated method stub
		HDFS.ListFilesResponse.Builder responseBuilder = HDFS.ListFilesResponse
				.newBuilder();

		responseBuilder.setStatus(Constants.STATUS_SUCCESS);
		for (String fileName : fs.keySet()) {
			responseBuilder.addFileNames(fileName);
		}
		return responseBuilder.build().toByteArray();
	}

	@Override
	public byte[] blockReport(byte[] data) {
		// TODO Auto-generated method stub
		HDFS.BlockReportRequest request=null;
		try {
			request = HDFS.BlockReportRequest.parseFrom(data);
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		HDFS.BlockReportResponse.Builder responseBuilder = HDFS.BlockReportResponse
				.newBuilder();
		System.out.println(" ####### Block Report Request "+ request);
		int id = request.getId();
		System.out.println("Got block report from " + id);
		NodeLocation nodeLocation = new NodeLocation(id, request
				.getLocation().getIp(), request.getLocation().getPort());
		synchronized (this) {

			if (!dataNodes.containsKey(id)) {
				dataNodes.put(id, nodeLocation);
			}
			Set<Integer> nodes;
			System.out.println(" Block list from : "+ id +" "+request.getBlockNumbersList());
			for (Integer blockNums : request.getBlockNumbersList()) {
				if (locations.containsKey(blockNums)) {
					nodes = locations.get(blockNums);
					nodes.add(id);
				} else {
					nodes = new HashSet<Integer>();
					nodes.add(id);
					locations.put(blockNums, nodes);
				}
				responseBuilder.addStatus(Constants.STATUS_SUCCESS);
			}
			System.out.println ("Current Location Map :" + locations.toString());
		}

		return responseBuilder.build().toByteArray();
	}

	@Override
	public byte[] heartBeat(byte[] data) {
		// TODO Auto-generated method stub
		HDFS.HeartBeatRequest.Builder requestBuilder = HDFS.HeartBeatRequest
				.newBuilder();
		HDFS.HeartBeatResponse.Builder responseBuilder = HDFS.HeartBeatResponse
				.newBuilder();
		try {
			requestBuilder.mergeFrom(data);
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int id = requestBuilder.getId();
		heartMap.put(id, System.currentTimeMillis());
		System.out.println("Received Heart beat from " + id);
		responseBuilder.setStatus(Constants.STATUS_SUCCESS);
		return responseBuilder.build().toByteArray();
	}

	private static int port = 5050;

	public static void main(String args[]) {

		try {
			String name = "NN";
			INameNode engine = new NameNode();
			INameNode stub = (INameNode) UnicastRemoteObject.exportObject(
					engine, 0);
			Registry registry = LocateRegistry.getRegistry();
			registry.rebind(name, stub);
			System.out.println("NameNode bound  " + name);
		} catch (Exception e) {
			System.err.println("NameNode exception:");
			e.printStackTrace();
		}
	}
}
