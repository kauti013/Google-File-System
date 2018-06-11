package mserver;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import commons.sendingObject;

public class MetaServer implements Serializable {

	private static final long serialVersionUID = 1L;
	public static HashMap<String, Integer> servers = new HashMap<>();
	public static HashMap<String, Integer> clients = new HashMap<>();
	public static HashMap<String, Boolean> liveFlag = new HashMap<>();
	public static String selfName = null;
	public static ArrayList<sendingObject> list = new ArrayList<>();
	public static HashMap<String, ArrayList<sendingObject>> map = new HashMap<>();
	public static ConcurrentHashMap<String, Long> time = new ConcurrentHashMap<>();

	public static void main(String[] args) throws UnknownHostException, IOException {

		String csvFile = "/home/010/j/ja/jas160630/AOS/jas160630_Proj3/connections.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		Scanner scan = new Scanner(System.in);
		Integer port = null;

		try {
			selfName = InetAddress.getLocalHost().getHostName();
			System.out.println("My Hostname is: " + selfName);
			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {
				// use comma as separator
				String[] connections = line.split(cvsSplitBy);
				// get the port number to start listening on that port
				if (connections[1].equals(selfName))
					port = Integer.parseInt(connections[2]);
				System.out.println("After calling start method of MetaServerThread");
				// get the host name and port number from file to populate
				// hashmaps
				if (connections[0].equals("servers")) {
					servers.put(connections[1], Integer.parseInt(connections[2]));
					liveFlag.put(connections[1], true);
				} else if (connections[0].equals("clients"))
					clients.put(connections[1], Integer.parseInt(connections[2]));
				else
					continue;
				System.out.println("keys in servers: "+servers.keySet()+" keys in clients: "+clients.keySet());
			}
			new Thread(new MetaServerThread(port)).start();
			new Thread(new CheckLiveness(45000)).start();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			br.close();
			scan.close();
		}
	}
}
