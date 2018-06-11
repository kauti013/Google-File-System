package servers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import commons.sendingObject;

public class Servers implements Serializable {

	private static final long serialVersionUID = 1L;
	public static HashMap<String, Integer> metaserver = new HashMap<String, Integer>();
	public static HashMap<String, Integer> clients = new HashMap<String, Integer>();
	public static HashMap<String, Boolean> liveFlag = new HashMap<>();
	public static String selfName = null;
	public static ServersThread st;
	public static HashMap<String, ArrayList<sendingObject>> map = new HashMap<>();
	public static boolean flag = true;

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
				// st.start(Integer.parseInt(connections[2]));
				System.out.println("After calling start method of MetaServerThread");
				if (connections[0].equals("clients"))
					clients.put(connections[1], Integer.parseInt(connections[2]));
				else if (connections[0].equals("meta"))
					metaserver.put(connections[1], Integer.parseInt(connections[2]));
				else
					continue;
			}
			// ExecutorService thread = Executors.newSingleThreadExecutor();
			// Runnable heartbeat = new heartBeat();
			// thread.execute(heartbeat);
			// System.out.println("Starting server listener");
			// st.start(port);
			
		} catch (Exception e) {
			e.printStackTrace();
		}

		while (true) {
			System.out.println("\nEnter Valid Commands: Enter EXIT to quit the program\n");
			String in = scan.nextLine();
			in = in.trim();
			String input[] = in.split(" ");
			switch (input[0].toUpperCase()) {

			case "SHOW_CLIENTS":
				for (Map.Entry<String, Integer> entry : clients.entrySet()) {
					String key = entry.getKey();
					Integer value = entry.getValue();
					StringBuilder sb = new StringBuilder();
					sb.append(key);
					sb.append(": ");
					sb.append(value);
					String peer = sb.toString();
					System.out.println(peer);
					sb.setLength(0);
				}
				break;

			case "SHOW_MSERVER":
				for (Map.Entry<String, Integer> entry : metaserver.entrySet()) {
					String key = entry.getKey();
					Integer value = entry.getValue();
					StringBuilder sb = new StringBuilder();
					sb.append(key);
					sb.append(": ");
					sb.append(value);
					String peer = sb.toString();
					System.out.println(peer);
					sb.setLength(0);
				}
				break;
				
			case "UP":
				heartBeat.flag = "UP";
				ExecutorService thread = Executors.newSingleThreadExecutor();
				Runnable heartbeat = new heartBeat();
				thread.execute(heartbeat);
//				new Thread(new heartBeat()).start();
				System.out.println("Starting server listener");
				new Thread(new ServersThread(port)).start();
				break;
			
				
			case "DOWN":
				heartBeat.flag = "DOWN";
				break;
				
			case "RE":
				heartBeat.flag = "RE";
				ExecutorService thread1 = Executors.newSingleThreadExecutor();
				Runnable heartbeat1 = new heartBeat();
				thread1.execute(heartbeat1);
				break;

			default:
				System.out.println("Please enter only Valid Cases! Thank You!");
				break;
			}
		}
	}
}