package clients;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import commons.sendingObject;

public class Clients implements Serializable {

	private static final long serialVersionUID = 1L;
	public static HashMap<String, Integer> servers = new HashMap<String, Integer>();
	public static HashMap<String, Integer> metaserver = new HashMap<String, Integer>();
	public static HashMap<String, Boolean> liveFlag = new HashMap<>();
	public static String selfName = null;
	public static int port;
	public static ServerSocket sock = null;
	private static PrintWriter out;

	public static void main(String[] args) throws IOException, ClassNotFoundException {

		String csvFile = "/home/010/j/ja/jas160630/AOS/jas160630_Proj3/connections.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		Scanner scan = new Scanner(System.in);

		try {
			selfName = InetAddress.getLocalHost().getHostName();
			System.out.println("My Hostname is: " + selfName);
			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {
				// use comma as separator
				String[] connections = line.split(cvsSplitBy);
				if (connections[0].equals("servers"))
					servers.put(connections[1], Integer.parseInt(connections[2]));
				else if (connections[0].equals("meta"))
					metaserver.put(connections[1], Integer.parseInt(connections[2]));
				else
					continue;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			br.close();
		}

		while (true) {
			System.out.println("\nEnter Valid Commands: Enter EXIT to quit the program\n");
			String in = scan.nextLine();
			in = in.trim();
			String input[] = in.split(" ");
			String key = null;
			Integer value = null;
			switch (input[0].toUpperCase()) {

			case "SHOW_SERVERS":
				for (Map.Entry<String, Integer> entry : servers.entrySet()) {
					key = entry.getKey();
					value = entry.getValue();
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
					key = entry.getKey();
					value = entry.getValue();
					StringBuilder sb = new StringBuilder();
					sb.append(key);
					sb.append(": ");
					sb.append(value);
					String peer = sb.toString();
					System.out.println(peer);
					sb.setLength(0);
				}
				break;

			case "READ":
				for (Map.Entry<String, Integer> entry : metaserver.entrySet()) {
					key = entry.getKey();
					value = entry.getValue();

					// getting input and output streams for metaserver
					Socket clientSocket = new Socket(key, value);
					OutputStream outStream = clientSocket.getOutputStream();
					ObjectOutputStream objOutStream = new ObjectOutputStream(outStream);
					objOutStream.flush();
					InputStream inStream = clientSocket.getInputStream();
					ObjectInputStream objInStream = new ObjectInputStream(inStream);

					// sending object to metaserver
					sendingObject sORead = new sendingObject();
					sORead.setType(input[0]);
					sORead.setFileName(input[1]);
					sORead.setReadBytesFrom(Integer.parseInt(input[2]));
					sORead.setReadNumOfBytes(Integer.parseInt(input[3]));
					objOutStream.writeObject(sORead);
					objOutStream.flush();

					// receiving object from metaserver
					/*
					 * sendingObject sO = new sendingObject(); sO = (sendingObject)
					 * objInStream.readObject();
					 */
					List<sendingObject> l = new ArrayList<>();
					l = (List<sendingObject>) objInStream.readObject();

					for (sendingObject sO : l) {
						if (sO.getServerName() == null) {
							System.out.println(sO.getMessage());
						} else {
							// getting input and output streams for server
							List<String> serverList = sO.getServerName();
							Random randomizer = new Random();
							String servername = serverList.get(randomizer.nextInt(serverList.size()));
							Integer port = servers.get(servername);
							Socket sock = new Socket(servername, port);
							OutputStream outStream1 = sock.getOutputStream();
							ObjectOutputStream objOutStream1 = new ObjectOutputStream(outStream1);
							objOutStream1.flush();
							InputStream inStream1 = sock.getInputStream();
							ObjectInputStream objInStream1 = new ObjectInputStream(inStream1);

							// sending object to server
							sendingObject sONew = new sendingObject();
							sONew.setType(sORead.getType());
							sONew.setFileName(sORead.getFileName());
							sONew.setChunkName(sO.getChunkName());
							sONew.setUpdatedReadBytesFrom(sO.getUpdatedReadBytesFrom());
							sONew.setReadNumOfBytes(sO.getReadNumOfBytes());
							objOutStream1.writeObject(sONew);
							objOutStream1.flush();
							System.out.println("ChunkName is " + sO.getChunkName());

							// receiving object from server
							sendingObject sO1 = new sendingObject();
							sO1 = (sendingObject) objInStream1.readObject();
							System.out.println("The contents in file are: " + sO1.getMessage());
						}
					}
				}
				break;

			case "APPEND":
				for (Map.Entry<String, Integer> entry : metaserver.entrySet()) {
					key = entry.getKey();
					value = entry.getValue();
					Socket clientSocket = new Socket(key, value);
					OutputStream outStream = clientSocket.getOutputStream();
					ObjectOutputStream objOutStream = new ObjectOutputStream(outStream);
					objOutStream.flush();
					InputStream inStream = clientSocket.getInputStream();
					ObjectInputStream objInStream = new ObjectInputStream(inStream);

					// sending object to metaserver
					sendingObject sOAppend = new sendingObject();
					sOAppend.setType(input[0]);
					sOAppend.setFileName(input[1]);
					sOAppend.setWriteNumOfBytes(Integer.parseInt(input[2]));
					objOutStream.writeObject(sOAppend);
					objOutStream.flush();

					// receiving object from metaserver
					// sendingObject sO = new sendingObject();
					List<sendingObject> l = new ArrayList<>();
					l = (List<sendingObject>) objInStream.readObject();

					for (sendingObject sO : l) {
						if (sO.getServerName() == null) {
							System.out.println(sO.getMessage());
						} else {
							// getting input and output streams for server
							// list of servers - iterate for each server
							for (String ser : sO.getServerName()) {
								Integer port = servers.get(ser);
								Socket sock = new Socket(ser, port);
								OutputStream outStream1 = sock.getOutputStream();
								ObjectOutputStream objOutStream1 = new ObjectOutputStream(outStream1);
								objOutStream1.flush();
								InputStream inStream1 = sock.getInputStream();
								ObjectInputStream objInStream1 = new ObjectInputStream(inStream1);

								// sending object to server
								sendingObject sONew = new sendingObject();
								sONew.setType(sOAppend.getType());
								sONew.setFileName(sOAppend.getFileName());
								sONew.setChunkName(sO.getChunkName());
								sONew.setWriteNumOfBytes(sOAppend.getWriteNumOfBytes());
								objOutStream1.writeObject(sONew);
								objOutStream1.flush();
								System.out.println("ChunkName is " + sO.getChunkName());

								// receiving object from server
								sendingObject sO1 = new sendingObject();
								sO1 = (sendingObject) objInStream1.readObject();
								System.out.println(sO1.getMessage());
							}
						}
					}
				}
				break;

			default:
				System.out.println("Please enter only Valid Cases! Thank You!");
				break;
			}
		}
	}
}