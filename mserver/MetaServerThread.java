package mserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import commons.sendingObject;

public class MetaServerThread implements Runnable, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final int CHUNK_SIZE = 8192;
	private Integer port = null;

	public MetaServerThread(Integer port) {
		this.port = port;
	}

	public void run() {
		try {
			ServerSocket serv = new ServerSocket(port);
			while (true) {
				new Thread(new MultiServer(serv.accept())).start();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static class MultiServer extends Thread implements Serializable {
		private static final long serialVersionUID = 1L;
		// private MetaServer ms = new MetaServer();
		private Socket clientSocket;
		private OutputStream outStream;
		private ObjectOutputStream objOutStream;
		private InputStream inStream;
		private ObjectInputStream objInStream;
		static sendingObject sO = new sendingObject();
		static HashMap<String, ArrayList<sendingObject>> mapObject = new HashMap<>();
		String hostname = null;

		public MultiServer(Socket socket) {
			this.clientSocket = socket;
		}

		public void run() {
			try {
				hostname = clientSocket.getInetAddress().getHostName();
				System.out.println(hostname);
				outStream = clientSocket.getOutputStream();
				objOutStream = new ObjectOutputStream(outStream);
				objOutStream.flush();
				inStream = clientSocket.getInputStream();
				objInStream = new ObjectInputStream(inStream);
				Object obj = objInStream.readObject();

				if (obj instanceof sendingObject) {
					System.out.println("Object received of type: sendingObject");
					sO = (sendingObject) obj;
					System.out.println("Type is: " + sO.getType() + " Filename is: " + sO.getFileName());
					String type = sO.getType().toUpperCase();
					switch (type) {

					case "READ":
						System.out.println("\nInside " + type + " case");
						List<sendingObject> lR = new ArrayList<>();
						if (!MetaServer.map.containsKey(sO.getFileName())) {
							System.out.println("File " + sO.getFileName() + " does not exist");
							sO.setMessage("File " + sO.getFileName() + " does not exist");
							lR.add(sO);
							objOutStream.writeObject(lR);
						} else {
							int chunkNo = sO.getReadBytesFrom() / CHUNK_SIZE;
							System.out.println("CHUNK NUMBER:" + " " + chunkNo);
							System.out.println("READ FROM:" + " " + sO.getReadBytesFrom());
							System.out.println("READ TILL:" + " " + sO.getReadNumOfBytes());
							int sum = sO.getReadBytesFrom() + sO.getReadNumOfBytes();
							int diff = 0;
							sendingObject sONew;
							try {
								// sendingObject OBJECT which needs to be
								// accessed by metaserver to get
								// server name, chunkNo/chunkName,
								int chunksize = MetaServer.map.get(sO.getFileName()).get(chunkNo).getFileSize();
								if (sum > ((chunkNo + 1) * CHUNK_SIZE)) {
									diff = (chunkNo + 1) * CHUNK_SIZE - sO.getReadBytesFrom();
									int loopTill = (sO.getReadBytesFrom() + sO.getReadNumOfBytes()) / CHUNK_SIZE;
									System.out.println("Looptill: " + loopTill);
									int toReadFrom = sO.getReadBytesFrom();
									int numToRead = sO.getReadNumOfBytes();
									for (int i = chunkNo; i <= loopTill; i++) {
										System.out.println("CHUNK IN LOOP" + " " + i);
										List<String> serverList = MetaServer.map.get(sO.getFileName()).get(i)
												.getServerName();
										String servername = chooseServer(serverList);
										if (!MetaServer.liveFlag.get(servername)) {
											sO.setMessage("Server is Down, Cannot Access file");
											lR.add(sO);
											objOutStream.writeObject(lR);
										} else {
											sONew = new sendingObject();
											System.out.println("The chunk name to be accessed is "
													+ MetaServer.map.get(sO.getFileName()).get(i).getChunkName());
											sONew.setChunkName(
													MetaServer.map.get(sO.getFileName()).get(i).getChunkName());
											sONew.setServerName(serverList);
											sONew.setUpdatedReadBytesFrom(toReadFrom % CHUNK_SIZE);
											// sONew.setUpdatedReadBytesFrom(chunksize % CHUNK_SIZE);
											System.out.println("MOD VALUE:" + " " + toReadFrom % CHUNK_SIZE);
											// sONew.setFileSize(sO.getFileSize());
											sONew.setReadNumOfBytes(diff);
											// objOutStream.writeObject(sONew);
											System.out.println("CHUNK NAME:" + " " + sONew.getChunkName());
											System.out.println("Server Name:" + " " + sONew.getServerName());
											System.out.println("READ FROM:" + " " + sONew.getUpdatedReadBytesFrom());
											System.out.println("READ TILL:" + " " + sONew.getReadNumOfBytes());
											chunksize = MetaServer.map.get(sO.getFileName()).get(i).getFileSize();
											toReadFrom = ((i + 1) * CHUNK_SIZE) + 1;
											diff = sO.getReadNumOfBytes() - diff;
											lR.add(sONew);
										}
									}
									objOutStream.writeObject(lR);
								} else {
									List<String> serverList = MetaServer.map.get(sO.getFileName()).get(chunkNo)
											.getServerName();
									String servername = chooseServer(serverList);
									if (!MetaServer.liveFlag.get(servername)) {
										sO.setMessage("Server is Down, Cannot Access file");
										lR.add(sO);
										objOutStream.writeObject(lR);
									} else {
										sONew = new sendingObject();
										System.out.println("The chunk name to be accessed is "
												+ MetaServer.map.get(sO.getFileName()).get(chunkNo).getChunkName());
										sONew.setChunkName(
												MetaServer.map.get(sO.getFileName()).get(chunkNo).getChunkName());
										sONew.setServerName(serverList);
										sONew.setUpdatedReadBytesFrom(sO.getReadBytesFrom() % CHUNK_SIZE);
										sONew.setReadNumOfBytes(sO.getReadNumOfBytes());
										lR.add(sONew);
										objOutStream.writeObject(lR);
									}
								}
							} catch (Exception e) {
								System.out.println("Chunk not found while accessing the read.");
							}
						}
						break;

					case "APPEND":
						System.out.println("\nInside " + type + " case");
						List<sendingObject> lA = new ArrayList<>();
						// check if file exists: If not entering if case
						if (!MetaServer.map.containsKey(sO.getFileName())) {
							if (sO.getWriteNumOfBytes() > 2048) {
								System.out.println("Illegal write size");
								sO.setMessage("Cannot read more than 2048 bytes");
								lA.add(sO);
								objOutStream.writeObject(lA);
							} else {
								System.out.println("File " + sO.getFileName() + " does not exist");
								System.out.println(
										"Create a new file and chunk for the requested file: " + sO.getFileName());
								List<String> servername = selectServer();
								if (servername.isEmpty()) {
									sO.setMessage("Server is Down, Cannot Access file");
									lA.add(sO);
									// objOutStream.writeObject(lA);
								} else {

									String cn = new SimpleDateFormat("yyyyMMddHHmmss_SSS'.txt'").format(new Date());
									sendingObject sOCreate = null;
									for (String s : servername) {
										ArrayList<sendingObject> list = new ArrayList<>();
										sOCreate = new sendingObject();
										sOCreate = create(list, sOCreate, sO, s, cn, servername);

										makeConnection(sOCreate, s);
										// lA.add(sOCreate);
									}
									lA.add(sOCreate);
								}

								objOutStream.writeObject(lA);
								// File exists
							}
						} else {
							sendingObject sONew = new sendingObject();
							int listSize = MetaServer.map.get(sO.getFileName()).size();
							System.out.println(sO.getWriteNumOfBytes());
							if (sO.getWriteNumOfBytes() > 2048) {
								System.out.println("Illegal write size");
								sO.setMessage("Cannot read more than 2048 bytes");
								lA.add(sO);
								objOutStream.writeObject(lA);
							}
							int size = CHUNK_SIZE
									- MetaServer.map.get(sO.getFileName()).get(listSize - 1).getFileSize();
							// checks if the file size is less than write
							// command from user/client
							if (sO.getWriteNumOfBytes() > size) {
								// Sending the object to server containting
								// last
								// chunk of the file
								// Integer size = CHUNK_SIZE
								// -
								// MetaServer.map.get(sO.getFileName()).get(listSize
								// - 1).getFileSize();
								sONew.setType("append");
								sONew.setChunkName(
										MetaServer.map.get(sO.getFileName()).get(listSize - 1).getChunkName());
								sONew.setWriteNumOfBytes(size);
								sONew.setFileSize(CHUNK_SIZE);
								sONew.setWriteType("null");
								List<String> serverList = MetaServer.map.get(sO.getFileName()).get(listSize - 1)
										.getServerName();
								// String server1 = chooseServer(serverList);
								for (Iterator<String> it = serverList.iterator(); it.hasNext();) {
									String server1 = it.next();
									if (!MetaServer.liveFlag.get(server1)) {
										it.remove();
										if (serverList.isEmpty()) {
											sO.setMessage("All Servers are Down, Cannot Access file");
											lA.add(sO);
											objOutStream.writeObject(lA);
											break;
										}
									}
								}
								for (String server : serverList) {
									Integer port = MetaServer.servers.get(server);
									Socket sock = new Socket(server, port);
									OutputStream outStream1 = sock.getOutputStream();
									ObjectOutputStream objOutStream1 = new ObjectOutputStream(outStream1);
									objOutStream1.flush();
									objOutStream1.writeObject(sONew);
									objOutStream1.flush();

									// accepting connection from that server and
									// doing nothing.
									InputStream inStream1 = sock.getInputStream();
									ObjectInputStream objInStream1 = new ObjectInputStream(inStream1);
									objInStream1.readObject();
								}

								// Choosing a random server to send the
								// create
								// chunk command
								List<String> servername = selectServer();
								if (servername.isEmpty()) {
									sO.setMessage("Server is Down, Cannot Access file");
									lA.add(sO);
									objOutStream.writeObject(lA);
								}

								// generating a new object to be sent to
								// that
								// newly chosen server
								// List<sendingObject> l = new ArrayList<>();
								String cn = new SimpleDateFormat("yyyyMMddHHmmss_SSS'.txt'").format(new Date());
								sendingObject sOCreate = null;
								for (String s : servername) {
									ArrayList<sendingObject> list = MetaServer.map.get(sO.getFileName());
									sOCreate = new sendingObject();
									sOCreate = create(list, sOCreate, sO, s, cn, servername);

									// creating a socket connection to the new
									// server and sending the object
									makeConnection(sOCreate, s);
									// lA.add(sOCreate);
								}
								lA.add(sOCreate);
								// Sending object to client with servers
								// name
								// and chunk name
								objOutStream.writeObject(lA);
								objOutStream.flush();
							} else {
								List<String> serverList = MetaServer.map.get(sO.getFileName()).get(listSize - 1)
										.getServerName();
								String server = chooseServer(serverList);
								int newSize = MetaServer.map.get(sO.getFileName()).get(listSize - 1).getFileSize()
										+ sO.getWriteNumOfBytes();
								MetaServer.map.get(sO.getFileName()).get(listSize - 1).setFileSize(newSize);
								for (Iterator<String> it = serverList.iterator(); it.hasNext();) {
									String server1 = it.next();
									if (!MetaServer.liveFlag.get(server1)) {
										it.remove();
										if (serverList.isEmpty()) {
											sO.setMessage("All Servers are Down, Cannot Access file");
											lA.add(sO);
											objOutStream.writeObject(lA);
											break;
										}
									}
								}
								sONew.setChunkName(
										MetaServer.map.get(sO.getFileName()).get(listSize - 1).getChunkName());
								sONew.setServerName(
										MetaServer.map.get(sO.getFileName()).get(listSize - 1).getServerName());
								sONew.setServer(server);
								lA.add(sONew);
								objOutStream.writeObject(lA);
							}
						}
						break;

					default:
						System.out.println("ERROR: Request type not found.");
						break;
					}
				} else if (obj instanceof HashMap) {
					System.out.println();
					// System.out.println("Printing thread name: " +
					// Thread.currentThread().getName());
					System.out.println("Object received of type: Hashmap from: " + hostname);
					// System.out.println("Heartbeat received from: " +
					// hostname);
					MetaServer.time.put(hostname, System.currentTimeMillis());
					System.out.println(System.currentTimeMillis());
					MetaServer.liveFlag.replace(hostname, true);
					System.out.println("Boolean for: "+hostname+ "is: "+MetaServer.liveFlag.get(hostname));
					mapObject = (HashMap<String, ArrayList<sendingObject>>) obj;
					System.out.println("Before: " + MetaServer.map);
					for (Entry<String, ArrayList<sendingObject>> entry : mapObject.entrySet()) {
						ArrayList<sendingObject> listed = MetaServer.map.get(entry.getKey());
						for (sendingObject object : mapObject.get(entry.getKey())) {
							for (int i = 0; i < listed.size() - 1; i++) {
								String og = listed.get(i).getChunkName();
								String dup = object.getChunkName();
								if (og.equals(dup)) {
									MetaServer.map.get(entry.getKey()).set(i, object);
								}
							}
						}
					}
					System.out.println("After: " + MetaServer.map);
				}
			} catch (IOException | ClassNotFoundException e) {
				e.printStackTrace();
			}
		}

		private void makeConnection(sendingObject sOCreate, String servername)
				throws UnknownHostException, IOException {
			Integer port = MetaServer.servers.get(servername);
			Socket sock = null;
			try {
				sock = new Socket(servername, port);
				OutputStream outStream = sock.getOutputStream();
				ObjectOutputStream objOutStream = new ObjectOutputStream(outStream);
				objOutStream.flush();
				objOutStream.writeObject(sOCreate);
				objOutStream.flush();
			} finally {
				sock.close();
			}
		}

		private List<String> selectServer() {
			Random random = new Random();
			List<String> keys = new ArrayList<String>(MetaServer.servers.keySet());
			for (Map.Entry<String, Boolean> entry : MetaServer.liveFlag.entrySet()) {
				if (entry.getValue() == false) {
					keys.remove(entry.getKey());
				}
			}
			// if (!keys.isEmpty()) {
			// Collections.shuffle(keys);
			// List<String> sublist = keys.subList(0, 3);
			// return sublist;
			// }
			// else {
			// return keys;
			// }
			if (keys.size() >= 0 && keys.size() <= 3)
				return keys;
			else if (keys.size() > 3) {
				for (int i = 1; i < keys.size() - 3; i++)
					keys.remove(random.nextInt(keys.size()));
			}
			return keys;
			// if(!keys.isEmpty()) {
			// String servername = keys.get(random.nextInt(keys.size()));
			// return servername;
			// }
			// else
			// return null;
		}

		private sendingObject create(ArrayList<sendingObject> list, sendingObject sOCreate, sendingObject sO,
				String server, String chunkName, List<String> serverList) {
			sOCreate.setType("create");
			sOCreate.setFileSize(sO.getWriteNumOfBytes());
			sOCreate.setFileName(sO.getFileName());
			sOCreate.setChunkName(chunkName);
			sOCreate.setServerName(serverList);
			sOCreate.setServer(server);
			list.add(sOCreate);
			// System.out.println("Server selected is: " + servername);
			System.out.println("New chunk created for file: " + sO.getFileName() + " is: " + sOCreate.getChunkName());
			MetaServer.map.put(sO.getFileName(), list);
			return sOCreate;
		}

		private String chooseServer(List<String> serverList) {
			System.out.println("ServerList size is: "+serverList.size());
			Random randomizer = new Random();
			String servername = serverList.get(randomizer.nextInt(serverList.size()));
			return servername;
		}
	}
}
