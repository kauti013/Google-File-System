package mserver;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import commons.sendingObject;

public class CheckLiveness implements Runnable {

	private Integer port = null;

	public CheckLiveness(Integer port) {
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
		static HashMap<String, ArrayList<sendingObject>> mapObject = new HashMap<>();
		String hostname = null;

		public MultiServer(Socket socket) {
			this.clientSocket = socket;
		}

		@Override
		public void run() {
			try {
				hostname = clientSocket.getInetAddress().getHostName();
				System.out.println(hostname);
				outStream = clientSocket.getOutputStream();
				objOutStream = new ObjectOutputStream(outStream);
				objOutStream.flush();
				inStream = clientSocket.getInputStream();
				objInStream = new ObjectInputStream(inStream);
				while (true) {
					Object obj = objInStream.readObject();

					if (obj instanceof HashMap) {
						System.out.println();
						// System.out.println("Printing thread name: " +
						// Thread.currentThread().getName());
						System.out.println("Object received of type: Hashmap from: " + hostname);
						// System.out.println("Heartbeat received from: " +
						// hostname);
						MetaServer.time.put(hostname, System.currentTimeMillis());
						System.out.println(System.currentTimeMillis());
						MetaServer.liveFlag.put(hostname, true);
						mapObject = (HashMap<String, ArrayList<sendingObject>>) obj;
						System.out.println("Before: " + MetaServer.map);
						for (Entry<String, ArrayList<sendingObject>> entry : mapObject.entrySet()) {
							ArrayList<sendingObject> listed = MetaServer.map.get(entry.getKey());
							for (sendingObject object : mapObject.get(entry.getKey())) {
								for (int i = 0; i < listed.size() - 1; i++) {
									// if(O.getChunkName().equals(object.getChunkName()))
									// {
									// O = object;
									// sendingObject oo = listed.get(i);
									String og = listed.get(i).getChunkName();
									String dup = object.getChunkName();
									if (og.equals(dup)) {
										/*
										 * System.out.
										 * println("changing the map to mapobject"
										 * ); oo = object;
										 */
										MetaServer.map.get(entry.getKey()).set(i, object);
									}
								}
							}
						}
						System.out.println("After: " + MetaServer.map);
					} else if (obj instanceof String) {
						String key = null;
						if (((String) obj).contains("ServerDown")) {
							Thread.sleep(2000);
							System.out.println("Total number of threads: " + Thread.activeCount());
							System.out.println(MetaServer.time);
							// if (!MetaServer.time.isEmpty()) {
							Iterator<String> it = MetaServer.time.keySet().iterator();
							while (it.hasNext()) {
								try {
									Thread.sleep(2000);
									key = it.next().toString();
									Long value = MetaServer.time.get(key);
									System.out.println("Printing key " + key + " and value: " + value);
									// Object item = it.next();
									// it.remove();
									Long diff = System.currentTimeMillis() - value;
									System.out.println("diff is: " + diff);
									if (diff > 15000) {
										// close the mserver flag for this
										// server
										System.out.println("Updated live flag to false for: "+key);
										MetaServer.liveFlag.put(key, false);
									}
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
							}
						} else if (((String) obj).contains("ServerUp")) {
							System.out.println("Updated live flag to true for: "+key);
							MetaServer.liveFlag.put(key, true);
						}
					} else if (obj instanceof sendingObject) {
						sendingObject sO = new sendingObject();
						sO = (sendingObject) obj;
						String chunk = sO.getChunkName();
						String file = sO.getFileName();
						for(int i = 0; i < MetaServer.map.get(file).size(); i++) {
							if(MetaServer.map.get(file).get(i).getChunkName().equals(chunk)) {
								int diff = MetaServer.map.get(file).get(i).getFileSize() - sO.getFileSize();
								sendingObject sONew = new sendingObject();
								sONew.setType("append");
								sONew.setWriteType("normal");
								sONew.setWriteNumOfBytes(diff);
								sONew.setChunkName(chunk);
								sONew.setFileName(file);
								Integer port = MetaServer.servers.get(hostname);
								Socket sock = new Socket(hostname, port);
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
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
