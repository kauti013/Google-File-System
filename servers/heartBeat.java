package servers;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import commons.sendingObject;

public class heartBeat implements Runnable, Serializable {

	private int DEFAULT_SAMPLING_PERIOD = 5000; // seconds
	private String DEFAULT_NAME = "HeartbeatAgent";
	// private HashMap<Integer, Object> values; // <id, value>
	private Servers s = new Servers();
	private String key;
	private Integer value;
	private OutputStream outStream;
	private ObjectOutputStream objOutStream;
	public static String flag = null;
	private Socket clientSocket = null;

	public heartBeat() throws UnknownHostException, IOException {
		// values = new HashMap<Integer,Object>();
		// for (Map.Entry<String, Integer> entry :
		// Servers.metaserver.entrySet()) {
		// key = entry.getKey();
		// value = entry.getValue();
		// Socket clientSocket = new Socket(key,value);
		// outStream = clientSocket.getOutputStream();
		// objOutStream = new ObjectOutputStream(outStream);
		// }
		// System.out.println("Done");
	}

	// private void collect() {
	// /**
	// * Here you should collect the data you want to send and store it in the
	// * hash
	// **/
	// }
	public void run() {
		for (Map.Entry<String, Integer> entry : Servers.metaserver.entrySet()) {
			key = entry.getKey();
			value = entry.getValue();
			System.out.println("Key is "+key+" value is: "+value);
			try {
				clientSocket = new Socket(key, 45000);
				outStream = clientSocket.getOutputStream();
				objOutStream = new ObjectOutputStream(outStream);
				objOutStream.flush();
				System.out.println("printing object output stream: "+objOutStream);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("This is the fault");
				e.printStackTrace();
			}
		}
		System.out.println("Done");
		System.out.println("Running " + DEFAULT_NAME);
		try {
			while (true) {
				if(flag.contains("UP")) {
					System.out.println("printing flag: "+flag);
					System.out.println("Thread: " + DEFAULT_NAME + ", " + "I'm alive");

					// this.collect();
					objOutStream.reset();
					this.sendData();
					// Let the thread sleep for a while.
					Thread.sleep(DEFAULT_SAMPLING_PERIOD);
				}
				else if(flag.contains("DOWN")){
					System.out.println("printing flag: "+flag);
					objOutStream.writeObject("ServerDown");
					Thread.sleep(2000);
				}
				else if(flag.contains("RE")){
					if(!Servers.map.isEmpty()) {
						Set<String> list = Servers.map.keySet();
						for(String file : list) {
							sendingObject sO = new sendingObject();
							int size = Servers.map.get(file).size()-1;
							sO = Servers.map.get(file).get(size);
							objOutStream.writeObject(sO);
						}
					}						
					flag = "UP";
					System.out.println("################Sending ServerUP to MetaServer from RE############");
					objOutStream.writeObject("ServerUp");
					objOutStream.flush();					
				}
			}
		} catch (InterruptedException | IOException e) {
			System.out.println("Thread " + DEFAULT_NAME + " interrupted.");
		}
		System.out.println("Thread " + DEFAULT_NAME + " exiting.");
	}

	public synchronized void sendData() throws UnknownHostException, IOException {
		/**
		 * Here you should send the data to the server. Use REST/SOAP/multicast
		 * messages, whatever you want/need/are forced to
		 **/
//		if(!Servers.map.isEmpty()){
			System.out.println(Servers.map.keySet());
			System.out.println("Sending Server.map: "+Servers.map);
			objOutStream.writeObject(Servers.map);
			objOutStream.flush();
//		}
//		else
//			System.out.println("empty");
	}
}