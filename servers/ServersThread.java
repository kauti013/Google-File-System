package servers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import commons.sendingObject;

public class ServersThread implements Runnable{

	private ServerSocket serverSocket;
	private Integer port = null;

	public ServersThread(Integer port) {
		this.port = port;
	}
	
	public void run() {
		try {
			ServerSocket serv = new ServerSocket(port);
			while(true) {
				new Thread(new MultiServer(serv.accept())).start();
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	private static class MultiServer extends Thread implements Serializable {
		// private Servers s = new Servers();
		private Socket clientSocket;
		private OutputStream outStream;
		private ObjectOutputStream objOutStream;
		private InputStream inStream;
		private ObjectInputStream objInStream;
		static sendingObject sO = new sendingObject();
		String hostname = null;

		public MultiServer(Socket socket) {
			this.clientSocket = socket;
		}

		public void run() {
			
			try {
				hostname = clientSocket.getInetAddress().getHostName();
				System.out.println("The client connecting to me is: " + hostname);
				outStream = clientSocket.getOutputStream();
				objOutStream = new ObjectOutputStream(outStream);
				objOutStream.flush();
				inStream = clientSocket.getInputStream();
				objInStream = new ObjectInputStream(inStream);
				sO = (sendingObject) objInStream.readObject();

				String type = sO.getType().toUpperCase();
				System.out.println("Type is: " + sO.getType() + " Filename is: " + sO.getFileName());
				switch (type) {
				case "READ":
					System.out.println("\nInside " + type + " case");
					System.out.println("The chunk to read is:" + sO.getChunkName());
					File fileToRead = new File(
							"/home/010/j/ja/jas160630/AOS/" + Servers.selfName + "/" + sO.getChunkName());
					RandomAccessFile raf = new RandomAccessFile(fileToRead, "r");
					byte[] b = new byte[sO.getReadNumOfBytes()];
					raf.seek(sO.getUpdatedReadBytesFrom());
					System.out.println("No of bytes to read: " + sO.getReadNumOfBytes());
					System.out.println("Updated Bytes from: " + sO.getUpdatedReadBytesFrom());
					raf.readFully(b);
					String message = new String(b);
					System.out.println("Sending the text: " + message);
					sendingObject sONew = new sendingObject();
					sONew.setMessage(message);
					objOutStream.writeObject(sONew);
					objOutStream.flush();
					break;

				case "APPEND":
					System.out.println("\nInside " + type + " case");
					System.out.println("The chunk to write is:" + sO.getChunkName());
					Integer writeBytes = sO.getWriteNumOfBytes();
					String data = null;
					if(hostname.equals("dc41.utdallas.edu"))
						if(sO.getWriteType().equals("null"))
							data = createNullString(writeBytes);
						else
							data = createRandomString(writeBytes);
					else
						data = createRandomString(writeBytes);
					File fileToWrite = new File(
							"/home/010/j/ja/jas160630/AOS/" + Servers.selfName + "/" + sO.getChunkName());
					FileWriter fw = null;
					BufferedWriter bw = null;
					PrintWriter out = null;
					try {
						fw = new FileWriter(fileToWrite, true);
						bw = new BufferedWriter(fw);
						out = new PrintWriter(bw);
						out.println(data);
						out.close();
					} catch (IOException e) {
						// exception handling left as an exercise for the reader
					} finally {
						if (out != null)
							out.close();
						try {
							if (bw != null)
								bw.close();
						} catch (IOException e) {
							// exception handling left as an exercise for the
							// reader
						}
						try {
							if (fw != null)
								fw.close();
						} catch (IOException e) {
							// exception handling left as an exercise for the
							// reader
						}
					}
					sO.setMessage("The file " + sO.getFileName() + "has been written the following data: " + data);
					objOutStream.writeObject(sO);
					break;

				case "CREATE":
					if (!Servers.map.containsKey(sO.getFileName())) {
						System.out.println(sO.getFileName());
						ArrayList<sendingObject> list = new ArrayList<>();
						sendingObject sOCreate = new sendingObject();
						sOCreate.setFileSize(sO.getFileSize());
						sOCreate.setFileName(sO.getFileName());
						sOCreate.setChunkName(sO.getChunkName());
						sOCreate.setServerName(sO.getServerName());
						list.add(sOCreate);
						Servers.map.put(sO.getFileName(), list);
						System.out.println(Servers.map.keySet());
						StringBuilder sb = new StringBuilder();
						sb.append("/home/010/j/ja/jas160630/AOS/");
						sb.append(Servers.selfName);
						sb.append("/");
						sb.append(sO.getChunkName());
						String filepath = sb.toString();
						System.out.println("New file created is: " + filepath);
						File file = new File(filepath);
						if (file.getParentFile().mkdir()) {
							file.createNewFile();
						} else {
							throw new IOException("Failed to create directory " + file.getParent());
						}
					} else {
						ArrayList<sendingObject> list = Servers.map.get(sO.getFileName());
						sendingObject sOCreate = new sendingObject();
						sOCreate.setFileSize(sO.getFileSize());
						sOCreate.setFileName(sO.getFileName());
						sOCreate.setChunkName(sO.getChunkName());
						sOCreate.setServerName(sO.getServerName());
						list.add(sOCreate);
						Servers.map.put(sO.getFileName(), list);
						System.out.println(Servers.map.keySet());
						StringBuilder sb = new StringBuilder();
						sb.append("/home/010/j/ja/jas160630/AOS/");
						sb.append(Servers.selfName);
						sb.append("/");
						sb.append(sO.getChunkName());
						String filepath = sb.toString();
						System.out.println("New file created is: " + filepath);
						File file = new File(filepath);
						if (file.getParentFile().mkdir()) {
							file.createNewFile();
						} else {
							throw new IOException("Failed to create directory " + file.getParent());
						}
					}
					break;

				default:
					System.out.println("something went wrong in case selection");
					break;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		private String createRandomString(Integer writeBytes) {
			StringBuilder sb = new StringBuilder(writeBytes);
			for (int i = 0; i < writeBytes; i++) {
				sb.append('a');			
			}
			return sb.toString();
		}
		
		private String createNullString(Integer writeBytes){
			StringBuilder sb = new StringBuilder(writeBytes);
			for (int i = 0; i < writeBytes; i++) {
				sb.append('\0');			
			}
			return sb.toString();
		}
	}
}
