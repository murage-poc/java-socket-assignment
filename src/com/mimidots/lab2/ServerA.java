package com.mimidots.lab2;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.TreeMap;

public class ServerA {
    //Last synchronized file list <filename,[size,last-modified]> - global so it can be reused between the runs
    TreeMap<String, Long[]> lastSyncedFileList = new TreeMap<>();

    static enum COMMAND {
        GETLISTING, //request for directory listing data
        FOLDERLISTING, //response with directory listing data
        FILEUPLOAD, //sending file to a server (upload)
        FILEREQUEST, //requesting for a file (download)
        FILEDELETE, //requesting for a file to be deleted
    }

    public void deleteFile(String path) {
        File file = new File(path);
        //ensure the file exists and is a file
        if (file.exists() && file.isFile()) {
            file.delete();
            System.out.println("File deleted successfully");
        }
    }

    public void writeFileContents(String path, byte[] content, long timestamp) throws IOException {

        File file = new File(path);

        try (FileOutputStream stream = new FileOutputStream(file)) {
            stream.write(content);
            file.setLastModified(timestamp);
        }
    }

    public byte[] readFileContents(String path) throws IOException {
        File file = new File(path);
        if (!file.exists() && file.canRead()) {
            System.out.println("Cannot locate or read the file: " + path);
        }

        return Files.readAllBytes(Paths.get(path));
    }

    public long getFileTimestamp(String path) {
        File file = new File(path);
        if (!file.exists() && file.canRead()) {
            System.out.println("Cannot locate or read the file: " + path);
        }
        return file.lastModified();
    }

    public TreeMap<String, Long[]> listDirectoryInventory(String path) {
        File folder = new File(path);
        File[] fileList = folder.listFiles();

        TreeMap<String, Long[]> fileMap = new TreeMap<>();
        for (File file : fileList) {
            fileMap.put(file.getName(), new Long[]{file.length(), file.lastModified()});
        }
        return fileMap;
    }


    public String serializeFiles(TreeMap<String, Long[]> files) {
        StringBuilder builder = new StringBuilder();
        String delimiter = ":"; //we will use a delimiter than cannot conflict with filename

        for (String filename : files.keySet()) {
            builder.append(filename + delimiter + files.get(filename)[0] + delimiter + files.get(filename)[1] + "\n");
        }
        return builder.toString();
    }

    public TreeMap<String, Long[]> deserializeFiles(String data) {
        String metaDelimiter = ":";
        TreeMap<String, Long[]> fileMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);


        //split by end of line to get each file representation
        String[] fileMeta = data.split("\n");


        //for each file meta, expand it
        for (String meta : fileMeta) {
            String[] f = meta.split(metaDelimiter);
            //the key represent the filename while value= file length at index 0 and timestamp at index 2
            fileMap.put(f[0], new Long[]{Long.valueOf(f[1]), Long.valueOf(f[2])});
        }
        return fileMap;
    }

    /**
     * @param command   Command being sent
     * @param data      The content of the data in bytes (array byte)
     * @param filename  The filename if content of data is a file
     * @param timestamp The file timestamp if content of data is a file
     * @return A serialized array byte of data
     */
    public byte[] serializeData(String command, byte[] data, String filename, Long timestamp) throws IOException {
        char headersSeparator = ':';
        char headersEndSeparator = '\n';

        long size = 0;
        long lastModified = 0;
        if (data != null) {
            size = data.length;
        }
        if (timestamp != null) {
            lastModified = timestamp;
        }

        /*
        COMMAND:SIZE:FILENAME:TIMESTAMP\rDATA
         */

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(command.getBytes());
        stream.write(headersSeparator);
        stream.write(Long.toString(size).getBytes());
        stream.write(headersSeparator);
        stream.write(filename.getBytes());
        stream.write(headersSeparator);
        stream.write(Long.toString(lastModified).getBytes());
        stream.write(headersEndSeparator);
        if (data != null) {
            stream.write(data);
        }
        return stream.toByteArray();
    }

    /**
     * @param stream Data input stream to read from. Should not be null
     * @return
     * @throws IOException
     */
    public TreeMap<String, Object> deserializeData(DataInputStream stream) throws IOException {
        TreeMap<String, Object> content = new TreeMap<>();
        char headersSeparator = ':';
        char headersEndSeparator = '\n';

        if (stream == null || stream.available() == 0) { //if there is no data at the moment, return empty map
            return content;
        }

        char c;

        //1. Extract command
        StringBuilder commandBuilder = new StringBuilder();
        while ((c = (char) stream.read()) != headersSeparator) {
            commandBuilder.append(c);
        }

        if (!commandBuilder.isEmpty()) {
            content.put("command", commandBuilder.toString());
            System.out.println("New command received: " + commandBuilder);
        }

        //2. Extract size (can be zero)
        StringBuilder sizeBuilder = new StringBuilder();
        while ((c = (char) stream.read()) != headersSeparator) {
            sizeBuilder.append(c);
        }

        long contentSize = Long.valueOf(sizeBuilder.toString());
        if (!sizeBuilder.isEmpty()) {
            content.put("size", contentSize);
            System.out.println("Received data of size: " + sizeBuilder);

        }

        //3. Extract filename (can be empty)
        StringBuilder filename = new StringBuilder();
        while ((c = (char) stream.read()) != headersSeparator) {
            filename.append(c);
        }

        if (!filename.isEmpty()) {
            System.out.println("Received file: " + filename);
            content.put("filename", filename.toString());
        }

        //4. Extract timestamp (can be zero)
        StringBuilder timestamp = new StringBuilder();
        while ((c = (char) stream.read()) != headersEndSeparator) {
            timestamp.append(c);

        }

        if (!timestamp.isEmpty()) {
            content.put("timestamp", Long.valueOf(timestamp.toString()));
        }

        //5.Extract the data (can be empty)

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        while (contentSize != byteArrayOutputStream.size()) {
            byteArrayOutputStream.write(stream.readByte());
        }

        content.put("data", byteArrayOutputStream.toByteArray());

        return content;
    }


    /**
     * Get the directory listing of server B
     *
     * @param streamIn  Server B input stream
     * @param streamOut Server B output stream
     * @return returns directory listing in a treemap with filenames as keys
     */
    public TreeMap<String, Long[]> getServerBInventory(DataInputStream streamIn, DataOutputStream streamOut) {
        try {
            System.out.println("Requesting server B inventory");
            streamOut.write(this.serializeData(COMMAND.GETLISTING.name(), null, "", 0L));

            TreeMap<String, Object> response = new TreeMap<>();

            while (!response.getOrDefault("command", "").equals(COMMAND.FOLDERLISTING.name())) {
                response = this.deserializeData(streamIn);
            }

            System.out.println("Received directory listing from server B");

            String data = new String((byte[]) response.get("data"));
            //if there is no data, return an empty treemap
            if (data.equals("")) {
                System.out.println("Server B directory has no files to deserialize");
                return new TreeMap<>();
            }
            return deserializeFiles(data);

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

    }

    /**
     * Requests for inventory of server B and synchronizes with that of server A (current)
     *
     * @param directoryServerA :Server A directory
     * @return inventory :Unsorted recently synchronized composite directory listing
     */
    public TreeMap<String, Long[]> synchronizeInventories(String directoryServerA, DataInputStream inStream, DataOutputStream outStream) throws IOException {


        //1) Get server B directory contents.
        TreeMap<String, Long[]> inventoryB = this.getServerBInventory(inStream, outStream);


        //2) read server A inventory
        TreeMap<String, Long[]> inventoryA = this.listDirectoryInventory(directoryServerA);


        //3 if directory A and B are empty, return
        if (inventoryA.size() == 0 && inventoryB.size() == 0) {
            System.out.println("Both directories in server A and B are empty. Skipping synchronization");
            return inventoryA;
        }

        //if last synced file list is empty, set it to server B contents
        if (lastSyncedFileList.isEmpty()) {
            this.lastSyncedFileList = inventoryB;
        }

        //4) Check which files to swap or delete (A recent file has the latest timestamp)

        System.out.println("Comparing files from server B and that of A");
        TreeMap<String, Long[]> filesOnAToDelete = new TreeMap<>();
        TreeMap<String, Long[]> filesOnBToDelete = new TreeMap<>();
        TreeMap<String, Long[]> filesOnAToUpload = new TreeMap<>();
        TreeMap<String, Long[]> filesOnBToDownload = new TreeMap<>();

        //Check Server B directory discrepancies
        for (String filename : inventoryB.keySet()) {
            Long metaA[] = inventoryA.get(filename);
            Long metaB[] = inventoryB.get(filename);
            if (!inventoryA.containsKey(filename) && !this.lastSyncedFileList.containsKey(filename)) {
                //if file is not in A and we didn't have it on last synced file list, set it for download from B to A
                filesOnBToDownload.put(filename, inventoryB.get(filename));
                this.lastSyncedFileList.put(filename,inventoryB.get(filename)); //add as a placeholder

            } else if (!inventoryA.containsKey(filename) && this.lastSyncedFileList.containsKey(filename)) {
                //if file is not in A and we have it on last synced file list, set it for deletion
                filesOnBToDelete.put(filename, inventoryB.get(filename));

            } else if (metaB[1] > metaA[1]) {
                //if the file in server B is more recent set the file for download
                filesOnBToDownload.put(filename, metaB);
            }

        }

        //Check Server A directory discrepancies
        for (String filename : inventoryA.keySet()) {
            if (!inventoryB.containsKey(filename) && !this.lastSyncedFileList.containsKey(filename)) {
                //if file is not in B, but we didn't have it on last synced file list, set it for upload from A to B
                filesOnAToUpload.put(filename, inventoryA.get(filename));

                this.lastSyncedFileList.put(filename,inventoryA.get(filename)); //add as a placeholder

            } else if (!inventoryB.containsKey(filename) && this.lastSyncedFileList.containsKey(filename)) {
                //if file is not in B, but we have it on last synced file list, set it for deletion
                filesOnAToDelete.put(filename, inventoryA.get(filename));
            } else {
                Long metaA[] = inventoryA.get(filename);
                Long metaB[] = inventoryB.get(filename);

                if ((metaA[1] > metaB[1])) {
                    //if a file in server A is more recent than that in B, set it for upload
                    filesOnAToUpload.put(filename, metaA);
                }
            }
        }

        //5) Delete outdated files locally
        for (String filename : filesOnAToDelete.keySet()) {
            System.out.println("Deleting outdated file on server A " + filename);
            this.deleteFile(directoryServerA + filename);
            //update last file synced list
            this.lastSyncedFileList.remove(filename);
        }

        //Delete outdated files on server B
        for (String filename : filesOnBToDelete.keySet()) {
            System.out.println("Sending request to delete outdated file on server B " + filename);
            outStream.write(this.serializeData(COMMAND.FILEDELETE.name(), filename.getBytes(), "", 0L));
            //update last file synced list
            this.lastSyncedFileList.remove(filename);
        }


        //6) Uploads files that need modification (if available)
        for (String filename : filesOnAToUpload.keySet()) {
            System.out.println("Uploading file from server A to B ");
            byte[] data = readFileContents(directoryServerA + filename);
            Long timestamp = this.getFileTimestamp(directoryServerA + filename);
            outStream.write(this.serializeData(COMMAND.FILEUPLOAD.name(), data, filename, timestamp));

            //update last file synced list
            this.lastSyncedFileList.replace(filename, new Long[]{(long) data.length, timestamp});
        }

        //7) Request for any outdated or missing files
        for (String filename : filesOnBToDownload.keySet()) {
            System.out.println("Requesting for file from server B " + filename);
            outStream.write(this.serializeData(COMMAND.FILEREQUEST.name(), filename.getBytes(), "", 0L));

            System.out.println("Awaiting for server B to upload " + filename);
            TreeMap<String, Object> response = new TreeMap<>();

            while (!response.getOrDefault("command", "").equals(COMMAND.FILEUPLOAD.name())) {
                response = this.deserializeData(inStream);
            }
            //write the response into a file
            writeFileContents(directoryServerA + response.get("filename"),
                    (byte[]) response.get("data"), (Long) response.get("timestamp"));

            //update last file synced list
            this.lastSyncedFileList.replace(filename, new Long[]{(Long) response.get("size"), (Long) response.get("timestamp")});

        }
        System.out.println("Files synchronization in current cycle done");

        //8) Return the composite synchronized inventory list
        return this.lastSyncedFileList;
    }


    public static void main(String[] args) {
        int portServerA = 8080; //this server port

        //directory path for server A
        String directoryServerA = "/home/murage/Desktop/directory_a/";

        int portServerB = 2500; //port for server B
        String addressServerB = "localhost"; // address for server B

        ServerA server = new ServerA();

        ServerSocket serverSocket = null;
        Socket clientSocket = null; //Socket used for client and server interaction
        Socket clientSocketB = null; //Socket used for server A and server B interaction

        DataOutputStream clientOutputStream = null; //for sending data to client

        DataOutputStream serverBOutputStream = null; //for sending data to server B
        DataInputStream serverBInputStream = null; //for receiving data from server B

        try {
            serverSocket = new ServerSocket(portServerA);
            System.out.println("Server A started. Ready to accept connections");

            clientSocket = serverSocket.accept();

            System.out.println("New client connected to server A with address: " + clientSocket.getRemoteSocketAddress());

            //set client output stream (writer)
            clientOutputStream = new DataOutputStream(clientSocket.getOutputStream());

            //1) Connect to server B
            System.out.println("Connecting to server B");
            clientSocketB = new Socket(addressServerB, portServerB);
            System.out.println("Connection to server B established");

            System.out.println("Creating binary streams for server B connection");
            serverBOutputStream = new DataOutputStream(clientSocketB.getOutputStream());
            serverBInputStream = new DataInputStream(clientSocketB.getInputStream());


            TreeMap<String, Long[]> inventory;

            while (clientSocketB.isConnected()) {  //while we are connected to server B

                //2) Synchronize the directory listings
                inventory = server.synchronizeInventories(directoryServerA, serverBInputStream, serverBOutputStream);

                //3) Send the data to the client
                String clientData = server.serializeFiles(inventory);
                clientOutputStream.writeBytes(clientData.length() + "\n" + clientData);

                //sleep for 5 seconds
                Thread.sleep(5000);
                System.out.println(); //separate each cycle log
            }

        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Cleanup");
            try {
                if (clientOutputStream != null) {
                    clientOutputStream.close();
                }
                if (clientSocket != null) {
                    clientSocket.close();
                }
                if (serverBOutputStream != null) {
                    serverBOutputStream.close();
                }
                if (serverBInputStream != null) {
                    serverBInputStream.close();
                }
                if (clientSocketB != null) {
                    clientSocketB.close();
                }
                if (serverSocket != null) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
