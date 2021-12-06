package com.mimidots.lab3;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class ServerA {
    //Last synchronized file list <filename,[size,last-modified,locked]> - global to ease by different threads
    ConcurrentHashMap<String, Long[]> lastSyncedFileList = new ConcurrentHashMap<>();

    // LOCKED Files <fileIndex,Changes[action(delete,create),filename,timestamp,file-data]> -global to ease client threads utilization
    ConcurrentHashMap<Integer, LinkedBlockingDeque<Object[]>> lockedFiles = new ConcurrentHashMap<>();

    /**
     * SUPPORTED COMMANDS FOR IDENTIFYING TYPE OF MESSAGES(request and response)
     */
    static enum COMMAND {
        GETLISTING, //request for directory listing data
        FOLDERLISTING, //response with directory listing data
        FILEUPLOAD, //sending file to a server (upload)
        FILEREQUEST, //requesting for a file (download)
        LOCKFILE,
        UNLOCKFILE,
        FILEDELETE, //requesting for a file to be deleted
    }


    public List<Map.Entry<String, Long[]>> sort(ConcurrentHashMap<String, Long[]> map) {
        List<Map.Entry<String, Long[]>> list = new ArrayList<>(map.entrySet());

        list.sort((k1, k2) -> k1.getKey().toLowerCase().compareTo(k2.getKey().toLowerCase()));
        return list;
    }

    public String getFilenameByIndex(int index) {
        //sort the global last synced file list
        List<Map.Entry<String, Long[]>> list = sort(this.lastSyncedFileList);
        //return the key of the sorted list == filename
        return list.get(index).getKey();
    }

    public int getFilenameIndex(String filename) {
        //sort the global last synced file list
        List<Map.Entry<String, Long[]>> list = sort(this.lastSyncedFileList);
        //return the key of the sorted list == filename
        for (int i = 0; i < list.size(); i++) {
            if ((list.get(i).getKey()).equals(filename)) {
                return i;
            }
        }
        return -1;
    }

    public void deleteFile(String path) {
        File file = new File(path);
        //ensure the file exists and is a file
        if (file.exists() && file.isFile()) {
            file.delete();
            System.out.println("File deleted successfully");
        } else {
            System.out.println("File doesn't seem to exist or is not a file");
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
            //name as the key, [file-size,file-timestamp,file-lock-flag]
            fileMap.put(file.getName(), new Long[]{file.length(), file.lastModified(), 0L});
        }
        return fileMap;
    }


    /**
     * Serializes a mapped data into a string that can be deserialized later
     * A single file string output format is
     * FILENAME:FILELENGTH:FILETIMESTAMP:IFFILEISLOCKEDFLAG
     *
     * @param files A map of files metadata
     * @return serialized string data
     */
    public String serializeFiles(TreeMap<String, Long[]> files) {
        StringBuilder builder = new StringBuilder();
        String delimiter = ":"; //we will use a delimiter than cannot conflict with filename

        for (String filename : files.keySet()) {
            builder.append(filename + delimiter + files.get(filename)[0] + delimiter +
                    files.get(filename)[1] + delimiter + files.get(filename)[2] + "\n");
        }
        return builder.toString();
    }

    /**
     * Deserializes a string into a mapped data
     * A single mapped file output contains
     * filename as key
     * Array of long containing [file-length, file-timestamp,if-file-is-locked-flag]
     *
     * @param data: A serialized string data
     * @return a map of deserialized files metadata
     */
    public TreeMap<String, Long[]> deserializeFiles(String data) {
        String metaDelimiter = ":";
        TreeMap<String, Long[]> fileMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);


        //split by end of line to get each file representation
        String[] fileMeta = data.split("\n");


        //for each file meta, expand it
        for (String meta : fileMeta) {
            String[] f = meta.split(metaDelimiter);
            //the key represent the filename
            fileMap.put(f[0], new Long[]{Long.valueOf(f[1]), Long.valueOf(f[2]), Long.valueOf(f[3])});
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
     * Deserializes data from an input stream
     *
     * @param stream Data input stream to read from. Should not be null
     * @return Mapped data with key as filename
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
     * @param directory :Server A directory
     * @return inventory :Unsorted recently synchronized composite directory listing
     */
    public ConcurrentHashMap<String, Long[]> syncInventories(String directory, DataInputStream inStream, DataOutputStream outStream) throws IOException {


        //1) Get server B directory contents.
        TreeMap<String, Long[]> inventoryB = this.getServerBInventory(inStream, outStream);

        //2) read server A inventory
        TreeMap<String, Long[]> inventoryA = this.listDirectoryInventory(directory);


        //3 if directory A and B are empty, return
        if (inventoryA.size() == 0 && inventoryB.size() == 0) {
            System.err.println("Both directories in server A and B are empty. Skipping synchronization");
            return this.lastSyncedFileList;
        }

        //if last synced file list is empty,
        if (this.lastSyncedFileList.isEmpty()) {
            this.lastSyncedFileList.putAll(inventoryB); //since B file list is the source of truth
        }

        //4) Check which files are outdated

        System.out.println("Comparing files from server B and that of A");
        TreeMap<String, Long[]> filesFromAToDownload = new TreeMap<>();
        TreeMap<String, Long[]> filesFromAToUpload = new TreeMap<>();
        TreeMap<String, Long[]> filesOnAToDelete = new TreeMap<>();
        TreeMap<String, Long[]> filesOnBToDelete = new TreeMap<>();

        //Check Server B directory discrepancies
        for (String filename : inventoryB.keySet()) {

            if (!inventoryA.containsKey(filename) && !this.lastSyncedFileList.containsKey(filename)) {
                //if the file is not in A and also on last synced files, set it to be downloaded
                filesFromAToDownload.put(filename, inventoryB.get(filename));

                this.lastSyncedFileList.put(filename, inventoryB.get(filename)); //add as a placeholder
            } else if (!inventoryA.containsKey(filename) && this.lastSyncedFileList.containsKey(filename)) {
                //if file is only in B and is on last synced file list, set it to be deleted
                filesOnBToDelete.put(filename, inventoryB.get(filename));
            } else {
                Long[] metaA = inventoryA.get(filename);
                Long[] metaB = inventoryB.get(filename);

                if (metaB[1] > metaA[1]) {
                    //if the file in server B is more recent, set the file for download
                    filesFromAToDownload.put(filename, metaB);
                }
            }
        }

        //Check Server A directory discrepancies
        for (String filename : inventoryA.keySet()) {

            if (!inventoryB.containsKey(filename) && !this.lastSyncedFileList.containsKey(filename)) {

                //if the file is not in B and also on last synced files, set it to be downloaded
                filesFromAToUpload.put(filename, inventoryA.get(filename));

                this.lastSyncedFileList.put(filename, inventoryA.get(filename)); //add as a placeholder

            } else if (!inventoryB.containsKey(filename) && this.lastSyncedFileList.containsKey(filename)) {
                //if the file is not in B, but is on last synced files, set it to be deleted

                filesOnAToDelete.put(filename, inventoryA.get(filename));
            } else {
                Long metaA[] = inventoryA.get(filename);
                Long metaB[] = inventoryB.get(filename);

                //if file in A is more recent, set it for upload to server B
                //NOTE: locked file should not be modified on directory A
                if (metaA[1] > metaB[1]) {
                    filesFromAToUpload.put(filename, metaA);
                }
            }
        }


        //6) Request for any outdated or missing files from server B
        for (String filename : filesFromAToDownload.keySet()) {
            //check if file exist on locked files
            int index = getFilenameIndex(filename);
            if (this.lockedFiles.containsKey(index)) {
                LinkedBlockingDeque<Object[]> modifications = this.lockedFiles.get(index);
                //check if last modifications  match current

                if (!modifications.isEmpty()) { //in-case there are no actions
                    //check if the file has already been queued, skip
                    Object[] lastAction = modifications.peekLast();

                    long newSize = filesFromAToDownload.get(filename)[0];
                    long oldSize = (Long) lastAction[4];

                    long newTimestamp = filesFromAToDownload.get(filename)[1];
                    long oldTimestamp = (Long) lastAction[2];

                    //check if size && timestamp are equal where action is create
                    if (lastAction[0].equals("create") && (newSize == oldSize && newTimestamp == oldTimestamp)) {
                        System.out.println("Skipping. Similar file update/creation already queued");
                        continue;
                    }
                }
            }

            System.out.println("Requesting for file from server B " + filename);

            outStream.write(this.serializeData(COMMAND.FILEREQUEST.name(), filename.getBytes(), "", 0L));

            System.out.println("Awaiting for server B to upload " + filename);

            TreeMap<String, Object> response = new TreeMap<>();

            while (!response.getOrDefault("command", "").equals(COMMAND.FILEUPLOAD.name())) {
                response = this.deserializeData(inStream);
            }

            if (this.lockedFiles.containsKey(index)) {
                System.out.println("File has been locked. Queuing changes");

                //Add the creation/modification request to the Queue

                LinkedBlockingDeque<Object[]> modifications = this.lockedFiles.get(index);

                System.out.println("Queuing the changes of the locked file " + filename);
                //add object of : action=create, filename, new timestamp,file array bytes
                modifications.add(new Object[]{"create", filename, response.get("timestamp"), response.get("data"), response.get("size")});
                this.lockedFiles.replace(index, modifications);

            } else {
                System.out.println("File not locked. Update the changes.");

                byte[] data = (byte[]) response.get("data");
                Long timestamp = (Long) response.get("timestamp");
                //write the response into a file
                writeFileContents(directory + response.get("filename"), data, timestamp);

                //update the synced metadata
                this.lastSyncedFileList.replace(filename, new Long[]{(long) data.length, timestamp, 0L});

            }

        }

        //7) Upload any new files from server A to B
        for (String filename : filesFromAToUpload.keySet()) {
            System.out.println("Uploading file from server A to B ");
            byte[] data = readFileContents(directory + filename);
            Long timestamp = this.getFileTimestamp(directory + filename);
            outStream.write(this.serializeData(COMMAND.FILEUPLOAD.name(), data, filename, timestamp));
            //update the synced metadata
            this.lastSyncedFileList.replace(filename, new Long[]{(long) data.length, timestamp, 0L});
        }

        //8 Delete file on A
        for (String filename : filesOnAToDelete.keySet()) {
            System.out.println("Deleting outdated file on server A " + filename);
            this.deleteFile(directory + filename);
            //update last file synced list
            this.lastSyncedFileList.remove(filename);
        }

        //Send request to delete files on B
        for (String filename : filesOnBToDelete.keySet()) {
            System.out.println("Sending request to delete outdated file on server B " + filename);
            outStream.write(this.serializeData(COMMAND.FILEDELETE.name(), filename.getBytes(), "", 0L));
            //update last file synced list
            this.lastSyncedFileList.remove(filename);
        }

        System.out.println("Files synchronization in current cycle done");

        //) Return the composite synchronized inventory list
        return this.lastSyncedFileList;
    }

    /**
     * Process queued file changes and then remove it from lockedFiles map
     *
     * @param fileIndex index of the file as specified by user.
     */
    public void processQueuedChanges(String serverRoot, int fileIndex) {
        System.out.println("Processing queued file changes");
        //get the modifications
        LinkedBlockingDeque<Object[]> modifications = this.lockedFiles.get(fileIndex);

        System.out.println("Queued changes total " + modifications.size());

        Iterator<Object[]> iterator = modifications.iterator();
        String filename = null;
        Object[] modification = null;
        while (iterator.hasNext()) {
            modification = iterator.next();
            filename = (String) modification[0];
            if (modification[0] == "delete") {
                System.out.println("Processing queued deletion request for " + filename + " with timestamp: " + modification[2]);
                this.deleteFile(serverRoot + filename);

            } else {
                System.out.println("Processing writing to file " + filename + " with timestamp: " + modification[2]);
                try {
                    this.writeFileContents(serverRoot + filename, (byte[]) modification[3], (Long) modification[2]);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }

        //remove the file from locked list
        this.lockedFiles.remove(fileIndex);
        if (filename == null) { //in case there were no queued requests
            filename = getFilenameByIndex(fileIndex);
        }
        //if the last action was deletion, the remove it from global map
        if (modification != null && modification[0] == "delete") {
            //remove from global file list
            this.lastSyncedFileList.remove(filename);
        } else {
            //else, remove locked flag
            Long[] metadata = this.lastSyncedFileList.get(filename);
            metadata[2] = 0L;
            this.lastSyncedFileList.replace(filename, metadata);
        }
    }

    public void clientHandler(Socket socket, String directory, DataInputStream inStream, DataOutputStream outStream) {

        //set client output stream (writer)
        DataOutputStream clientOutputStream = null;
        DataInputStream clientInputStream = null;
        try {
            clientOutputStream = new DataOutputStream(socket.getOutputStream());

            //set client input stream (writer)
            clientInputStream = new DataInputStream(socket.getInputStream());

            while (true) {

                //1) check if client sent a message
                if (clientInputStream.available() > 0) {
                    TreeMap<String, Object> res = this.deserializeData(clientInputStream);


                    if (res.getOrDefault("command", "").equals(COMMAND.LOCKFILE.name())) {
                        //if the user want to lock a file
                        int fileIndex = Integer.parseInt(new String((byte[]) res.get("data")));
                        System.out.println("Received a new request to lock a file at index " + fileIndex);

                        //if their index is out of range, send response
                        if (fileIndex >= this.lastSyncedFileList.size()) {
                            System.err.println("File index out of range.\n");
                            continue; //don't execute the rest of logic
                        }

                        String filename = this.getFilenameByIndex(fileIndex);

                        //check if its already locked
                        if (this.lockedFiles.containsKey(fileIndex)) {
                            System.err.println("File already locked " + filename);
                        } else {
                            //lock the file
                            LinkedBlockingDeque<Object[]> modifications = new LinkedBlockingDeque<>();
                            this.lockedFiles.put(fileIndex, modifications);
                            //set the lock flag
                            Long[] metadata = this.lastSyncedFileList.get(filename);
                            metadata[2] = 1L;
                            this.lastSyncedFileList.replace(filename, metadata);

                            System.out.println("File locked successfully " + filename);
                        }

                    } else if (res.getOrDefault("command", "").equals(COMMAND.UNLOCKFILE.name())) {
                        //if the user want to unlock a file
                        int fileIndex = Integer.parseInt(new String((byte[]) res.get("data")));

                        System.out.println("Received request to unlock file on index " + fileIndex);

                        //if their index is out of range, send response
                        if (fileIndex >= this.lastSyncedFileList.size()) {
                            System.err.println("File index out of range");
                            continue; //don't execute the rest of loop logic
                        }

                        //check if file is in the lock
                        if (!this.lockedFiles.containsKey(fileIndex)) {
                            System.err.println("File not locked. You requested to unlock file that is not locked ");
                        } else {
                            this.processQueuedChanges(directory, fileIndex);
                            System.out.println("File at index " + fileIndex + " unlocked");
                        }
                    }
                }

                //2) Synchronize the directory listings
                List<Map.Entry<String, Long[]>> sortedList = sort(this.syncInventories(directory, inStream, outStream));

                //client expect treemap, so:
                TreeMap<String, Long[]> inventory = new TreeMap<>();
                for (Map.Entry<String, Long[]> entry : sortedList) {
                    inventory.put(entry.getKey(), entry.getValue());
                }

                //3) Send the data to the client
                String clientData = this.serializeFiles(inventory);
                clientOutputStream.writeBytes(clientData.length() + "\n" + clientData);

                //sleep for 5 seconds
                Thread.sleep(5000);
                System.out.println(); //separate each cycle log
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {

                if (clientOutputStream != null) {
                    clientOutputStream.close();
                }
                if (socket != null) {
                    socket.close();
                }
                if (clientOutputStream != null) {
                    clientOutputStream.close();
                }
                if (clientInputStream != null) {
                    clientInputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
        int portServerA = 8080; //this server port

        //directory path for server A
        final String serverDirectory = "/home/murage/Desktop/directory_a/";

        int portServerB = 2500; //port for server B
        String addressServerB = "localhost"; // address for server B

        ServerA server = new ServerA();

        ServerSocket serverSocket = null;
        Socket clientSocketB = null; //Socket used for server A and server B interaction


        try {
            //1) Connect to server B
            System.out.println("Connecting to server B");
            clientSocketB = new Socket(addressServerB, portServerB);
            System.out.println("Connection to server B established");

            System.out.println("Creating binary streams for server B connection");

            //setup input stream for receiving data from server B
            final DataOutputStream serverBOutputStream = new DataOutputStream(clientSocketB.getOutputStream());
            //setup input stream for sending data from server B
            final DataInputStream serverBInputStream = new DataInputStream(clientSocketB.getInputStream());

            serverSocket = new ServerSocket(portServerA);
            System.out.println("Server A started. Ready to accept connections");

            //2) Launch server and start accepting connections (unlimited clients)
            while (true) {
                final Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected to server A with address: " + clientSocket.getRemoteSocketAddress());

                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        server.clientHandler(clientSocket, serverDirectory, serverBInputStream, serverBOutputStream);
                    }
                }).start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Cleanup");
            try {
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
