package com.mimidots.lab3;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.TreeMap;

public class ServerB {

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


    public static void main(String[] args) {

        int portServerB = 2500; //this server port

        //directory path for server B
        String directoryServerB = "/home/murage/Desktop/directory_b/";

        ServerB server = new ServerB();

        ServerSocket serverSocket = null;
        Socket socket = null; //Socket used for current server B and server A interaction
        DataOutputStream outputStream = null; //sending data from server B to A
        DataInputStream inputStream = null; //for receiving data from server A
        try {
            serverSocket = new ServerSocket(portServerB);

            System.out.println("Server B started. Ready to accept connections");
            System.out.println("Awaiting client to connect to continue");
            socket = serverSocket.accept();

            System.out.println("New client connected to server B with address: " + socket.getRemoteSocketAddress());

            System.out.println("Creating binary streams for the new connection");
            //create an output stream for the socket
            outputStream = new DataOutputStream(socket.getOutputStream());
            //create an input stream for the socket
            inputStream = new DataInputStream(socket.getInputStream());

            TreeMap<String, Object> response;

            while (socket.isConnected()) { //loop forever while we are connected to server B

                if (inputStream.available() == 0) { //if there is nothing to process, skip
                    continue;
                }

                response = server.deserializeData(inputStream);

                //1) Get directory listing request
                if (response.getOrDefault("command", "").equals(COMMAND.GETLISTING.name())) {
                    System.out.println("Received directory listing request");
                    //read the directory and return the response
                    TreeMap<String, Long[]> files = server.listDirectoryInventory(directoryServerB);

                    String data = server.serializeFiles(files); //serialize the files data into a predefined standard
                    System.out.println("Sending current directory listing. Serialized data size: " + data.length());
                    outputStream.write(
                            server.serializeData(COMMAND.FOLDERLISTING.name(), data.getBytes(), "", 0L));

                    System.out.println("Current directory listing sent");
                }

                //2) When server A requests for a file
                if (response.getOrDefault("command", "").equals(COMMAND.FILEREQUEST.name())) {
                    String filename = new String((byte[]) response.get("data"));
                    System.out.println("Received file download request for " + filename);

                    byte[] data = server.readFileContents(directoryServerB + filename);
                    outputStream.write(server.serializeData(COMMAND.FILEUPLOAD.name(), data, filename,
                            server.getFileTimestamp(directoryServerB + filename)));

                    System.out.println("File sent to the server");
                }

                //3) When server A uploads a file
                if (response.getOrDefault("command", "").equals(COMMAND.FILEUPLOAD.name())) {
                    System.out.println("Received new upload from server A");
                    server.writeFileContents(directoryServerB + response.get("filename"),
                            (byte[]) response.get("data"), (Long) response.get("timestamp"));
                    System.out.println("File successfully written to disk");
                }

                //4) When server A request for a file to be deleted
                if (response.getOrDefault("command", "").equals(COMMAND.FILEDELETE.name())) {
                    String filename = new String((byte[]) response.get("data"));
                    System.out.println("Received request to delete a file " + filename);
                    server.deleteFile(directoryServerB + filename);
                    System.out.println("File successfully deleted");
                }

                System.out.println(); //separate each cycle log
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Cleanup");


            try {
                if (outputStream != null) {

                    outputStream.close();
                }
                if (inputStream != null) {
                    inputStream.close();
                }
                if (socket != null) {
                    socket.close();
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
