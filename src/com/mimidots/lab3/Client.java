package com.mimidots.lab3;

import java.io.*;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TreeMap;

public class Client {

    static enum COMMAND {
        LOCKFILE,
        UNLOCKFILE
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

    TreeMap<String, Long[]> deserializeFiles(String data) {
        String fileMetaDelimiter = ":";
        TreeMap<String, Long[]> fileMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        //if data is null, return empty treemap
        if (data.equals("")) {
            return fileMap;
        }

        //split by end of line to get each file representation
        String[] fileMeta = data.split("\n");

        //for each file meta, expand it
        for (String meta : fileMeta) {
            String[] f = meta.split(fileMetaDelimiter);
            //the key represent the filename and, long contains:  file-length, file-timestamp,is-file-locked-flag
            fileMap.put(f[0], new Long[]{Long.valueOf(f[1]), Long.valueOf(f[2]), Long.valueOf(f[3])});
        }
        return fileMap;
    }

    void print(TreeMap<String, Long[]> files) {
        int index = 0;
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:m");
        for (String filename : files.keySet()) {
            Long[] metadata = files.get(filename);
            Date date = new Date(metadata[1]);
            //check if file is locked
            String locked = "<locked>";

            //zero represent it's not locked while  1 is locked
            if (metadata[2] == 0) {
                locked = "";
            }

            System.out.println(index + " " + filename + " " + metadata[0] / 1000 + "kB" + " " + sdf.format(date) + "  " + locked);

            ++index;
        }

    }

    public static void main(String[] args) {

        String serverAddress = "localhost";
        int serverPort = 8080;

        Socket socket = null;
        DataInputStream inputStream = null; //for receiving data from server
        DataOutputStream outputStream = null;
        Client client = new Client();

        try {
            String command = "";
            String index = "";

            //if this was run with command line arguments,
            if (args.length == 2 && (args[0].equals("-lock") || args[0].equals("-unlock"))) {
                //extract action
                String action = args[0].replace("-", "");
                command = COMMAND.LOCKFILE.name();
                if (action.equals("unlock")) {
                    command = COMMAND.UNLOCKFILE.name();
                }

                //process the index
                index = args[1].replace("-", "");
                try {
                    //ensure it's a number and positive one
                    if (Integer.parseInt(index) < 0) {
                        throw new NumberFormatException();
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Invalid index argument. Index should be a valid list index");
                    System.exit(1);
                }

               // System.exit(0);
            }

            //establish a connection to the server
            System.out.println("Establishing connection to server A at " + serverAddress + ":" + serverPort);

            socket = new Socket(serverAddress, serverPort);
            System.out.println("Connected to Server A");

            //Input stream to read any bytes stream received from server
            inputStream = new DataInputStream(socket.getInputStream());

            //Input stream to send data stream received to the server
            outputStream = new DataOutputStream(socket.getOutputStream());

            //if a command to lock file was issued, send it to the server immediately
            if (!command.equals("") && !index.equals("")) {
                System.out.println("Sending your request to " + command + " file to server A");

                //send the command
                outputStream.write(client.serializeData(command, index.getBytes(), "", 0L));

                System.out.println("Request sent to the server A");

            }

            while (socket.isConnected()) { //disconnect only if explicitly disconnected

                if (inputStream.available() == 0) {
                    //if there is no data to process, skip all the logic below
                    continue;
                }

                char c;
                StringBuilder sizeBuilder = new StringBuilder();
                while ((c = (char) inputStream.read()) != '\n') {
                    sizeBuilder.append(c);
                }
                if (!sizeBuilder.isEmpty()) {
                    System.out.println("Received data from server A");

                    //read all the data
                    byte[] data = inputStream.readNBytes(Integer.parseInt(sizeBuilder.toString()));
                    TreeMap<String, Long[]> files = client.deserializeFiles(new String((byte[]) data));

                    if (files.size() == 0) {
                        System.out.println("Directories empty. Skipping..");
                    } else {
                        System.out.println();
                        //print the data to console
                        client.print(files);
                        System.out.println();
                    }
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


    }
}
