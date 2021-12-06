package com.mimidots.lab2;
import java.io.*;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TreeMap;

public class Client {

    static TreeMap<String, Long[]> deserializeFiles(String data) {
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

            //the key represent the filename while value= file length at index 0 and timestamp at index 2
            fileMap.put(f[0], new Long[]{Long.valueOf(f[1]), Long.valueOf(f[2])});
        }
        return fileMap;
    }

    static void print(TreeMap<String, Long[]> files) {
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:m");
        for (String filename : files.keySet()) {
            Date date = new Date(files.get(filename)[1]);

            System.out.println(filename + " " + files.get(filename)[0] / 1000 + "kB" + " " + sdf.format(date));
        }

    }

    public static void main(String[] args) {

        String serverAddress = "localhost";
        int serverPort = 8080;

        Socket socket = null;
        DataInputStream inputStream = null; //for receiving data from server
        try {
            System.out.println("Establishing connection to server A at " + serverAddress + ":" + serverPort);

            socket = new Socket(serverAddress, serverPort);
            System.out.println("Connected to Server A");

            //Input stream to read any bytes stream received from server
            inputStream = new DataInputStream(socket.getInputStream());


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
                    TreeMap<String, Long[]> files = deserializeFiles(new String((byte[]) data));

                    if (files.size() == 0) {
                        System.out.println("Directories empty. Skipping..");
                    } else {
                        System.out.println();
                        //print the data to console
                        print(files);
                    }
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
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
