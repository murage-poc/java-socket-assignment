**Setup Steps**
1) Ensure you have JDK 15+
2) Set server A directory. It must end with a trailing slash
3) Set server B directory. It must end with a trailing slash
4) Launch server B
5) Launch server A
6) Launch the client


** Server to server communication **

- Server A can send and receive data from server B and vice versa
- Data is sent as binary due to differences in file encodings
- Due to the variation of the data being transmitted across,
   this has been standardized to fit the following format:

COMMAND:SIZE:FILENAME:TIMESTAMP\rDATA

i.e.
Command - what kind of message/request is this. The available commands are defined in an enum.
Size -the size of data. Size can be zero if there is no data
Filename - the name of the file (if the data is file content)
Timestamp - last file modification date (if the data is a file content)
Data    - Any data ranging from a file to a directory list

I am considering command, size, filename, and timestamp as headers. Headers are separated by the ``:`` character
since the character cannot exist in a filename. A new line marks the header's end.


**Considerations**

- Used treemap which
    - has better memory management than hashmap
    - the number of files that can resize
    - has automatic sorting based on key. Key is the filename in this case
- A file with the same size but different modification date is treated as dirty i.e.
 we replace it with the most recent one
- If a file exists on server A but not in server B, it is deleted.
- It's not clear how long the client connection should be kept alive. Currently, it closes after receiving the first synced directories metadata


Synchronization logic

1) If a file exists only on directory_a and did not exist on immediate previous cycle, copy it to directory_b
2) If a file exists only on directory_b and did not exist on immediate previous cycle, copy it to directory_a
3) If a file in directory_b is more recent, copy it to directory_a
4) If a file in directory_a is more recent, copy it to directory_b
5) If a file exists only on directory_a and was on immediate previous cycle, delete it
5) If a file exists only on directory_b and was on immediate previous cycle, delete it
