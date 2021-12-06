**Setup Steps**
1) Ensure you have JDK 15+
2) Set server A directory. It must end with a trailing slash
3) Set server B directory. It must end with a trailing slash
4) Launch server B
5) Launch server A
6) Launch the client

7) Client contains the logic to accept command line arguments to lock or unlock a file.
	(Compile it first, then call it with `-lock -<index>` or `-unlock -<index>`

	If client is launched with command line arguments, it sends the commands to server A and
	waits for file listing. The lock/unlock should reflect on all available clients. Check server A logs
	on how the server processes the logic.

N.B:
ConcurrentHashMap is used for global maps (for up-to-date list of locked files and last synchronized file lists)
because other maps are not thread safe. The FIFO queue is implemented using LinkedBlockingDeque.


