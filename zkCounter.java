package es.upm.dit.cnvr.lab1;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Random;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.spi.RootLogger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

// This class is a modification of zkMember. It creates threads (or processes, depending on the executing env) that write
// or read a shared lock through Zookeeper.

public class zkCounter implements Watcher {

	private static final int SESSION_TIMEOUT = 5000;

	private static int sharedCounter = 0;

	private static String rootMembers = "/members";
	private static String aMember = "/members-";
	private static String lockMembers = "/lock";
	private static String aLockMember = "/lock-";

	private String myId;
	private String myIdLockRead;
	private String myIdLockWrite;

	// Locks for Read/Write ids
	private static boolean readLockFree = true;
	private static boolean writeLockFree = true;

	// Make the processes to sleep for a bit when they have a lock, so that
	// concurrency tests are easier to perform.
	private static final boolean SLEEP = false;

	// This is static. A list of zookeeper can be provided for decide where to
	// connect
	String[] hosts = { "127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181" };

	private ZooKeeper zk;

	public zkCounter() {

		// Select a random zookeeper server
		Random rand = new Random();
		int i = rand.nextInt(hosts.length);

		// Create a session and wait until it is created.
		// When is created, the watcher is notified
		try {
			if (zk == null) {
				zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, cWatcher);
				try {
					// Wait for creating the session. Use the object lock
					wait();
					// zk.exists("/",false);
				} catch (Exception e) {
					// TODO: handle exception
				}
			}
		} catch (Exception e) {
			System.out.println("Error");
		}

		// Add the process to the members in zookeeper

		if (zk != null) {
			// Create a folder for members&locks and include this process/server in members
			// only
			try {
				// Create a folder, if it is not created
				String response = new String();
				Stat s = zk.exists(rootMembers, watcherMember); // this);
				if (s == null) {
					// Create the zNode, if it is not created.
					response = zk.create(rootMembers, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println(response);
				}

				// Create a zNode for registering as member and get my id
				myId = zk.create(rootMembers + aMember, new byte[0], Ids.OPEN_ACL_UNSAFE,
						CreateMode.EPHEMERAL_SEQUENTIAL);

				myId = myId.replace(rootMembers + "/", "");

				List<String> list = zk.getChildren(rootMembers, watcherMember, s); // this, s);
				System.out.println("Created znode nember id:" + myId);
				printListMembers(list);

				// Create a folder for locks, if it is not created
				String responseLocks = new String();
				Stat sLocks = zk.exists(lockMembers, false); // this);
				if (sLocks == null) {
					// Create the zNode, if it is not created.
					responseLocks = zk.create(lockMembers, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					// Save the value
					try {
						zk.setData(lockMembers, String.valueOf(sharedCounter).getBytes("UTF-8"),
								zk.exists(lockMembers, false).getVersion());
					} catch (UnsupportedEncodingException e) {
					}
					System.out.println(responseLocks);
				}

			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failed. Closing.");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised.");
			}

		}
	}

	// Notified when the session is created
	private Watcher cWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			System.out.println("Session created.");
			System.out.println(e.toString());
			notify();
		}
	};

	// Notified when the number of children in /member is updated
	private Watcher watcherMember = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("------------------Watcher Member------------------\n");
			try {
				System.out.println("        Update!!");
				List<String> list = zk.getChildren(rootMembers, watcherMember); // this);
				printListMembers(list);
			} catch (Exception e) {
				System.out.println("Exception: watcherMember");
			}
		}
	};

	@Override
	public void process(WatchedEvent event) {
		try {
			System.out.println("Unexpected invocated this method. Process of the object.");
			List<String> list = zk.getChildren(rootMembers, watcherMember); // this);
			printListMembers(list);
		} catch (Exception e) {
			System.out.println("Unexpected exception. Process of the object.");
		}
	}

	private void printListMembers(List<String> list) {
		System.out.println("Remaining # members:" + list.size());

		Long leader = 0L;
		String leaderStr = "";
		Pattern p = Pattern.compile("(?<=member-)\\d{10}");
		boolean firstTime = true;

		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			System.out.print(string + ", ");
			Matcher m = p.matcher(string);
			if (firstTime) {
				if (m.find()) {
					leader = Long.parseLong(m.group(0));
					leaderStr = string;
				}
			}
			firstTime = false;
			if (m.find() && Long.parseLong(m.group(0)) < leader) {
				leader = Long.parseLong(m.group(0));
				leaderStr = string;
			}
		}
		System.out.println();
		// System.out.println("The zNode owner is " + zk.getData(rootMembers + "/" +
		// leaderStr, null, zk.exists(rootMembers, null) ));
	}

	// ----------
	// Most of the changes start here

	// Notified when the number of children in /locks is updated, this means we are
	// waiting to hold the lock for reading
	private Watcher watcherLocksReading = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("------------------Watcher Locks Reading------------------\n");
			try {
				// System.out.println(" Update!!");
				List<String> list = zk.getChildren(lockMembers, false);
				// String newHolder = getNewLockHolder(list);
				String nodeToWatch = getNewNodeToWatch(list, true);
				// System.out.println("The new holder for reading is " + newHolder);
				if (nodeToWatch.equals(myIdLockRead)) {
					readCounter(nodeToWatch);
				} else {
					try {
						list = zk.getChildren(lockMembers + "/" + nodeToWatch, watcherLocksReading);
					} catch (KeeperException e) {
						list = zk.getChildren(lockMembers, false);
						nodeToWatch = getNewNodeToWatch(list, true);
						if (nodeToWatch.equals(myIdLockRead)) {
							readCounter(nodeToWatch);
						}
					}
				}
			} catch (Exception e) {
				System.out.println("Exception: watcherLocksReading");
			}
		}
	};

	// Notified when the number of children in /locks is updated, this means we are
	// waiting to hold the lock for writting
	private Watcher watcherLocksWritting = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("------------------Watcher Locks Writting------------------\n");
			try {
				// System.out.println(" Update!!");
				List<String> list = zk.getChildren(lockMembers, false);
				// String newHolder = getNewLockHolder(list);
				String nodeToWatch = getNewNodeToWatch(list, false);
				// System.out.println("The new holder for writting is " + newHolder);
				if (nodeToWatch.equals(myIdLockWrite)) {
					writeCounter(nodeToWatch);
				} else {
					try {
						list = zk.getChildren(lockMembers + "/" + nodeToWatch, watcherLocksWritting);
					} catch (KeeperException e) {
						list = zk.getChildren(lockMembers, false);
						nodeToWatch = getNewNodeToWatch(list, false);
						if (nodeToWatch.equals(myIdLockWrite)) {
							writeCounter(nodeToWatch);
						}
					}
				}
			} catch (Exception e) {
				System.out.println("Exception: watcherLocksWritting");
			}
		}
	};

	private synchronized void getCounter() {
		try {
			// Create a zNode for registering as lock and get my id
			myIdLockRead = zk.create(lockMembers + aLockMember, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL);
			myIdLockRead = myIdLockRead.replace(lockMembers + "/", "");
			List<String> list = zk.getChildren(lockMembers, false);
			// String newHolder = getNewLockHolder(list);
			String nodeToWatch = getNewNodeToWatch(list, true);
			// System.out.println("The new holder for reading is " + newHolder);
			if (nodeToWatch.equals(myIdLockRead)) {
				readCounter(nodeToWatch);
			} else {
				try {
					list = zk.getChildren(lockMembers + "/" + nodeToWatch, watcherLocksReading);
				} catch (KeeperException e) {
					list = zk.getChildren(lockMembers, false);
					nodeToWatch = getNewNodeToWatch(list, true);
					if (nodeToWatch.equals(myIdLockRead)) {
						readCounter(nodeToWatch);
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private synchronized void readCounter(String nodeToWatch) {
		System.out.println("I am the new holder for reading! (" + nodeToWatch + ")");
		byte[] result = null;
		try {
			result = zk.getData(lockMembers, false, zk.exists(lockMembers, false));
			if (result != null) {
				sharedCounter = Integer.parseInt(new String(result, "UTF-8"));
			}
			System.out.println("The value of the counter after reading is " + sharedCounter);
			if (SLEEP) {
				System.out.println("Sleeping for 5 seconds...");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			zk.delete(lockMembers + "/" + myIdLockRead, 0);
			readLockFree = true;
			System.out.println("Read operation completed!\n");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private synchronized void  incrementCounter() {
		try {
			// Create a zNode for registering as lock and get my id
			myIdLockWrite = zk.create(lockMembers + aLockMember, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL);
			myIdLockWrite = myIdLockWrite.replace(lockMembers + "/", "");

			List<String> list = zk.getChildren(lockMembers, false);
			String nodeToWatch = getNewNodeToWatch(list, false);
			// System.out.println("The new holder for writting is " + newHolder);
			if (nodeToWatch.equals(myIdLockWrite)) {
				writeCounter(nodeToWatch);
			} else {
				try {
					list = zk.getChildren(lockMembers + "/" + nodeToWatch, watcherLocksWritting);
				} catch (KeeperException e) {
					list = zk.getChildren(lockMembers, false);
					nodeToWatch = getNewNodeToWatch(list, false);
					if (nodeToWatch.equals(myIdLockWrite)) {
						writeCounter(nodeToWatch);
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private synchronized void writeCounter(String nodeToWatch) throws Exception {
		System.out.println("I am the new holder for writting! (" + nodeToWatch + ")");
		// Get the data
		byte[] result = null;
		try {
			result = zk.getData(lockMembers, false, zk.exists(lockMembers, false));
		} catch (Exception e) {
		}
		if (result != null) {
			sharedCounter = Integer.parseInt(new String(result, "UTF-8"));
		}
		// Update the data
		Stat stat = zk.exists(lockMembers, false);
		sharedCounter++;
		zk.setData(lockMembers, String.valueOf(sharedCounter).getBytes("UTF-8"), stat.getVersion());
		System.out.println("The value of the counter after writting is " + sharedCounter);
		if (SLEEP) {
			System.out.println("Sleeping for 5 seconds...");
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		zk.delete(lockMembers + "/" + myIdLockWrite, 0);
		writeLockFree = true;
		System.out.println("Write operation completed!\n");
	}

	private synchronized String getNewNodeToWatch(List<String> list, boolean read) {
		Collections.sort(list);
		System.out.println("List is " + list.toString() + " and I am for reading" + myIdLockRead + " and for writting " + myIdLockWrite);
		if (read) {
			int index = list.indexOf(myIdLockRead);
			return (index > 0) ? list.get(index - 1) : list.get(index);
		} else {
			int index = list.indexOf(myIdLockWrite);
			return (index > 0) ? list.get(index - 1) : list.get(index);
		}
	}

	private String getNewLockHolder(List<String> list) {
		Long holder = 0L;
		String holderStr = "";
		Pattern p = Pattern.compile("(?<=lock-)\\d{10}");
		boolean firstTime = true;

		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			System.out.print(string + ", ");
			Matcher m = p.matcher(string);
			if (firstTime) {
				if (m.find()) {
					holder = Long.parseLong(m.group(0));
					holderStr = string;
				}
			}
			firstTime = false;
			if (m.find() && Long.parseLong(m.group(0)) < holder) {
				holder = Long.parseLong(m.group(0));
				holderStr = string;
			}
		}
		return holderStr;
	}

	public int getCounterVariable() {
		return sharedCounter;
	}

	public static void main(String[] args) {
		zkCounter zkCount = new zkCounter();
		System.out.println("Accepted commands: get, increment or exit.");
		
		
		while (true) {
			Scanner scanner = new Scanner(System.in);
			 String command = scanner.nextLine();
			 if (command.equals("get")) {
			 if (readLockFree) {
			 readLockFree = false;
			 System.out.println("Read lock acquired!");
			 zkCount.getCounter();
			 }
			 } else if (command.equals("increment")) {
			 if (writeLockFree) {
			 writeLockFree = false;
			 System.out.println("Write lock acquired!");
			 zkCount.incrementCounter();
			 }
			 } else if (command.equals("exit")) {
			 System.out.println("Exiting!");
			 return;
			 } else {
			 System.out.println("Wrong command. Try again with get, increment or exit.");
			 }
//			double rand = Math.random();
//			if (rand >= 0.5) {
//				if (readLockFree) {
//					readLockFree = false;
//					System.out.println("Read lock acquired!");
//					zkCount.getCounter();
//					System.out.println("-------- sharedCounter value is " + sharedCounter + " --------");
//				}
//			} else {
//				if (writeLockFree) {
//					writeLockFree = false;
//					System.out.println("Write lock acquired!");
//					zkCount.incrementCounter();
//					System.out.println("-------- sharedCounter value is " + sharedCounter + " --------");
//				}
//			}
//			try {
//				Thread.sleep(1);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		}
	}
}
