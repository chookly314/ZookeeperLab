package es.upm.dit.cnvr.lab1;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.io.IOException;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

// This is a simple application for detecting the correct processes using ZK. 
// Several instances of this code can be created. Each of them detects the 
// valid numbers.

// Two watchers are used:
// - cwatcher: wait until the session is created. 
// - watcherMember: notified when the number of members is updated

// the method process has to be created for implement Watcher. However
// this process should never be invoked, as the "this" watcher is used

public class zkLeader implements Watcher {
	private static final int SESSION_TIMEOUT = 5000;

	private static String rootMembers = "/members";
	private static String aMember = "/member-";
	private String myId;

	// Leader
	private String groupLeader = "";
	// This param determines if every time a process dies, all the remaining processes are notified (besides the new leader)
	private static final boolean NOTIFYNEWLEADERTOALLPROCESSES = true;

	// This is static. A list of zookeeper can be provided for decide where to
	// connect
	String[] hosts = { "127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181" };

	private ZooKeeper zk;

	public zkLeader() {

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
				}
			}
		} catch (Exception e) {
			System.out.println("Error");
		}

		// Add the process to the members in zookeeper

		if (zk != null) {
			// Create a folder for members and include this process/server
			try {
				// Create a folder, if it is not created
				String response = new String();
				Stat s = zk.exists(rootMembers, false); // this);
				if (s == null) {
					// Created the znode, if it is not created.
					response = zk.create(rootMembers, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println(response);
				}

				// Create a znode for registering as member and get my id
				myId = zk.create(rootMembers + aMember, new byte[0], Ids.OPEN_ACL_UNSAFE,
						CreateMode.EPHEMERAL_SEQUENTIAL);
				myId = myId.replace(rootMembers + "/", "");
				System.out.println("Created zNode member id:" + myId);

				// Get current leader
				leaderElection();
				// printListMembers(list);
			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failed. Closing...");
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
			leaderElection();
		}
	};

	@Override
	public void process(WatchedEvent event) {
		try {
			System.out.println("Unexpected invocated this method. Process of the object");
			List<String> list = zk.getChildren(rootMembers, watcherMember); // this);
			printListMembers(list);
		} catch (Exception e) {
			System.out.println("Unexpected exception. Process of the object");
		}
	}

	// Changed to support leader election
	private void printListMembers(List<String> list) {
		System.out.println("Remaining # members:" + list.size());
		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			System.out.print(string + ", ");				
		}
		System.out.println();
	}

	private synchronized void leaderElection() {
		try {
			Stat s = zk.exists(rootMembers, false);
			System.out.println("Update!");
			// Get current leader
			List<String> list = zk.getChildren(rootMembers, false, s);
			groupLeader = getNewLeader(list);
			System.out.println();
			System.out.println("The leader zNode is " + groupLeader);
			printListMembers(list);
			if (NOTIFYNEWLEADERTOALLPROCESSES) {
				zk.getChildren(rootMembers, watcherMember, s);
			} else {
				// Get zNode to watch (instead of setting a watcher on /members, to avoid herding effect)
				String zNodeToWatch = getNewNodeToWatch(list);
				if (!(zNodeToWatch.equals(myId))) {
					zk.getChildren(rootMembers + "/" + zNodeToWatch, false, s);					
				}
				zk.getChildren(rootMembers + "/" + zNodeToWatch, watcherMember, s);				
			}
		} catch (Exception e) {
			System.out.println("Exception: wacherMember");
		}
	}
	
	private synchronized String getNewLeader(List<String> list) {
		Long leader = 0L;
		String leaderStr = "";
		Pattern p = Pattern.compile("(?<=member-)\\d{10}");
		boolean firstTime = true;

		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
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
		return leaderStr;
		// System.out.println("The zNode owner is " + zk.getData(rootMembers + "/" +
		// leaderStr, null, zk.exists(rootMembers, null) ));
	}

	private synchronized String getNewNodeToWatch(List<String> list) {
		Collections.sort(list);
		int index = list.indexOf(myId);
		return (index > 0) ? list.get(index - 1) : list.get(index);
	}

	public static void main(String[] args) {
		zkLeader zk = new zkLeader();
		try {
			Thread.sleep(300000);
		} catch (Exception e) {
		}
	}
}
