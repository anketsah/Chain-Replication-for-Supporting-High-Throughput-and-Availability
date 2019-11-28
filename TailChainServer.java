package edu.sjsu.cs249.chain;
import java.util.LinkedList;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class TailChainServer extends TailChainReplicaGrpc.TailChainReplicaImplBase implements Watcher {

	private Server server;
	private int port;

	private Map<String, Integer> map = new HashMap<>();
	private LinkedList<TailStateUpdateRequest> sentList = new LinkedList<>();

	private boolean isHead = false;
	private boolean isTail = false;
	private boolean isConnected = false; // connected to ZooKeeper

	private ZooKeeper zk;
	private long sessionID; // this server's current sessionID with ZooKeeper
	private long prevChainID; // sessionID of predecessor in chain
	private long nextChainID;
	private long chainTailID;
	private TailChainReplicaGrpc.TailChainReplicaBlockingStub prevChain;
	private TailChainReplicaGrpc.TailChainReplicaBlockingStub nextChain;
	private TailChainReplicaGrpc.TailChainReplicaBlockingStub chainTail;
	String chainRoot = "/tailchain";
	private Map<String, TailClientGrpc.TailClientBlockingStub> clientMap = new HashMap<>();
	private String host;

	private AtomicBoolean retryChainCheck = new AtomicBoolean(); // true when node list updated
	private long currentXid = 0; // current xid number server has seen so far

	public TailChainServer(String zkServer, String zroot, String host, int port) throws IOException {
		this.port = port;
		this.chainRoot = zroot;
		this.host = host;
		server = ServerBuilder.forPort(port).addService(this).build();
		zk = new ZooKeeper(zkServer, 3000, this);
	}

	private void setPrevChain(long session) throws KeeperException, InterruptedException {
		if (prevChainID == session) {
			return;
		}
		prevChain = null; // getStub could throw exception
		prevChain = getStub(session);
		prevChainID = session;
	}

	private void setNextChain(long session) throws KeeperException, InterruptedException {
		if (nextChainID == session) {
			return;
		}
		nextChain = null;
		nextChain = getStub(session);
		nextChainID = session;

		TailStateTransferRequest request = TailStateTransferRequest.newBuilder().setSrc(sessionID).setStateXid(currentXid).putAllState(map).addAllSent(sentList).build();
		ChainResponse chainResponse = nextChain.stateTransfer(request);
	}

	private void setChainTail(long session) throws KeeperException, InterruptedException {
		if (chainTailID == session) {
			return;
		}
		chainTail = null;
		chainTail = getStub(session);
		chainTailID = session;
	}

	private TailChainReplicaGrpc.TailChainReplicaBlockingStub getStub(long session) throws KeeperException, InterruptedException {
		byte data[] = zk.getData(chainRoot + "/" + Long.toHexString(session), false, null);
		InetSocketAddress addr = str2addr(new String(data).split("\n")[0]);
		ManagedChannel channel = ManagedChannelBuilder.forAddress(addr.getHostName(), addr.getPort()).usePlaintext().build();
		return TailChainReplicaGrpc.newBlockingStub(channel);
	}

	private static InetSocketAddress str2addr(String addr) {
		int colon = addr.lastIndexOf(':');
		return new InetSocketAddress(addr.substring(0, colon), Integer.parseInt(addr.substring(colon + 1)));
	}

	// Check if connections with others in chain changed when ZooKeeper nodes updated
	private void checkConnections() {
		try {
			List<String> children = zk.getChildren(chainRoot, true);
			ArrayList<Long> sessions = new ArrayList<>();
			for (String child : children) {
				try {
					sessions.add(Long.parseLong(child, 16));
				} catch (NumberFormatException e) {
					// just skip non numbers
				}
			}
			sessions.sort(Long::compare);

			// Check if head of chain
			if (sessionID == sessions.get(0)) {
				isHead = true;
			} else {
				isHead = false;
			}

			// Check if tail or set connection with tail
			long tail = sessions.get(children.size() - 1);
			if (sessionID == tail) {
				isTail = true;
			} else {
				isTail = false;
			}
			setChainTail(tail);

			// Set connections with predecessor and successor nodes
			int index = sessions.indexOf(sessionID);
			if (index > 0) {
				setPrevChain(sessions.get(index - 1));
			} else {
				prevChain = null;
			}

			if (index < sessions.size() - 1) {
				setNextChain(sessions.get(index + 1));
			} else {
				nextChain = null;
			}

			System.out.println("---");
			System.out.println("Check for Head: " + isHead + ", head id " + Long.toHexString(sessions.get(0)));
			System.out.println("Check for Tail: " + isTail + ", tail id " + Long.toHexString(chainTailID));
			if (prevChain != null) {
				System.out.println("Previous in the chain: " + Long.toHexString(prevChainID));
			}
			if (nextChain != null) {
				System.out.println("Next in the chain: " + Long.toHexString(nextChainID));
			}
		} catch (KeeperException | InterruptedException e) {
			retryChainCheck.set(true);
		}
	}

	@Override
	public void process(WatchedEvent watchedEvent) {
		// Lost connection with ZooKeeper, abort
		if (watchedEvent.getState().equals(Event.KeeperState.Expired) || watchedEvent.getState().equals(Event.KeeperState.Closed)) {
			System.err.println("Disconnected");
			System.exit(2);
		}
		// Finished connecting to ZooKeeper
		if (watchedEvent.getState().equals(Event.KeeperState.SyncConnected) && !isConnected) {
			isConnected = true;
			sessionID = zk.getSessionId();
			String hexSessionID = Long.toHexString(sessionID);
			System.out.println("Connection established to ZooKeeper with the session id: " + hexSessionID);

			// create ZooKeeper node with session id and containing address and port of this grpc server
			try {
//				String host = InetAddress.getLocalHost().getHostAddress() + ":" + port;
				String ip = host + ":" + port;
				byte[] data = ip.getBytes();
				zk.create(chainRoot + "/" + hexSessionID, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			} catch (Exception e) {
				e.printStackTrace();
			}

			// occasionally check if connections need updating
			new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(
					() -> {
						if (retryChainCheck.getAndSet(false)) checkConnections();
					},
					5,
					5,
					TimeUnit.SECONDS
			);

			// occasionally check if sent list can be cleaned
			new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(
					() -> cleanSentList(),
					10,
					10,
					TimeUnit.SECONDS
			);
		}

		checkConnections();
	}

	// Asks tail for latest processed xid and clears requests in sent list up to that xid
	private void cleanSentList() {
		if (sentList.size() > 0) {
			System.out.println("Emptying present list: " + sentList.size());
			LatestXidRequest request = LatestXidRequest.newBuilder().build();
			LatestXidResponse response = chainTail.getLatestXid(request);

			System.out.println(response.getRc() + " " + response.getXid());

			if (response.getRc() == 0) {
				long xid = response.getXid();
				while (!sentList.isEmpty() && sentList.peek().getXid() <= xid) {
					sentList.pop();
				}
			}
			System.out.println("Current sent list size: " + sentList.size());
		}
	}

	@Override
	public void proposeStateUpdate(TailStateUpdateRequest request, StreamObserver<ChainResponse> responseObserver) {
		long src = request.getSrc();
		boolean isFromPrev = src == prevChainID;

		if (isFromPrev) {
			long xid = request.getXid();
			String key = request.getKey();
			int value = request.getValue();
			String host = request.getHost();
			int port = request.getPort();
			int cxid = request.getCxid();
			System.out.printf(" State Update Request for tail with xid %d with key '%s' and value %d\n",
					xid, key, value);

			map.put(key, value);
			currentXid = xid;

			//reply to client
			if (isTail) {
				// store the client stub for future use
				if (!clientMap.containsKey(host + ":" + port)) {
					ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
					clientMap.put(host + ":" + port, TailClientGrpc.newBlockingStub(channel));
				}
				CxidProcessedRequest cxidRequest = CxidProcessedRequest.newBuilder().setCxid(cxid).build();
				ChainResponse chainResponse = clientMap.get(host + ":" + port).cxidProcessed(cxidRequest);
			}
			//pass on state update
			else {
				TailStateUpdateRequest updateRequest = TailStateUpdateRequest.newBuilder().setSrc(sessionID)
						.setXid(currentXid).setKey(key).setValue(map.get(key)).setHost(host).setPort(port).setCxid(cxid).build();

				ChainResponse chainResponse = nextChain.proposeStateUpdate(updateRequest);
				sentList.add(updateRequest);
			}

			ChainResponse response = ChainResponse.newBuilder().setRc(0).build();
			responseObserver.onNext(response);
		} else {
			System.out.println("Improper Request ");
			ChainResponse response = ChainResponse.newBuilder().setRc(1).build();
			responseObserver.onNext(response);
		}

		responseObserver.onCompleted();
	}

	@Override
	public void getLatestXid(LatestXidRequest request, StreamObserver<LatestXidResponse> responseObserver) {
		if (isTail) {
			System.out.println("Present Xid Request ");
			LatestXidResponse response = LatestXidResponse.newBuilder().setRc(0).setXid(currentXid).build();
			responseObserver.onNext(response);
		} else {
			System.out.println("Improper Request ");
			LatestXidResponse response = LatestXidResponse.newBuilder().setRc(1).setXid(0).build();
			responseObserver.onNext(response);
		}

		responseObserver.onCompleted();
	}

	@Override
	public void stateTransfer(TailStateTransferRequest request, StreamObserver<ChainResponse> responseObserver) {
		long src = request.getSrc();
		boolean isFromPrev = src == prevChainID;
		System.out.println("State transfer request started...");
		if (isFromPrev) {
			long stateXid = request.getStateXid();
			Map<String, Integer> mapState = request.getStateMap();
			List<TailStateUpdateRequest> updateRequests = request.getSentList();

			System.out.println("State transfer received ");

			// Copy map contents
			for (String key : mapState.keySet()) {
				map.put(key, mapState.get(key));
			}

			// Handle update requests from predecessor
			for (TailStateUpdateRequest updateRequest : updateRequests) {
				long xid = updateRequest.getXid();
				String key = updateRequest.getKey();
				int value = updateRequest.getValue();
				String host = updateRequest.getHost();
				int port = updateRequest.getPort();
				int cxid = updateRequest.getCxid();

				// ignore requests we've seen
				if (xid > currentXid) {
					currentXid = xid;

					//reply to client
					if (isTail) {
						// store the client stub for future use
						if (!clientMap.containsKey(host + ":" + port)) {
							ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
							clientMap.put(host + ":" + port, TailClientGrpc.newBlockingStub(channel));
						}
						CxidProcessedRequest cxidRequest = CxidProcessedRequest.newBuilder().setCxid(cxid).build();
						ChainResponse chainResponse = clientMap.get(host + ":" + port).cxidProcessed(cxidRequest);
					}
					//pass on state update
					else {
						TailStateUpdateRequest nextUpdateRequest = TailStateUpdateRequest.newBuilder().setSrc(sessionID)
								.setXid(currentXid).setKey(key).setValue(map.get(key)).setHost(host).setPort(port).setCxid(cxid).build();

						ChainResponse chainResponse = nextChain.proposeStateUpdate(nextUpdateRequest);
						sentList.add(nextUpdateRequest);
					}
				}
			}

			ChainResponse response = ChainResponse.newBuilder().setRc(0).build();
			responseObserver.onNext(response);
		} else {
			System.out.println("Improper Request ");
			ChainResponse response = ChainResponse.newBuilder().setRc(1).build();
			responseObserver.onNext(response);
		}

		responseObserver.onCompleted();
	}

	@Override
	public void increment(TailIncrementRequest request, StreamObserver<HeadResponse> responseObserver) {
		if (isHead) {
			String key = request.getKey();
			int incrValue = request.getIncrValue();
			String host = request.getHost();
			int port = request.getPort();
			int cxid = request.getCxid();
			System.out.printf("Increment Request for key '%s' with value %d from %s:%d with cxid %d\n",
					key, incrValue, host, port, cxid);

			int value = 0;
			if (map.containsKey(key)) {
				value = map.get(key);
			}
			map.put(key, value + incrValue);
			currentXid++;

			// send update to successor if not tail
			if (!isTail) {
				TailStateUpdateRequest updateRequest = TailStateUpdateRequest.newBuilder().setSrc(sessionID)
						.setXid(currentXid).setKey(key).setValue(map.get(key)).setHost(host).setPort(port).setCxid(cxid).build();

				ChainResponse chainResponse = nextChain.proposeStateUpdate(updateRequest);
				sentList.add(updateRequest);
			}
			// respond to client
			else {
				// store the client stub for future use
				if (!clientMap.containsKey(host + ":" + port)) {
					ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
					clientMap.put(host + ":" + port, TailClientGrpc.newBlockingStub(channel));
				}
				CxidProcessedRequest cxidRequest = CxidProcessedRequest.newBuilder().setCxid(cxid).build();
				ChainResponse chainResponse = clientMap.get(host + ":" + port).cxidProcessed(cxidRequest);
			}

			HeadResponse response = HeadResponse.newBuilder().setRc(0).build();
			responseObserver.onNext(response);
		} else {
			System.out.println("Improper request for increment");
			HeadResponse response = HeadResponse.newBuilder().setRc(1).build();
			responseObserver.onNext(response);
		}

		responseObserver.onCompleted();

	}

	@Override
	public void delete(TailDeleteRequest request, StreamObserver<HeadResponse> responseObserver) {
		if (isHead) {
			String key = request.getKey();
			String host = request.getHost();
			int port = request.getPort();
			int cxid = request.getCxid();
			System.out.printf("Request for delete for key '%s' from %s:%d with cxid %d\n",
					key, host, port, cxid);

			map.put(key, 0);
			currentXid++;

			// send update to successor if not the tail
			if (!isTail) {
				TailStateUpdateRequest updateRequest = TailStateUpdateRequest.newBuilder().setSrc(sessionID)
						.setXid(currentXid).setKey(key).setValue(0).setHost(host).setPort(port).setCxid(cxid).build();

				ChainResponse chainResponse = nextChain.proposeStateUpdate(updateRequest);
				sentList.add(updateRequest);
			}
			// respond to client
			else {
				// store the client stub for future use
				if (!clientMap.containsKey(host + ":" + port)) {
					ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
					clientMap.put(host + ":" + port, TailClientGrpc.newBlockingStub(channel));
				}
				CxidProcessedRequest cxidRequest = CxidProcessedRequest.newBuilder().setCxid(cxid).build();
				ChainResponse chainResponse = clientMap.get(host + ":" + port).cxidProcessed(cxidRequest);
			}

			HeadResponse response = HeadResponse.newBuilder().setRc(0).build();
			responseObserver.onNext(response);
		} else {
			System.out.println("Improper request for delete ");
			HeadResponse response = HeadResponse.newBuilder().setRc(1).build();
			responseObserver.onNext(response);
		}

		responseObserver.onCompleted();
	}

	@Override
	public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
		if (isTail) {
			String key = request.getKey();
			System.out.println("Request(get) for key '" + key + "'");

			int value = 0;
			if (map.containsKey(key)) {
				value = map.get(key);
			} else {
				map.put(key, 0);
			}

			GetResponse response = GetResponse.newBuilder().setRc(0).setValue(value).build();
			responseObserver.onNext(response);
		} else {
			System.out.println("Improper Request ");
			GetResponse response = GetResponse.newBuilder().setRc(1).setValue(0).build();
			responseObserver.onNext(response);
		}

		responseObserver.onCompleted();


	}


	public static void main(String[] args) throws Exception {
		String zkServer = args[0];
		String zRoot = args[1];
		String host = args[2];
		int port = Integer.parseInt(args[3]);


		TailChainServer server = new TailChainServer(zkServer, zRoot, host, port);
		System.out.println("Starting grpc server on port " + port);
		server.server.start();
		server.server.awaitTermination();
	}
}
