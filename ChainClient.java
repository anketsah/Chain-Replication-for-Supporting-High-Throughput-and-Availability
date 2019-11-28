package edu.sjsu.cs249.chain;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChainClient implements Watcher {
    private ZooKeeper zk;
    private long currentChainTail;
    private long currentChainHead;
    private TailChainReplicaGrpc.TailChainReplicaBlockingStub  chainTail;
    private TailChainReplicaGrpc.TailChainReplicaBlockingStub  chainHead;
    private AtomicBoolean retryChainCheck = new AtomicBoolean();

    private void setChainHead(long session) throws KeeperException, InterruptedException {
        if (currentChainHead == session) {
            return;
        }
        chainHead = null;
        chainHead = getStub(session);
        currentChainHead = session;
    }

    private TailChainReplicaGrpc.TailChainReplicaBlockingStub getStub(long session) throws KeeperException, InterruptedException {
        byte data[] = zk.getData("/tailchain/" + session, false, null);
        InetSocketAddress addr = str2addr(new String(data).split("\n")[0]);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(addr.getHostName(), addr.getPort()).usePlaintext().build();
        return TailChainReplicaGrpc.newBlockingStub(channel);
    }

    private void setChainTail(long session) throws KeeperException, InterruptedException {
        if (currentChainTail == session) {
            return;
        }
        chainTail = null;
        chainTail = getStub(session);
        currentChainTail = session;
    }

    private static InetSocketAddress str2addr(String addr) {
        int colon = addr.lastIndexOf(':');
        return new InetSocketAddress(addr.substring(0, colon), Integer.parseInt(addr.substring(colon+1)));
    }

    private ChainClient(String zkServer) throws KeeperException, InterruptedException, IOException {
        zk = new ZooKeeper(zkServer, 3000, this);
        checkHeadTail();
        new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(
                () -> { if (retryChainCheck.getAndSet(false)) checkHeadTail();},
                5,
                5,
                TimeUnit.SECONDS
        );
    }

    private void get(String key) {
        GetResponse rsp = chainTail.get(GetRequest.newBuilder().setKey(key).build());
        if (rsp.getRc() != 0) {
            System.out.printf("Error %d occurred\n", rsp.getRc());
        } else {
            System.out.printf("%d\n", rsp.getValue());
        }
    }

    private void del(String key) {
        HeadResponse rsp = chainHead.delete(TailDeleteRequest.newBuilder().setKey(key).build());
        if (rsp.getRc() != 0) {
            System.out.printf("Error %d occurred\n", rsp.getRc());
        } else {
            System.out.print("%s deleted\n");
        }
    }

    private void inc(String key, int val) {
        HeadResponse rsp = chainHead.increment(TailIncrementRequest.newBuilder().setKey(key).setIncrValue(val).build());
        if (rsp.getRc() != 0) {
            System.out.printf("Error %d occurred\n", rsp.getRc());
        } else {
            System.out.printf("%s incremented by %d\n", key, val);
        }


    }

    public static void main(String args[]) throws Exception {
        String zkServer;
        Scanner scanner = new Scanner(System.in);
        System.out.println("enter the server IP");
        zkServer = scanner.nextLine();
        int client;
        Scanner sc1 = new Scanner(System.in);
        System.out.println("enter the server port");
        client = sc1.nextInt();
//        ChainClient client = new ChainClient(args[0]);
//        Scanner scanner = new Scanner(System.in);
        String line;
        while ((line = scanner.nextLine()) != null) {
            String parts[] = line.split(" ");
            if (parts[0].equals("get")) {
                client.get(parts[1]);
            } else if (parts[0].equals("inc")) {
                client.inc(parts[1], Integer.parseInt(parts[2]));
            } else if (parts[0].equals("del")) {
                client.del(parts[1]);
            } else {
                System.out.println("don't know " + parts[0]);
                System.out.println("i know:");
                System.out.println("get key");
                System.out.println("inc key value");
                System.out.println("del key");
            }
        }
    }

    private void checkHeadTail() {
        try {
            List<String> children = zk.getChildren("/tailchain", true);
            ArrayList<Long> sessions = new ArrayList<>();
            for (String child: children) {
                try {
                    sessions.add(Long.parseLong(child, 16));
                } catch (NumberFormatException e) {
                    // just skip non numbers
                }
            }
            sessions.sort(Long::compare);
            long head = sessions.get(0);
            long tail = sessions.get(children.size() - 1);
            setChainHead(head);
            setChainTail(tail);
        } catch (KeeperException | InterruptedException e) {
            retryChainCheck.set(true);
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getState().equals(Event.KeeperState.Expired) || watchedEvent.getState().equals(Event.KeeperState.Closed)) {
            System.err.println("disconnected from zookeeper");
            System.exit(2);
        }
        checkHeadTail();
    }
}
