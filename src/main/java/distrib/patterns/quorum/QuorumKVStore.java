package distrib.patterns.quorum;

import distrib.patterns.common.*;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.SocketListener;
import distrib.patterns.net.NIOSocketListener;
import distrib.patterns.net.requestwaitinglist.RequestCallback;
import distrib.patterns.net.requestwaitinglist.RequestWaitingList;
import distrib.patterns.requests.GetValueRequest;
import distrib.patterns.requests.SetValueRequest;
import distrib.patterns.wal.DurableKVStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QuorumKVStore {
    public static final int firstGeneration = 1;
    private static final Logger logger = LogManager.getLogger(QuorumKVStore.class);
    private final NIOSocketListener clientListener;
    private final SystemClock clock;
    private final Config config;
    private final InetAddressAndPort clientConnectionAddress;
    private final InetAddressAndPort peerConnectionAddress;
    private final List<InetAddressAndPort> peers;
    private final SocketListener peerListener;
    Map<String, StoredValue> kv = new HashMap<>();

    public void dropMessagesTo(QuorumKVStore clusterNode) {
        network.dropMessagesTo(clusterNode.peerConnectionAddress);
    }

    public StoredValue getStoredValue(String key) {
        StoredValue storedValue = kv.get(key);
        if (storedValue == null) {
            return StoredValue.EMPTY;
        }
        return storedValue;
    }

    public void reconnectTo(QuorumKVStore clusterNode) {
        network.reconnectTo(clusterNode.peerConnectionAddress);
    }

    Network network = new Network();
    DurableKVStore systemStorage;

    public QuorumKVStore(SystemClock clock, Config config, InetAddressAndPort clientAddress, InetAddressAndPort peerConnectionAddress, List<InetAddressAndPort> peers) throws IOException {
        this.clock = clock;
        this.config = config;
        this.clientConnectionAddress = clientAddress;
        this.peerConnectionAddress = peerConnectionAddress;
        this.peers = peers;
        systemStorage = new DurableKVStore(config);
        generation = incrementAndGetGeneration();
        requestWaitingList = new RequestWaitingList(clock);

        this.peerListener = new SocketListener(this::handleServerMessage, peerConnectionAddress, config);
        this.clientListener = new NIOSocketListener(message -> {
            handleClientRequest(message);
        }, clientAddress);
    }

    private void handleClientRequest(Message<RequestOrResponse> message) {
        RequestOrResponse request = message.getRequest();
        if (request.getRequestId() == RequestId.SetValueRequest.getId()) {
            handleClientRequestRequiringQuorum(peers, request, new WriteQuorumCallback(peers.size(), request, message.getClientConnection()), RequestId.SetValueRequest);

        } else if (request.getRequestId() == RequestId.GetValueRequest.getId()) {
            handleClientRequestRequiringQuorum(peers, request, new ReadQuorumCallback(peers.size(), request, message.getClientConnection()), RequestId.GetValueRequest);
        }
    }


    private int incrementAndGetGeneration() {
        String s = systemStorage.get("generation");
        int currentGeneration = s == null? firstGeneration :Integer.parseInt(s) + 1;
        systemStorage.put("generation", String.valueOf(currentGeneration));
        return currentGeneration;
    }

    RequestWaitingList requestWaitingList;
    int requestNumber;

    private int nextRequestId() {
        return requestNumber++;
    }

    int generation;
    private void handleClientRequestRequiringQuorum(List<InetAddressAndPort> replicas, RequestOrResponse clientRequest, RequestCallback requestCallback, RequestId requestId) {
        for (InetAddressAndPort replica : replicas) {
            int correlationId = nextRequestId();
            requestWaitingList.add(correlationId, requestCallback);
            try {
                //GC Pause
                network.sendOneWay(replica, new RequestOrResponse(generation, requestId.getId(), clientRequest.getMessageBodyJson(), correlationId, peerConnectionAddress));
            } catch (IOException e) {
                requestWaitingList.handleError(correlationId, e);
            }
        }
    }

    public void start() {
        peerListener.start();
        clientListener.start();
    }

    //<codeFragment name="handleServerMessage">
    private synchronized void handleServerMessage(Message<RequestOrResponse> message) {
        RequestOrResponse requestOrResponse = message.getRequest();
        if (requestOrResponse.getRequestId() == RequestId.SetValueRequest.getId()) {
            handleSetValueRequest(requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.GetValueRequest.getId()) {
            handleGetValueRequest(requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.SetValueResponse.getId()) {
            handleResponse(requestOrResponse);

        } else if (requestOrResponse.getRequestId() == RequestId.GetValueResponse.getId()) {
            handleResponse(requestOrResponse);
        }
    }

    private void handleGetValueRequest(RequestOrResponse request) {
        GetValueRequest getValueRequest = deserialize(request, GetValueRequest.class);
        sendResponseMessage(new RequestOrResponse(request.getGeneration(), RequestId.GetValueResponse.getId(), JsonSerDes.serialize(getStoredValue(getValueRequest.getKey())), request.getCorrelationId(), peerConnectionAddress), request.getFromAddress());
    }

    private void handleResponse(RequestOrResponse response) {
        requestWaitingList.handleResponse(response.getCorrelationId(), response);
    }

    private void handleSetValueRequest(RequestOrResponse request) {
        var maxKnownGeneration = maxKnownGeneration();
        var requestGeneration = request.getGeneration();
        //TODO: Assignment 3 Add check for generation while handling requests.
        if (requestGeneration < maxKnownGeneration) {
            var errorMessage = "Rejecting request from generation " + requestGeneration + " as already accepted from generation " + maxKnownGeneration;
            sendResponseMessage(new RequestOrResponse(requestGeneration, RequestId.SetValueResponse.getId(), errorMessage.getBytes(), request.getCorrelationId(), peerConnectionAddress), request.getFromAddress());
            return;
        }

        var setValueRequest = deserialize(request, SetValueRequest.class);
        kv.put(setValueRequest.getKey(), new StoredValue(setValueRequest.getKey(), setValueRequest.getValue(), clock.now(), requestGeneration));
        sendResponseMessage(new RequestOrResponse(requestGeneration, RequestId.SetValueResponse.getId(), "Success".getBytes(), request.getCorrelationId(), peerConnectionAddress), request.getFromAddress());
    }

    ///        if (requestGeneration < maxKnownGeneration) {
    //            String errorMessage = "Rejecting request from generation " + requestGeneration + " as already accepted from generation " + maxKnownGeneration;
    //            sendResponseMessage(new RequestOrResponse(requestGeneration, RequestId.SetValueResponse.getId(), errorMessage.getBytes(), request.getCorrelationId(), peerConnectionAddress), request.getFromAddress());
    //            return;
    //        }
    ///
    private int maxKnownGeneration() {
        return kv.values().stream().map(kv -> kv.generation).max(Integer::compare).orElse(0);
    }

    private void sendResponseMessage(RequestOrResponse message, InetAddressAndPort fromAddress) {
        try {
            network.sendOneWay(fromAddress, message);
        } catch (IOException e) {
            logger.error("Communication failure sending request to " + fromAddress);
        }
    }

    public Config getConfig() {
        return config;
    }

    public int getGeneration() {
        return generation;
    }

    private <T> T deserialize(RequestOrResponse request, Class<T> clazz) {
        return JsonSerDes.deserialize(request.getMessageBodyJson(), clazz);
    }

    public InetAddressAndPort getClientConnectionAddress() {
        return clientConnectionAddress;
    }

    public InetAddressAndPort getPeerConnectionAddress() {
        return peerConnectionAddress;
    }
}
