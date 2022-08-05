package quorum;

import distrib.patterns.common.JsonSerDes;
import distrib.patterns.common.RequestId;
import distrib.patterns.common.RequestOrResponse;
import distrib.patterns.net.InetAddressAndPort;
import distrib.patterns.net.SocketClient;
import distrib.patterns.requests.GetValueRequest;
import distrib.patterns.requests.SetValueRequest;

import java.io.IOException;

class KVClient {
    int correlationId;
    public String getValue(InetAddressAndPort nodeAddress, String key) throws IOException {
        var requestOrResponse1 = createGetValueRequest(key);
        var getResponse = new SocketClient<>(nodeAddress).blockingSend(requestOrResponse1);
        return new String(getResponse.getMessageBodyJson());
    }

    public String setKV(InetAddressAndPort primaryNodeAddress, String key, String value) throws IOException {
        var client = new SocketClient(primaryNodeAddress);
        var requestOrResponse = createSetValueRequest(key, value);
        var setResponse = client.blockingSend(requestOrResponse);
        return new String(setResponse.getMessageBodyJson());
    }

    private RequestOrResponse createGetValueRequest(String key) {
        var getValueRequest = new GetValueRequest(key);
        var requestOrResponse1 = new RequestOrResponse(RequestId.GetValueRequest.getId(), JsonSerDes.serialize(getValueRequest), correlationId++);
        return requestOrResponse1;
    }

    private RequestOrResponse createSetValueRequest(String key, String value) {
        var setValueRequest = new SetValueRequest(key, value);
        var requestOrResponse = new RequestOrResponse(RequestId.SetValueRequest.getId(),
                JsonSerDes.serialize(setValueRequest), correlationId++);
        return requestOrResponse;
    }
}
