package quorum;

import common.TestUtils;
import distrib.patterns.common.Config;
import distrib.patterns.common.SystemClock;
import distrib.patterns.quorum.QuorumKVStore;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class QuorumReadWriteTest {

    @Test
    public void quorumReadWriteTest() throws IOException {
        var clusterNodes = startCluster(3);
        var athens = clusterNodes.get(0);
        var byzantium = clusterNodes.get(1);
        var cyrene = clusterNodes.get(2);

        athens.dropMessagesTo(byzantium);

        var athensAddress = clusterNodes.get(0).getClientConnectionAddress();
        var kvClient = new KVClient();
        var response = kvClient.setKV(athensAddress, "title", "Microservices");
        assertThat(response).isEqualTo("Success");

        var value = kvClient.getValue(athensAddress, "title");
        assertThat(value).isEqualTo("Microservices");

        assertThat(athens.getStoredValue("title").getValue()).isEqualTo("Microservices");
    }

    @Test
    public void quorumReadRepairTest() throws IOException {
        List<QuorumKVStore> clusterNodes = startCluster(3);
        var athens = clusterNodes.get(0);
        var byzantium = clusterNodes.get(1);
        var cyrene = clusterNodes.get(2);

        athens.dropMessagesTo(byzantium);

        var kvClient = new KVClient();
        var response = kvClient.setKV(athens.getClientConnectionAddress(), "title", "Microservices");
        assertThat(response).isEqualTo("Success");

        assertThat(athens.getStoredValue("title").getValue()).isEqualTo("Microservices");
        assertThat(cyrene.getStoredValue("title").getValue()).isEqualTo("Microservices");
        assertThat(byzantium.getStoredValue("title").getValue()).isEmpty();

        var value = kvClient.getValue(cyrene.getClientConnectionAddress(), "title");
        assertThat(value).isEqualTo("Microservices");

        assertThat(byzantium.getStoredValue("title").getValue()).isEmpty();
        TestUtils.waitUntilTrue(()-> {
                return "Microservices".equals(byzantium.getStoredValue("title").getValue());
                }, "Waiting for read repair", Duration.ofSeconds(2));

    }
    
    @Test
    public void quorumIncompleteWriteTest() throws IOException {
        var clusterNodes = startCluster(3);
        var athens = clusterNodes.get(0);
        var byzantium = clusterNodes.get(1);
        var cyrene = clusterNodes.get(2);

        athens.dropMessagesTo(byzantium);
        athens.dropMessagesTo(cyrene);

        var athensAddress = athens.getClientConnectionAddress();
        var kvClient = new KVClient();
        var response = kvClient.setKV(athensAddress, "title", "Microservices");
        assertThat(response).isEqualTo("Error");

        assertThat(kvClient.getValue(athensAddress, "title")).isEqualTo("Error");

        assertThat(athens.getStoredValue("title").getValue()).isEqualTo("Microservices");
    }

    @Test
    public void quorumIncompleteWriteTestWhenWritingToUnreachableNodeDirectly() throws IOException {
        var clusterNodes = startCluster(3);
        var athens = clusterNodes.get(0);
        var byzantium = clusterNodes.get(1);
        var cyrene = clusterNodes.get(2);

        athens.dropMessagesTo(byzantium);
        athens.dropMessagesTo(cyrene);

        var kvClient = new KVClient();
        assertThat(kvClient.setKV(athens.getClientConnectionAddress(), "title", "Microservices")).isEqualTo("Error");
        assertThat(athens.getStoredValue("title").getValue()).isEqualTo("Microservices");
        assertThat(cyrene.getStoredValue("title").getValue()).isEmpty();
        assertThat(byzantium.getStoredValue("title").getValue()).isEmpty();

        assertThat(kvClient.setKV(cyrene.getClientConnectionAddress(), "title", "Distributed Systems")).isEqualTo("Success");

        assertThat(athens.getStoredValue("title").getValue()).isEqualTo("Distributed Systems");
        assertThat(cyrene.getStoredValue("title").getValue()).isEqualTo("Distributed Systems");
        assertThat(byzantium.getStoredValue("title").getValue()).isEqualTo("Distributed Systems");
    }

    private List<QuorumKVStore> startCluster(int clusterSize) throws IOException {
        List<QuorumKVStore> clusterNodes = new ArrayList<>();
        var clock = new SystemClock();
        var addresses = TestUtils.createNAddresses(clusterSize);
        var clientInterfaceAddresses = TestUtils.createNAddresses(clusterSize);

        for (int i = 0; i < clusterSize; i++) {
            Config config = new Config(TestUtils.tempDir("clusternode_" + i).getAbsolutePath());
            QuorumKVStore receivingClusterNode = new QuorumKVStore(clock, config, clientInterfaceAddresses.get(i), addresses.get(i), addresses);
            receivingClusterNode.start();
            clusterNodes.add(receivingClusterNode);
        }
        return clusterNodes;
    }


    @Test
    public void nodesShouldRejectRequestsFromPreviousGenerationNode() throws IOException {
        List<QuorumKVStore> clusterNodes = startCluster(3);
        var primaryClusterNode = clusterNodes.get(0);
        var client = new KVClient();
        var primaryNodeAddress = primaryClusterNode.getClientConnectionAddress();
        assertThat(client.setKV(primaryNodeAddress, "key", "value")).isEqualTo("Success");

        assertThat(client.getValue(primaryNodeAddress, "key")).isEqualTo("value");

        //Simulates starting a new primary instance because the first went under a GC pause.
        var config = new Config(primaryClusterNode.getConfig().getWalDir().getAbsolutePath());
        var newClientAddress = TestUtils.randomLocalAddress();
        var newInstance = new QuorumKVStore(new SystemClock(), config, newClientAddress, TestUtils.randomLocalAddress(), Arrays.asList(clusterNodes.get(1).getPeerConnectionAddress(), clusterNodes.get(2).getPeerConnectionAddress()));
        newInstance.start();

        assertThat(newInstance.getGeneration()).isEqualTo(2);
        String responseForWrite = client.setKV(newClientAddress, "key1", "value1");
        assertThat(responseForWrite).isEqualTo("Success");

        assertThat(client.setKV(primaryNodeAddress, "key2", "value2")).isEqualTo("Rejecting request from generation 1 as already accepted from generation 2");
    }
}