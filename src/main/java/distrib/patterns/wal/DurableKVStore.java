package distrib.patterns.wal;

import distrib.patterns.common.Config;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DurableKVStore {
    private final Map<String, String> kv = new HashMap<>();

    public String get(String key) {
        return kv.get(key);
    }

    public void put(String key, String value) {
        //TODO: Assignment 1: appendLog before storing key and value.
        appendLog(key, value);
        kv.put(key, value);
    }

    private Long appendLog(String key, String value) {
        Long aLong = wal.writeEntry(new SetValueCommand(key, value).serialize());
        wal.flush();
        return aLong;
    }

    //@VisibleForTesting
    final WriteAheadLog wal;
    private final Config config;

    public DurableKVStore(Config config) {
        this.config = config;
        this.wal = WriteAheadLog.openWAL(config);
        applyLog();
       //TODO: applyLog at startup.
        applyLog();
    }

    public void applyLog() {
        List<WALEntry> walEntries = wal.readAll();
        applyEntries(walEntries);
    }

    private void applyEntries(List<WALEntry> walEntries) {
        for (WALEntry walEntry : walEntries) {
            Command command = deserialize(walEntry);
            if (command instanceof SetValueCommand setValueCommand) {
                kv.put(setValueCommand.key, setValueCommand.value);
            }
        }
    }

    private Command deserialize(WALEntry walEntry) {
        return Command.deserialize(new ByteArrayInputStream(walEntry.getData()));
    }

    public void close() {
        wal.close();
        kv.clear();
    }
}
