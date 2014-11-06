package dk.mycorp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import net.openhft.chronicle.hash.TcpReplicationConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.MapEventListener;
import net.openhft.lang.io.Bytes;

import org.junit.Assert;
import org.junit.Test;

public class ChronicleReplication {
	@Test
	public void test() throws IOException, InterruptedException {

		ChronicleMap<Integer, CharSequence> map1;
		ChronicleMap<Integer, CharSequence> map2;
		final AtomicBoolean putWasCalled = new AtomicBoolean(false);

		int udpPort = 1234;
		MapEventListener<Integer, CharSequence, ChronicleMap<Integer, CharSequence>> eventListener = new MapEventListener<Integer, CharSequence, ChronicleMap<Integer, CharSequence>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void onPut(ChronicleMap<Integer, CharSequence> map, Bytes entry, int metaDataBytes, boolean added, Integer key,
					CharSequence value) {
				putWasCalled.getAndSet(true);
			}
		};

		// ---------- SERVER1 1 ----------
		{

			// we connect the maps via a TCP socket connection on port 8077

			TcpReplicationConfig tcpConfig = TcpReplicationConfig.of(8076, new InetSocketAddress("localhost", 8077));
			ChronicleMapBuilder<Integer, CharSequence> map1Builder =
					ChronicleMapBuilder.of(Integer.class, CharSequence.class)
							.entries(20000L)
							.replicators((byte) 1, tcpConfig);

			map1 = map1Builder.create();
		}
		// ---------- SERVER2 2 on the same server as ----------

		{
			TcpReplicationConfig tcpConfig = TcpReplicationConfig.of(8077);
			ChronicleMapBuilder<Integer, CharSequence> map2Builder =
					ChronicleMapBuilder.of(Integer.class, CharSequence.class)
							.entries(20000L)
							.replicators((byte) 2, tcpConfig)
							.eventListener(eventListener);

			map2 = map2Builder.create();
		}

		// we will stores some data into one map here
		map1.put(5, "EXAMPLE");
		int t = 0;
		for (; t < 5000; t++) {
			if (map1.equals(map2)) {
				break;
			}
			Thread.sleep(1);
		}

		Assert.assertEquals("Maps should be equal", map1, map2);
		Assert.assertTrue("Map 1 should not be empty", !map1.isEmpty());
		Assert.assertTrue("Map 2 should not be empty", !map2.isEmpty());
		Assert.assertTrue("The eventListener.onPut should have been called", putWasCalled.get());
	}
}