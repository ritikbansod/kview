package com.ritikbansod.kafkawrapper.history;

import com.ritikbansod.kafkawrapper.history.PartitionHistoryService.ChangeEvent;
import com.ritikbansod.kafkawrapper.history.PartitionHistoryService.ReplicaSnapshot;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionHistoryDiffTest {

    private final long now = 1_700_000_000_000L;

    private ReplicaSnapshot snapshot(int leader, int... ids) {
        List<Integer> list = new ArrayList<>();
        for (int id : ids) list.add(id);
        return new ReplicaSnapshot(leader, list, list);
    }

    @Test
    void detectsLeaderMoveWithPreferredElectionFlag() {
        Map<String, ReplicaSnapshot> before = new HashMap<>();
        before.put("orders/0", snapshot(1, 1, 2));
        Map<String, ReplicaSnapshot> after = new HashMap<>();
        after.put("orders/0", new ReplicaSnapshot(2, List.of(2, 1), List.of(2, 1)));

        List<ChangeEvent> events = PartitionHistoryService.diff("c", before, after, now);

        assertThat(events).hasSize(1);
        ChangeEvent e = events.get(0);
        assertThat(e.type()).isEqualTo("LEADER_CHANGED");
        assertThat(e.fromLeader()).isEqualTo(1);
        assertThat(e.toLeader()).isEqualTo(2);
        assertThat(e.detail()).contains("preferred leader");
    }

    @Test
    void nonPreferredLeaderMoveIsAnnotated() {
        Map<String, ReplicaSnapshot> before = new HashMap<>();
        before.put("orders/0", snapshot(1, 1, 2));
        Map<String, ReplicaSnapshot> after = new HashMap<>();
        after.put("orders/0", new ReplicaSnapshot(2, List.of(1, 2), List.of(1, 2)));

        List<ChangeEvent> events = PartitionHistoryService.diff("c", before, after, now);

        assertThat(events).hasSize(1);
        assertThat(events.get(0).type()).isEqualTo("LEADER_CHANGED");
        assertThat(events.get(0).detail()).doesNotContain("preferred");
    }

    @Test
    void detectsOfflineAndRestoredLeader() {
        Map<String, ReplicaSnapshot> before = new HashMap<>();
        before.put("orders/0", snapshot(1, 1, 2));
        Map<String, ReplicaSnapshot> offline = new HashMap<>();
        offline.put("orders/0", new ReplicaSnapshot(-1, List.of(1, 2), List.of()));
        Map<String, ReplicaSnapshot> restored = new HashMap<>();
        restored.put("orders/0", snapshot(2, 1, 2));

        List<ChangeEvent> toOffline = PartitionHistoryService.diff("c", before, offline, now);
        assertThat(toOffline).hasSize(1);
        assertThat(toOffline.get(0).type()).isEqualTo("LEADER_OFFLINE");
        assertThat(toOffline.get(0).fromLeader()).isEqualTo(1);
        assertThat(toOffline.get(0).toLeader()).isNull();

        List<ChangeEvent> backOnline = PartitionHistoryService.diff("c", offline, restored, now);
        assertThat(backOnline).hasSize(1);
        assertThat(backOnline.get(0).type()).isEqualTo("LEADER_CHANGED");
        assertThat(backOnline.get(0).toLeader()).isEqualTo(2);
    }

    @Test
    void detectsIsrShrinkAndExpand() {
        Map<String, ReplicaSnapshot> before = new HashMap<>();
        before.put("orders/0", snapshot(1, 1, 2));
        Map<String, ReplicaSnapshot> shrunk = new HashMap<>();
        shrunk.put("orders/0", new ReplicaSnapshot(1, List.of(1, 2), List.of(1)));
        Map<String, ReplicaSnapshot> expanded = new HashMap<>();
        expanded.put("orders/0", snapshot(1, 1, 2));

        List<ChangeEvent> shrink = PartitionHistoryService.diff("c", before, shrunk, now);
        assertThat(shrink).hasSize(1);
        assertThat(shrink.get(0).type()).isEqualTo("ISR_CHANGED");
        assertThat(shrink.get(0).detail()).contains("shrunk").contains("2");

        List<ChangeEvent> expand = PartitionHistoryService.diff("c", shrunk, expanded, now);
        assertThat(expand).hasSize(1);
        assertThat(expand.get(0).type()).isEqualTo("ISR_CHANGED");
        assertThat(expand.get(0).detail()).contains("expanded").contains("2");
    }

    @Test
    void detectsPartitionAddedAndRemoved() {
        Map<String, ReplicaSnapshot> before = new HashMap<>();
        before.put("orders/0", snapshot(1, 1));
        Map<String, ReplicaSnapshot> after = new HashMap<>();
        after.put("orders/0", snapshot(1, 1));
        after.put("orders/1", snapshot(1, 1));

        assertThat(PartitionHistoryService.diff("c", before, after, now))
                .singleElement()
                .satisfies(e -> {
                    assertThat(e.type()).isEqualTo("PARTITION_ADDED");
                    assertThat(e.topic()).isEqualTo("orders");
                    assertThat(e.partition()).isEqualTo(1);
                });

        assertThat(PartitionHistoryService.diff("c", after, before, now))
                .singleElement()
                .satisfies(e -> assertThat(e.type()).isEqualTo("PARTITION_REMOVED"));
    }

    @Test
    void detectsReassignment() {
        Map<String, ReplicaSnapshot> before = new HashMap<>();
        before.put("orders/0", new ReplicaSnapshot(1, List.of(1, 2), List.of(1, 2)));
        Map<String, ReplicaSnapshot> after = new HashMap<>();
        after.put("orders/0", new ReplicaSnapshot(1, List.of(1, 3), List.of(1, 3)));

        List<ChangeEvent> events = PartitionHistoryService.diff("c", before, after, now);

        assertThat(events).hasSize(1);
        assertThat(events.get(0).type()).isEqualTo("REASSIGNED");
        assertThat(events.get(0).detail()).contains("[1, 2]").contains("[1, 3]");
    }

    @Test
    void noChangesMeansNoEvents() {
        Map<String, ReplicaSnapshot> state = new HashMap<>();
        state.put("orders/0", snapshot(1, 1, 2));
        state.put("orders/1", snapshot(1, 1));
        assertThat(PartitionHistoryService.diff("c", state, new HashMap<>(state), now)).isEmpty();
    }
}
