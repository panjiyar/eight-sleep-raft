import random
import pytest
from typing import Dict, List, Optional, Set, Tuple
from raft import (
    RaftNode, NodeState, LogEntry, Message,
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse
)


@pytest.fixture(autouse=True)
def seed_random():
    """Seed RNG before each test for deterministic behavior."""
    random.seed(42)


class SimNetwork:
    """
    Simulated network for testing Raft nodes.

    This is TEST WORLD only - handles message routing between nodes,
    simulates partitions, and provides test utilities.
    """

    def __init__(self, node_ids: List[str]):
        self.nodes: Dict[str, RaftNode] = {}
        self.partitions: Set[Tuple[str, str]] = set()
        self.pending_messages: List[Message] = []

        # Create nodes with peer lists
        for node_id in node_ids:
            peers = [nid for nid in node_ids if nid != node_id]
            self.nodes[node_id] = RaftNode(node_id, peers)

    def tick_all(self, count: int = 1):
        """Advance time for all nodes and deliver messages."""
        for _ in range(count):
            for node in self.nodes.values():
                messages = node.tick()
                if messages:
                    self.pending_messages.extend(messages)
            self._deliver_messages()

    def _deliver_messages(self):
        """Deliver all pending messages, respecting partitions."""
        messages = self.pending_messages
        self.pending_messages = []

        for msg in messages:
            if self._is_partitioned(msg.sender, msg.target):
                continue  # Drop message due to partition

            target_node = self.nodes.get(msg.target)
            if target_node is None:
                continue

            # Route based on message type
            if isinstance(msg.payload, RequestVoteRequest):
                resp = target_node.handle_request_vote(msg.payload)
                # Deliver response back to sender
                sender = self.nodes.get(msg.sender)
                if sender and not self._is_partitioned(msg.target, msg.sender):
                    sender.handle_request_vote_response(msg.target, resp)

            elif isinstance(msg.payload, AppendEntriesRequest):
                resp = target_node.handle_append_entries(msg.payload)
                sender = self.nodes.get(msg.sender)
                if sender and not self._is_partitioned(msg.target, msg.sender):
                    sender.handle_append_entries_response(msg.target, resp)

    def _is_partitioned(self, node_a: str, node_b: str) -> bool:
        """Check if two nodes are partitioned."""
        return (node_a, node_b) in self.partitions or (node_b, node_a) in self.partitions

    def partition(self, node_a: str, node_b: str):
        """Create a network partition between two nodes."""
        self.partitions.add((node_a, node_b))

    def heal_partition(self, node_a: str, node_b: str):
        """Remove partition between two nodes."""
        self.partitions.discard((node_a, node_b))
        self.partitions.discard((node_b, node_a))

    def isolate(self, node_id: str):
        """Partition a node from all others."""
        for other in self.nodes:
            if other != node_id:
                self.partition(node_id, other)

    def reconnect(self, node_id: str):
        """Remove all partitions for a node."""
        for other in self.nodes:
            if other != node_id:
                self.heal_partition(node_id, other)

    def get_leader(self) -> Optional[RaftNode]:
        """Return the current leader (highest term if multiple exist)."""
        leaders = [n for n in self.nodes.values() if n.state == NodeState.LEADER]
        if not leaders:
            return None
        # Return leader with highest term (the "real" leader)
        return max(leaders, key=lambda n: n.current_term)

    def get_leaders(self) -> List[RaftNode]:
        """Return all nodes that think they're leader."""
        return [n for n in self.nodes.values() if n.state == NodeState.LEADER]

    def submit_command(self, command: Dict) -> bool:
        """Submit a command to the leader."""
        leader = self.get_leader()
        if leader:
            return leader.submit_command(command)
        return False

    def get_value(self, key: str) -> Optional[str]:
        """Get value from leader's state machine."""
        leader = self.get_leader()
        if leader:
            return leader.kv_store.get(key)
        return None


# ==================== Unit Tests: RequestVote ====================

class TestRequestVote:
    """Unit tests for RequestVote"""

    def test_rejects_stale_term(self):
        """Node rejects vote request with lower term."""
        node = RaftNode("A", ["B", "C"])
        node.current_term = 5

        req = RequestVoteRequest(
            term=3,  # stale term
            candidate_id="B",
            last_log_index=-1,
            last_log_term=0
        )
        resp = node.handle_request_vote(req)

        assert resp.vote_granted is False
        assert resp.term == 5  # return current term so candidate can update

    def test_grants_vote_once_per_term(self):
        """Node only grants one vote per term."""
        node = RaftNode("A", ["B", "C"])
        node.current_term = 1

        # First vote request from B
        req1 = RequestVoteRequest(term=2, candidate_id="B", last_log_index=-1, last_log_term=0)
        resp1 = node.handle_request_vote(req1)
        assert resp1.vote_granted is True

        # Second vote request from C in same term
        req2 = RequestVoteRequest(term=2, candidate_id="C", last_log_index=-1, last_log_term=0)
        resp2 = node.handle_request_vote(req2)
        assert resp2.vote_granted is False  # already voted for B

    def test_can_revote_for_same_candidate(self):
        """Node can vote for same candidate again in same term."""
        node = RaftNode("A", ["B", "C"])
        node.current_term = 1

        req = RequestVoteRequest(term=2, candidate_id="B", last_log_index=-1, last_log_term=0)

        resp1 = node.handle_request_vote(req)
        assert resp1.vote_granted is True

        # Same candidate asks again (e.g., retransmission)
        resp2 = node.handle_request_vote(req)
        assert resp2.vote_granted is True

    def test_updates_term_on_higher_term(self):
        """Node updates its term when it sees a higher term."""
        node = RaftNode("A", ["B", "C"])
        node.current_term = 1
        node.state = NodeState.LEADER  # even leaders step down

        req = RequestVoteRequest(term=5, candidate_id="B", last_log_index=-1, last_log_term=0)
        node.handle_request_vote(req)

        assert node.current_term == 5
        assert node.state == NodeState.FOLLOWER

    def test_rejects_candidate_with_stale_log_shorter(self):
        """Node rejects candidate whose log is shorter."""
        node = RaftNode("A", ["B", "C"])
        node.current_term = 2
        # Node has 2 entries
        node.log = [LogEntry(term=1, command={"op": "set", "key": "x", "value": "1"}),
                    LogEntry(term=2, command={"op": "set", "key": "y", "value": "2"})]

        # Candidate has same last term but shorter log
        req = RequestVoteRequest(term=3, candidate_id="B", last_log_index=0, last_log_term=2)
        resp = node.handle_request_vote(req)

        assert resp.vote_granted is False

    def test_rejects_candidate_with_stale_log_older_term(self):
        """Node rejects candidate whose last log term is older."""
        node = RaftNode("A", ["B", "C"])
        node.current_term = 2
        # Node's last entry is from term 2
        node.log = [LogEntry(term=2, command={"op": "set", "key": "x", "value": "1"})]

        # Candidate has older last term, even with longer log
        req = RequestVoteRequest(term=3, candidate_id="B", last_log_index=5, last_log_term=1)
        resp = node.handle_request_vote(req)

        assert resp.vote_granted is False

    def test_grants_vote_if_log_up_to_date(self):
        """Node grants vote if candidate's log is up-to-date."""
        node = RaftNode("A", ["B", "C"])
        node.current_term = 2
        node.log = [LogEntry(term=1, command={"op": "set", "key": "x", "value": "1"})]

        # Candidate has higher last term
        req = RequestVoteRequest(term=3, candidate_id="B", last_log_index=0, last_log_term=2)
        resp = node.handle_request_vote(req)

        assert resp.vote_granted is True


# ==================== Unit Tests: AppendEntries ====================

class TestAppendEntries:
    """Unit tests for AppendEntries"""

    def test_rejects_stale_term(self):
        """Node rejects AppendEntries with lower term."""
        node = RaftNode("A", ["B", "C"])
        node.current_term = 5

        req = AppendEntriesRequest(
            term=3,  # stale
            leader_id="B",
            prev_log_index=-1,
            prev_log_term=0,
            entries=[],
            leader_commit=-1
        )
        resp = node.handle_append_entries(req)

        assert resp.success is False
        assert resp.term == 5

    def test_resets_election_timer(self):
        """AppendEntries from valid leader resets election timer."""
        node = RaftNode("A", ["B", "C"])
        node.current_term = 1
        node.ticks_elapsed = 4  # almost at election timeout

        req = AppendEntriesRequest(
            term=1,
            leader_id="B",
            prev_log_index=-1,
            prev_log_term=0,
            entries=[],
            leader_commit=-1
        )
        node.handle_append_entries(req)

        assert node.ticks_elapsed == 0

    def test_appends_new_entries(self):
        """Node appends new log entries."""
        node = RaftNode("A", ["B", "C"])
        node.current_term = 1

        entries = [LogEntry(term=1, command={"op": "set", "key": "x", "value": "1"})]
        req = AppendEntriesRequest(
            term=1,
            leader_id="B",
            prev_log_index=-1,
            prev_log_term=0,
            entries=entries,
            leader_commit=-1
        )
        resp = node.handle_append_entries(req)

        assert resp.success is True
        assert len(node.log) == 1
        assert node.log[0].command["key"] == "x"

    def test_updates_commit_index(self):
        """Node updates commit index when leader_commit advances."""
        node = RaftNode("A", ["B", "C"])
        node.current_term = 1
        node.log = [LogEntry(term=1, command={"op": "set", "key": "x", "value": "1"})]
        node.commit_index = -1

        req = AppendEntriesRequest(
            term=1,
            leader_id="B",
            prev_log_index=0,
            prev_log_term=1,
            entries=[],
            leader_commit=0  # leader says index 0 is committed
        )
        resp = node.handle_append_entries(req)

        assert resp.success is True
        assert node.commit_index == 0

    def test_rejects_if_log_missing_prev_entry(self):
        """Node rejects if log doesn't have entry at prev_log_index."""
        node = RaftNode("A", ["B", "C"])
        node.current_term = 1
        node.log = []  # empty log

        req = AppendEntriesRequest(
            term=1,
            leader_id="B",
            prev_log_index=0,  # expects entry at index 0
            prev_log_term=1,
            entries=[LogEntry(term=1, command={"op": "set", "key": "x", "value": "1"})],
            leader_commit=-1
        )
        resp = node.handle_append_entries(req)

        assert resp.success is False

    def test_rejects_if_prev_term_mismatch(self):
        """Node rejects if entry at prev_log_index has different term."""
        node = RaftNode("A", ["B", "C"])
        node.current_term = 2
        node.log = [LogEntry(term=1, command={"op": "set", "key": "x", "value": "1"})]

        req = AppendEntriesRequest(
            term=2,
            leader_id="B",
            prev_log_index=0,
            prev_log_term=2,  # mismatch: node has term 1 at index 0
            entries=[],
            leader_commit=-1
        )
        resp = node.handle_append_entries(req)

        assert resp.success is False

    def test_truncates_conflicting_entries(self):
        """Node truncates log entries that conflict with leader."""
        node = RaftNode("A", ["B", "C"])
        node.current_term = 2
        # Node has entries from old term that conflict
        node.log = [
            LogEntry(term=1, command={"op": "set", "key": "x", "value": "1"}),
            LogEntry(term=1, command={"op": "set", "key": "y", "value": "old"})  # will be replaced
        ]

        req = AppendEntriesRequest(
            term=2,
            leader_id="B",
            prev_log_index=0,
            prev_log_term=1,
            entries=[LogEntry(term=2, command={"op": "set", "key": "y", "value": "new"})],
            leader_commit=-1
        )
        resp = node.handle_append_entries(req)

        assert resp.success is True
        assert len(node.log) == 2
        assert node.log[1].term == 2
        assert node.log[1].command["value"] == "new"

    def test_candidate_steps_down_on_valid_append_entries(self):
        """Candidate becomes follower when receiving valid AppendEntries."""
        node = RaftNode("A", ["B", "C"])
        node.current_term = 2
        node.state = NodeState.CANDIDATE

        req = AppendEntriesRequest(
            term=2,
            leader_id="B",
            prev_log_index=-1,
            prev_log_term=0,
            entries=[],
            leader_commit=-1
        )
        node.handle_append_entries(req)

        assert node.state == NodeState.FOLLOWER

    def test_sets_leader_id(self):
        """Node sets leader_id when receiving valid AppendEntries."""
        node = RaftNode("A", ["B", "C"])
        node.current_term = 1
        node.leader_id = None

        req = AppendEntriesRequest(
            term=1,
            leader_id="B",
            prev_log_index=-1,
            prev_log_term=0,
            entries=[],
            leader_commit=-1
        )
        node.handle_append_entries(req)

        assert node.leader_id == "B"


# ==================== Unit Tests: AppendEntriesResponse ====================

class TestAppendEntriesResponse:
    """Unit tests for leader handling AppendEntries responses."""

    def _make_leader(self, node_id: str, peers: List[str]) -> RaftNode:
        """Helper to create a leader node with initialized state."""
        node = RaftNode(node_id, peers)
        node.state = NodeState.LEADER
        node.current_term = 1
        # Initialize leader state
        for peer in peers:
            node.next_index[peer] = 0
            node.match_index[peer] = -1
        return node

    def test_leader_updates_match_index_on_success(self):
        """Leader updates match_index when follower accepts entries."""
        leader = self._make_leader("A", ["B", "C"])
        leader.log = [LogEntry(term=1, command={"op": "set", "key": "x", "value": "1"})]
        leader.next_index["B"] = 1  # we sent index 0

        resp = AppendEntriesResponse(term=1, success=True)
        leader.handle_append_entries_response("B", resp)

        assert leader.match_index["B"] == 0  # follower has replicated up to index 0

    def test_leader_decrements_next_index_on_failure(self):
        """Leader decrements next_index when follower rejects."""
        leader = self._make_leader("A", ["B", "C"])
        leader.log = [
            LogEntry(term=1, command={"op": "set", "key": "x", "value": "1"}),
            LogEntry(term=1, command={"op": "set", "key": "y", "value": "2"})
        ]
        leader.next_index["B"] = 2  # tried to send from index 2

        resp = AppendEntriesResponse(term=1, success=False)
        leader.handle_append_entries_response("B", resp)

        assert leader.next_index["B"] == 1  # decremented to retry earlier

    def test_leader_advances_commit_on_majority(self):
        """Leader advances commit_index when majority has replicated."""
        leader = self._make_leader("A", ["B", "C"])
        leader.log = [LogEntry(term=1, command={"op": "set", "key": "x", "value": "1"})]
        leader.commit_index = -1
        leader.next_index = {"B": 1, "C": 1}
        leader.match_index = {"B": -1, "C": -1}

        # B acknowledges
        resp = AppendEntriesResponse(term=1, success=True)
        leader.handle_append_entries_response("B", resp)

        # Leader + B = 2 nodes = majority in 3-node cluster
        assert leader.commit_index == 0

    def test_leader_steps_down_on_higher_term(self):
        """Leader becomes follower if response has higher term."""
        leader = self._make_leader("A", ["B", "C"])

        resp = AppendEntriesResponse(term=5, success=False)
        leader.handle_append_entries_response("B", resp)

        assert leader.state == NodeState.FOLLOWER
        assert leader.current_term == 5


# ==================== Unit Tests: RequestVoteResponse ====================

class TestRequestVoteResponse:
    """Unit tests for candidate handling RequestVote responses."""

    def _make_candidate(self, node_id: str, peers: List[str]) -> RaftNode:
        """Helper to create a candidate node."""
        node = RaftNode(node_id, peers)
        node.state = NodeState.CANDIDATE
        node.current_term = 2
        node.voted_for = node_id  # candidates vote for themselves
        node.votes_received = {node_id}  # count own vote
        return node

    def test_candidate_becomes_leader_on_majority(self):
        """Candidate becomes leader when receiving majority votes."""
        candidate = self._make_candidate("A", ["B", "C"])

        # Receive vote from B (A + B = 2 votes = majority)
        resp = RequestVoteResponse(term=2, vote_granted=True)
        candidate.handle_request_vote_response("B", resp)

        assert candidate.state == NodeState.LEADER

    def test_candidate_steps_down_on_higher_term(self):
        """Candidate becomes follower if response has higher term."""
        candidate = self._make_candidate("A", ["B", "C"])

        resp = RequestVoteResponse(term=5, vote_granted=False)
        candidate.handle_request_vote_response("B", resp)

        assert candidate.state == NodeState.FOLLOWER
        assert candidate.current_term == 5

    def test_candidate_ignores_stale_response(self):
        """Candidate ignores vote response from old term."""
        candidate = self._make_candidate("A", ["B", "C"])
        candidate.current_term = 3  # moved on to term 3

        # Stale response from term 2
        resp = RequestVoteResponse(term=2, vote_granted=True)
        candidate.handle_request_vote_response("B", resp)

        # Should still be candidate (not become leader from stale vote)
        assert candidate.state == NodeState.CANDIDATE
        assert len(candidate.votes_received) == 1  # only self-vote


# ==================== Integration Tests: Leader Election ====================

class TestLeaderElection:
    """Integration tests for leader election."""

    def test_elects_single_leader(self):
        """A 3-node cluster elects exactly one leader."""
        net = SimNetwork(["A", "B", "C"])

        # Run enough ticks to trigger election and complete it
        # With randomized timeout (5-15), need at least 15 + some buffer
        net.tick_all(20)

        leader = net.get_leader()
        assert leader is not None

    def test_leader_sends_heartbeats(self):
        """Leader sends heartbeats to prevent re-election."""
        net = SimNetwork(["A", "B", "C"])

        # Elect a leader
        net.tick_all(20)
        leader = net.get_leader()
        assert leader is not None
        initial_term = leader.current_term

        # Run more ticks - heartbeats should prevent new elections
        net.tick_all(30)

        # Still same leader, same term
        assert net.get_leader() == leader
        assert leader.current_term == initial_term

    def test_reelection_on_leader_failure(self):
        """Cluster elects new leader when current leader is partitioned."""
        net = SimNetwork(["A", "B", "C"])

        # Elect initial leader
        net.tick_all(20)
        old_leader = net.get_leader()
        assert old_leader is not None
        old_leader_id = old_leader.node_id

        # Partition the leader
        net.isolate(old_leader_id)

        # Run enough ticks for remaining nodes to elect new leader
        # Followers need to timeout (up to 15 ticks) + election time
        net.tick_all(40)

        # Should have a new leader (highest term wins)
        new_leader = net.get_leader()
        assert new_leader is not None
        assert new_leader.node_id != old_leader_id

    def test_leader_steps_down_on_higher_term(self):
        """Leader becomes follower when it sees a higher term."""
        net = SimNetwork(["A", "B", "C"])

        # Elect leader
        net.tick_all(20)
        leader = net.get_leader()
        assert leader is not None

        # Simulate a higher term message via RequestVote
        req = RequestVoteRequest(
            term=leader.current_term + 5,
            candidate_id="X",  # external candidate
            last_log_index=-1,
            last_log_term=0
        )
        leader.handle_request_vote(req)

        assert leader.state == NodeState.FOLLOWER

    def test_split_vote_triggers_new_election(self):
        """Split vote causes new election with incremented term."""
        # Create 4-node cluster where split votes are more likely
        net = SimNetwork(["A", "B", "C", "D"])

        # Partition into pairs to force split vote scenario
        net.partition("A", "C")
        net.partition("A", "D")
        net.partition("B", "C")
        net.partition("B", "D")

        # Tick - no majority possible in either partition
        net.tick_all(20)

        # Neither partition can elect (need 3 of 4)
        # Check that terms have incremented (elections happened)
        terms = [n.current_term for n in net.nodes.values()]
        assert all(t > 0 for t in terms)

        # Heal and let them elect
        for node_id in net.nodes:
            net.reconnect(node_id)
        net.tick_all(40)

        # Now should have a leader
        assert net.get_leader() is not None

    def test_candidate_with_longer_log_wins(self):
        """Candidate with more up-to-date log wins election."""
        net = SimNetwork(["A", "B", "C"])

        # Give node A a more up-to-date log
        net.nodes["A"].log = [
            LogEntry(term=1, command={"op": "set", "key": "x", "value": "1"}),
            LogEntry(term=2, command={"op": "set", "key": "y", "value": "2"})
        ]
        net.nodes["A"].current_term = 2
        # Force A to timeout first, was causing flakiness
        net.nodes["A"].election_timeout = 1

        # Give B and C shorter/older logs
        net.nodes["B"].log = [LogEntry(term=1, command={"op": "set", "key": "x", "value": "1"})]
        net.nodes["B"].current_term = 1
        net.nodes["C"].log = []
        net.nodes["C"].current_term = 1

        # Run election
        net.tick_all(20)

        # A should become leader (most up-to-date log)
        leader = net.get_leader()
        assert leader is not None
        assert leader.node_id == "A"


# ==================== Integration Tests: Log Replication ====================

class TestLogReplication:
    """Integration tests for log replication."""

    def test_command_replicates_to_followers(self):
        """Commands replicate to all followers."""
        net = SimNetwork(["A", "B", "C"])

        # Elect leader
        net.tick_all(20)
        leader = net.get_leader()
        assert leader is not None

        # Submit command
        leader.submit_command({"op": "set", "key": "x", "value": "1"})

        # Tick to replicate
        net.tick_all(10)

        # All nodes should have the entry
        for node in net.nodes.values():
            assert len(node.log) == 1
            assert node.log[0].command["key"] == "x"

    def test_command_commits_on_majority(self):
        """Command is committed when majority acknowledges."""
        net = SimNetwork(["A", "B", "C"])

        # Elect leader
        net.tick_all(20)
        leader = net.get_leader()
        assert leader is not None

        # Submit command
        leader.submit_command({"op": "set", "key": "x", "value": "1"})

        # Tick to replicate and commit
        net.tick_all(10)

        # Leader should have committed
        assert leader.commit_index == 0

    def test_follower_catches_up(self):
        """Partitioned follower catches up after rejoining."""
        net = SimNetwork(["A", "B", "C"])

        # Elect leader
        net.tick_all(20)
        leader = net.get_leader()
        assert leader is not None

        # Partition one follower
        follower_id = [n for n in net.nodes if n != leader.node_id][0]
        net.isolate(follower_id)

        # Submit command while follower is partitioned
        leader.submit_command({"op": "set", "key": "x", "value": "1"})
        net.tick_all(10)

        # Follower should not have the entry
        assert len(net.nodes[follower_id].log) == 0

        # Reconnect follower
        net.reconnect(follower_id)
        net.tick_all(30)

        # Follower should have caught up
        assert len(net.nodes[follower_id].log) == 1

    def test_leader_retries_on_rejection(self):
        """Leader decrements next_index and retries on rejection."""
        net = SimNetwork(["A", "B", "C"])

        # Elect leader
        net.tick_all(20)
        leader = net.get_leader()
        assert leader is not None

        # Partition a follower
        follower_id = [n for n in net.nodes if n != leader.node_id][0]
        net.isolate(follower_id)

        # Give leader some entries
        leader.submit_command({"op": "set", "key": "x", "value": "1"})
        leader.submit_command({"op": "set", "key": "y", "value": "2"})

        # Replicate to other follower
        net.tick_all(10)

        # Reconnect - follower has empty log, will reject initially
        net.reconnect(follower_id)

        # Leader should retry with decremented next_index
        net.tick_all(30)

        # Follower should eventually catch up
        assert len(net.nodes[follower_id].log) == 2

    def test_only_commits_current_term_entries(self):
        """Leader only commits entries from current term."""
        net = SimNetwork(["A", "B", "C"])

        # Elect leader
        net.tick_all(20)
        leader = net.get_leader()
        assert leader is not None

        # Add entry from current term
        leader.submit_command({"op": "set", "key": "x", "value": "1"})
        net.tick_all(10)

        # Entry should be committed (it's from current term)
        assert leader.commit_index == 0
        assert leader.log[0].term == leader.current_term


# ==================== Integration Tests: Key-Value Store ====================

class TestKeyValueStore:
    """Integration tests for the key-value state machine."""

    def test_set_and_get(self):
        """Can set and retrieve values."""
        net = SimNetwork(["A", "B", "C"])

        # Elect leader
        net.tick_all(20)
        leader = net.get_leader()
        assert leader is not None

        # Set a value
        leader.submit_command({"op": "set", "key": "x", "value": "42"})
        net.tick_all(15)  # replicate and apply

        # Check value in leader's kv_store
        assert leader.kv_store.get("x") == "42"

    def test_multiple_commands(self):
        """Multiple commands execute in order."""
        net = SimNetwork(["A", "B", "C"])

        # Elect leader
        net.tick_all(20)
        leader = net.get_leader()
        assert leader is not None

        # Submit multiple commands
        leader.submit_command({"op": "set", "key": "x", "value": "1"})
        leader.submit_command({"op": "set", "key": "x", "value": "2"})
        leader.submit_command({"op": "set", "key": "x", "value": "3"})
        net.tick_all(20)

        # Final value should be "3" (last write wins)
        assert leader.kv_store.get("x") == "3"

    def test_all_nodes_converge(self):
        """All nodes eventually have the same state."""
        net = SimNetwork(["A", "B", "C"])

        # Elect leader
        net.tick_all(20)
        leader = net.get_leader()
        assert leader is not None

        # Submit commands
        leader.submit_command({"op": "set", "key": "x", "value": "1"})
        leader.submit_command({"op": "set", "key": "y", "value": "2"})
        net.tick_all(20)

        # All nodes should have same kv_store
        for node in net.nodes.values():
            assert node.kv_store.get("x") == "1"
            assert node.kv_store.get("y") == "2"

    def test_follower_applies_committed_entries(self):
        """Followers apply entries when they learn of commit."""
        net = SimNetwork(["A", "B", "C"])

        # Elect leader
        net.tick_all(20)
        leader = net.get_leader()
        assert leader is not None

        # Submit command
        leader.submit_command({"op": "set", "key": "x", "value": "1"})
        net.tick_all(15)

        # All followers should have applied the entry
        for node_id, node in net.nodes.items():
            if node_id != leader.node_id:
                assert node.kv_store.get("x") == "1"
                assert node.last_applied == 0


# ==================== Integration Tests: Edge Cases ====================

class TestEdgeCases:
    """Edge cases and corner scenarios."""

    def test_five_node_cluster(self):
        """5-node cluster works correctly."""
        net = SimNetwork(["A", "B", "C", "D", "E"])

        # Elect leader
        net.tick_all(20)
        leader = net.get_leader()
        assert leader is not None

        # Submit command
        leader.submit_command({"op": "set", "key": "x", "value": "1"})
        net.tick_all(15)

        # All nodes should have the entry
        for node in net.nodes.values():
            assert len(node.log) == 1

        # Commit index should advance (need 3 of 5 for majority)
        assert leader.commit_index == 0

    def test_minority_partition_cannot_elect_leader(self):
        """Minority partition cannot elect a leader."""
        net = SimNetwork(["A", "B", "C", "D", "E"])

        # Elect initial leader
        net.tick_all(20)
        initial_leader = net.get_leader()
        assert initial_leader is not None

        # Isolate 2 nodes that are NOT the leader (minority)
        non_leaders = [n for n in net.nodes if n != initial_leader.node_id]
        minority = non_leaders[:2]
        for node_id in minority:
            net.isolate(node_id)

        # Run ticks - isolated nodes cannot elect (need 3 of 5)
        initial_terms = {n: net.nodes[n].current_term for n in minority}
        net.tick_all(40)

        # Minority nodes should NOT be leaders
        for node_id in minority:
            assert net.nodes[node_id].state != NodeState.LEADER

        # Majority partition should still have a leader
        majority = [n for n in net.nodes if n not in minority]
        majority_leaders = [n for n in majority if net.nodes[n].state == NodeState.LEADER]
        assert len(majority_leaders) == 1

    def test_old_leader_rejoins(self):
        """Old leader steps down when rejoining cluster."""
        net = SimNetwork(["A", "B", "C"])

        # Elect leader
        net.tick_all(20)
        old_leader = net.get_leader()
        assert old_leader is not None
        old_leader_id = old_leader.node_id
        old_term = old_leader.current_term

        # Isolate the leader
        net.isolate(old_leader_id)

        # Remaining nodes elect new leader (term will increase)
        net.tick_all(40)

        # Verify new leader exists in majority partition
        new_leader = None
        for node_id, node in net.nodes.items():
            if node_id != old_leader_id and node.state == NodeState.LEADER:
                new_leader = node
                break
        assert new_leader is not None
        assert new_leader.current_term > old_term

        # Old leader still thinks it's leader (hasn't heard from anyone)
        assert net.nodes[old_leader_id].state == NodeState.LEADER

        # Reconnect old leader
        net.reconnect(old_leader_id)
        net.tick_all(10)

        # Old leader should step down (sees higher term)
        assert net.nodes[old_leader_id].state == NodeState.FOLLOWER
        assert net.nodes[old_leader_id].current_term == new_leader.current_term


# ==================== Integration Tests: Safety Properties ====================

class TestSafetyProperties:
    """Critical Raft safety property tests."""

    def test_uncommitted_overwrite_after_leader_change(self):
        """Uncommitted entries from old leader can be overwritten by new leader."""
        net = SimNetwork(["A", "B", "C"])

        # Elect leader
        net.tick_all(20)
        leader = net.get_leader()
        assert leader is not None
        leader_id = leader.node_id

        # Isolate leader immediately after it appends an entry (before replication)
        leader.submit_command({"op": "set", "key": "x", "value": "from_old_leader"})
        net.isolate(leader_id)

        # Old leader has uncommitted entry
        assert len(net.nodes[leader_id].log) == 1
        assert net.nodes[leader_id].commit_index == -1  # not committed

        # Remaining nodes elect new leader
        net.tick_all(40)

        # Find new leader
        new_leader = None
        for node_id, node in net.nodes.items():
            if node_id != leader_id and node.state == NodeState.LEADER:
                new_leader = node
                break
        assert new_leader is not None

        # New leader appends different entry
        new_leader.submit_command({"op": "set", "key": "x", "value": "from_new_leader"})
        net.tick_all(10)

        # New leader's entry should be committed
        assert new_leader.commit_index == 0

        # Reconnect old leader
        net.reconnect(leader_id)
        net.tick_all(20)

        # Old leader's uncommitted entry should be overwritten
        old_leader_node = net.nodes[leader_id]
        assert old_leader_node.state == NodeState.FOLLOWER
        assert len(old_leader_node.log) == 1
        assert old_leader_node.log[0].command["value"] == "from_new_leader"

    def test_follower_never_applies_uncommitted_entries(self):
        """Follower must never apply entries that aren't committed."""
        net = SimNetwork(["A", "B", "C"])

        # Elect leader
        net.tick_all(20)
        leader = net.get_leader()
        assert leader is not None

        # Submit command
        leader.submit_command({"op": "set", "key": "x", "value": "1"})

        # Tick just once - entry should replicate but commit info may not propagate
        net.tick_all(1)

        # For all nodes: last_applied should never exceed commit_index
        for node in net.nodes.values():
            assert node.last_applied <= node.commit_index, \
                f"Node {node.node_id}: last_applied ({node.last_applied}) > commit_index ({node.commit_index})"

            # If commit_index is -1, kv_store should be empty (nothing applied)
            if node.commit_index == -1:
                assert node.kv_store.get("x") is None, \
                    f"Node {node.node_id} applied uncommitted entry!"

        # Now let it fully commit
        net.tick_all(15)

        # After commit, all nodes should have applied
        for node in net.nodes.values():
            assert node.last_applied == node.commit_index
            assert node.kv_store.get("x") == "1"

    def test_committed_entry_survives_leader_crash(self):
        """Once an entry is committed, it must survive leader crashes."""
        net = SimNetwork(["A", "B", "C", "D", "E"])  # 5 nodes for clearer majority

        # Elect leader
        net.tick_all(20)
        leader = net.get_leader()
        assert leader is not None
        leader_id = leader.node_id

        # Submit and replicate command
        leader.submit_command({"op": "set", "key": "x", "value": "committed_value"})
        net.tick_all(10)

        # Verify entry is committed on leader
        assert leader.commit_index == 0
        committed_entry = leader.log[0]

        # Find which followers have the entry
        followers_with_entry = []
        for node_id, node in net.nodes.items():
            if node_id != leader_id and len(node.log) > 0:
                followers_with_entry.append(node_id)

        # Need at least 2 followers with entry (majority = 3, leader + 2 followers)
        assert len(followers_with_entry) >= 2

        # Crash the leader (isolate it)
        net.isolate(leader_id)

        # Elect new leader from remaining nodes
        net.tick_all(40)

        # Find new leader
        new_leader = None
        for node_id, node in net.nodes.items():
            if node_id != leader_id and node.state == NodeState.LEADER:
                new_leader = node
                break
        assert new_leader is not None

        # The committed entry MUST be in new leader's log
        assert len(new_leader.log) >= 1, "New leader lost committed entry!"
        assert new_leader.log[0].command["key"] == "x"
        assert new_leader.log[0].command["value"] == "committed_value"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
