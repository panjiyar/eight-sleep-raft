import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any


class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """A single entry in the Raft log."""
    term: int
    command: Dict[str, Any]  # {"op": "set", "key": "x", "value": "1"}


@dataclass
class RequestVoteRequest:
    term: int  # candidate's term
    candidate_id: str  # candidate requesting vote
    # the following are for consistency check so that a candidate can't win an election if its log is behind:
    # raft likes to use both term and log length to decide if a follower will vote for a candidate
    last_log_index: int  # index of candidate's last log entry (for log comparison)
    last_log_term: int  # term of candidate's last log entry (for log comparison)


@dataclass
class RequestVoteResponse:
    term: int
    vote_granted: bool 


@dataclass
class AppendEntriesRequest:
    term: int 
    leader_id: str  # so follower can redirect clients

    # the following are for consistency check:
    prev_log_index: int 
    prev_log_term: int 

    entries: List[LogEntry]  # log entries to store (empty for heartbeat), a list so we can batch a lot of entries if needed 
    leader_commit: int  # leader's commit_index (tells follower what's safe to apply)


@dataclass
class AppendEntriesResponse:
    term: int 
    success: bool 


@dataclass
class Message:
    """Outgoing message from a node."""
    sender: str
    target: str
    payload: Any  # RequestVoteRequest, AppendEntriesRequest, etc.


class RaftNode:
    """
    A single Raft node implementing the core consensus algorithm.

    This node has no knowledge of the network - it only:
    1. Processes incoming RPCs and returns responses
    2. On tick(), returns a list of outgoing messages to send
    """

    ELECTION_TIMEOUT_MIN = 5
    ELECTION_TIMEOUT_MAX = 15
    HEARTBEAT_INTERVAL = 2

    def __init__(self, node_id: str, peers: List[str], majority: Optional[int] = None):
        self.node_id = node_id
        self.peers = peers
        self.majority = majority or (len(peers) // 2) + 1 # lets assume that the majority is never less than (n/2) + 1

        self.election_timeout = random.randint(self.ELECTION_TIMEOUT_MIN, self.ELECTION_TIMEOUT_MAX)

        # Persistent state (would survive crashes in real Raft)
        self.current_term = 0 
        self.voted_for: Optional[str] = None 
        self.log: List[LogEntry] = [] 

        # Volatile state
        self.state = NodeState.FOLLOWER 
        self.commit_index = -1  # highest log index known to be committed (safe to apply); followers get this from leader
        self.last_applied = -1  # always <= commit_index
        self.leader_id: Optional[str] = None # useful for followers to know who the leader is (for redirecting clients)

        # Leader state (reinitialized after election)
        self.next_index: Dict[str, int] = {}  # for each peer: next log index to send them, optimistic by nature. this is what the leader will try to send.
        self.match_index: Dict[str, int] = {}  # for each peer: highest log index known to be replicated on them, conservative by nature.  the leader knows that the peer def has this.

        # Timing
        self.ticks_elapsed = 0

        # Votes received (as candidate)
        self.votes_received: set = set()  # not using count to prevent double voting

        # State machine (the thing we're replicating)
        self.kv_store: Dict[str, str] = {}  # simple key-value store

    # ==================== Client API ====================

    def submit_command(self, command: Dict[str, Any]) -> bool:
        """Submit a command. Only succeeds if node is leader."""
        if self.state != NodeState.LEADER:
            return False
        self.log.append(LogEntry(term=self.current_term, command=command))
        return True
    
    # ==================== Core API: called by SimNetwork ====================

    def tick(self) -> List[Message]:
        """Advance time by one tick. Returns messages to send."""
        self.ticks_elapsed += 1
        messages = []

        if self.state == NodeState.LEADER:
            # Leaders send AppendEntries every tick (replication or heartbeat)
            messages = self._send_heartbeats()
        else:
            # Followers/Candidates start election if election_timeout reached
            if self.ticks_elapsed >= self.election_timeout:
                self._start_election()
                # Send RequestVote to all peers
                for peer in self.peers:
                    last_log_index = len(self.log) - 1
                    last_log_term = self.log[-1].term if self.log else 0
                    req = RequestVoteRequest(
                        term=self.current_term,
                        candidate_id=self.node_id,
                        last_log_index=last_log_index,
                        last_log_term=last_log_term
                    )
                    messages.append(Message(sender=self.node_id, target=peer, payload=req))

        return messages

    # ==================== RequestVote RPC ====================

    def handle_request_vote(self, req: RequestVoteRequest) -> RequestVoteResponse:
        """Handle incoming RequestVote RPC."""
        # Always update term if we see a higher one
        if req.term > self.current_term:
            self._become_follower(req.term)

        # Reject stale term
        if req.term < self.current_term:
            return RequestVoteResponse(self.current_term, False)

        # Can we vote for this candidate?
        already_voted_for_other = self.voted_for is not None and self.voted_for != req.candidate_id
        if already_voted_for_other:
            return RequestVoteResponse(self.current_term, False)

        # Is candidate's log up-to-date?
        if not self._is_log_up_to_date(req.last_log_index, req.last_log_term):
            return RequestVoteResponse(self.current_term, False)

        # Grant vote
        self.voted_for = req.candidate_id
        self._reset_election_timer()
        return RequestVoteResponse(self.current_term, True)

    def handle_request_vote_response(self, from_node: str, resp: RequestVoteResponse):
        """Handle response to our RequestVote RPC."""
        if resp.term > self.current_term:
            self._become_follower(resp.term)
            return

        # Ignore if not candidate or stale response
        if self.state != NodeState.CANDIDATE or resp.term < self.current_term:
            return

        if resp.vote_granted:
            self.votes_received.add(from_node)
            if len(self.votes_received) >= self.majority:
                self._become_leader()

    # ==================== AppendEntries RPC ====================

    def handle_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        """Handle incoming AppendEntries RPC."""
        # Always update term if we see a higher one
        if req.term > self.current_term:
            self._become_follower(req.term)

        # Reject stale term
        if req.term < self.current_term:
            return AppendEntriesResponse(self.current_term, False)

        # Valid leader - reset timer and record leader
        self._reset_election_timer()
        self.leader_id = req.leader_id
        if self.state == NodeState.CANDIDATE:
            self.state = NodeState.FOLLOWER

        # Log consistency check
        if req.prev_log_index >= 0:
            if req.prev_log_index >= len(self.log):
                return AppendEntriesResponse(self.current_term, False)
            if self.log[req.prev_log_index].term != req.prev_log_term:
                return AppendEntriesResponse(self.current_term, False)

        # Append new entries (truncate conflicts)
        for i, entry in enumerate(req.entries):
            idx = req.prev_log_index + 1 + i
            if idx < len(self.log):
                if self.log[idx].term != entry.term:
                    self.log = self.log[:idx]  # truncate
                    self.log.append(entry)
            else:
                self.log.append(entry)

        # Update commit index
        if req.leader_commit > self.commit_index:
            self.commit_index = min(req.leader_commit, len(self.log) - 1)
            self._apply_committed()

        return AppendEntriesResponse(self.current_term, True)

    def handle_append_entries_response(self, from_node: str, resp: AppendEntriesResponse):
        """Handle response to our AppendEntries RPC."""
        if resp.term > self.current_term:
            self._become_follower(resp.term)
            return

        if self.state != NodeState.LEADER:
            return

        if resp.success:
            # Follower accepted - they now have everything we sent
            self.match_index[from_node] = len(self.log) - 1
            self.next_index[from_node] = len(self.log)
            self._try_advance_commit()
        else:
            # Follower rejected - decrement and retry next heartbeat
            self.next_index[from_node] = max(0, self.next_index[from_node] - 1)

    # ==================== State Transitions ====================

    def _start_election(self):
        """Start a new election."""
        self.current_term += 1
        self.state = NodeState.CANDIDATE
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self._reset_election_timer()
        # Single-node cluster: already have majority
        if len(self.votes_received) >= self.majority:
            self._become_leader()

    def _become_follower(self, term: int):
        """Step down to follower."""
        self.state = NodeState.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self._reset_election_timer()

    def _become_leader(self):
        """Become leader."""
        self.state = NodeState.LEADER
        self.leader_id = self.node_id
        for peer in self.peers:
            self.next_index[peer] = len(self.log)
            self.match_index[peer] = -1
        self.ticks_elapsed = 0

    # ==================== Helper Methods ====================

    def _reset_election_timer(self):
        """Reset election timeout."""
        self.ticks_elapsed = 0
        self.election_timeout = random.randint(self.ELECTION_TIMEOUT_MIN, self.ELECTION_TIMEOUT_MAX)

    def _is_log_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
        """Check if candidate's log is at least as up-to-date as ours."""
        my_last_term = self.log[-1].term if self.log else 0
        my_last_index = len(self.log) - 1

        if last_log_term != my_last_term:
            return last_log_term > my_last_term
        return last_log_index >= my_last_index

    def _send_heartbeats(self) -> List[Message]:
        """Send AppendEntries to all peers."""
        return [self._send_append_entries(peer) for peer in self.peers]

    def _send_append_entries(self, peer: str) -> Message:
        """Create AppendEntries message for a peer."""
        next_idx = self.next_index.get(peer, len(self.log))
        prev_idx = next_idx - 1
        prev_term = self.log[prev_idx].term if prev_idx >= 0 else 0

        req = AppendEntriesRequest(
            term=self.current_term,
            leader_id=self.node_id,
            prev_log_index=prev_idx,
            prev_log_term=prev_term,
            entries=self.log[next_idx:],
            leader_commit=self.commit_index
        )
        return Message(self.node_id, peer, req)

    def _try_advance_commit(self):
        """Advance commit_index if majority has replicated."""
        for n in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[n].term != self.current_term:
                continue  # Only commit entries from current term
            # Count replicas (including self)
            count = 1 + sum(1 for p in self.peers if self.match_index.get(p, -1) >= n)
            if count >= self.majority:
                self.commit_index = n
                self._apply_committed()
                break

    def _apply_committed(self):
        """Apply committed entries to state machine."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            cmd = self.log[self.last_applied].command
            if cmd.get("op") == "set":
                self.kv_store[cmd["key"]] = cmd["value"]
