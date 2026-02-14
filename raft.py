"""
Raft-Lite: A toy implementation of the Raft consensus algorithm.

Simplifications:
1. Fixed membership - no dynamic membership changes
2. Peer list hardcoded on node - no service discovery
3. In-memory only - no persistent storage
4. Synchronous tick-based timing - no real timers
5. Direct method calls (SimNetwork) - no real network RPCs
"""

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

    ELECTION_TIMEOUT = 5
    HEARTBEAT_INTERVAL = 2

    def __init__(self, node_id: str, peers: List[str], majority: int | None = None):
        self.node_id = node_id
        self.peers = peers 
        self.majority = majority or (len(peers) // 2) + 1

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
        # TODO: Implement
        # - Reject if not leader
        # - Append to log, return True
        pass
    
    # ==================== Core API: called by SimNetwork ====================

    def tick(self) -> List[Message]:
        """Advance time by one tick. Returns messages to send."""
        # TODO: Implement tick logic
        # - Leaders: send heartbeats every HEARTBEAT_INTERVAL
        # - Followers/Candidates: start election if ELECTION_TIMEOUT reached
        pass

    # sent by candidates to anyone
    def handle_request_vote(self, req: RequestVoteRequest) -> RequestVoteResponse:
        """Handle incoming RequestVote RPC."""
        # TODO: Implement vote logic
        # - Reject if req.term < current_term
        # - Grant vote if haven't voted this term and candidate's log is up-to-date
        pass

    # only for candidates
    def handle_request_vote_response(self, from_node: str, resp: RequestVoteResponse):
        """Handle response to our RequestVote RPC."""
        # TODO: Implement
        # - If majority votes received, become leader
        pass

    # sent by leader, received by everyone else
    def handle_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        """Handle incoming AppendEntries RPC (heartbeat or log replication)."""
        # TODO: Implement
        # - Reject if req.term < current_term
        # - Check log consistency at prev_log_index
        # - Append entries, update commit_index
        pass

    # only for leader
    def handle_append_entries_response(self, from_node: str, resp: AppendEntriesResponse):
        """Handle response to our AppendEntries RPC."""
        # TODO: Implement
        # - Update next_index/match_index for follower
        # - Try to advance commit_index if majority replicated
        pass


    # ==================== Internal Helpers ====================

    def _start_election(self):
        """Transition to candidate and request votes."""
        # TODO: Implement
        pass

    def _become_follower(self, term: int):
        """Step down to follower state."""
        # TODO: Implement
        pass

    def _become_leader(self):
        """Transition to leader state."""
        # TODO: Implement
        pass

    def _send_heartbeats(self) -> List[Message]:
        """Send AppendEntries to all peers."""
        # TODO: Implement
        pass

    def _apply_committed(self):
        """Apply committed entries to kv_store."""
        # TODO: Implement
        pass

    def _is_log_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
        """Check if node's log is at least as up-to-date as the callers. usually used by nodes to determine if they should vote for the candidate"""
        # TODO: Implement
        pass
    
    # only for follower and candidate 
    def _reset_election_timer(self):
        """Reset election timeout counter."""
        self.ticks_elapsed = 0

    # only for leader 
    def _try_advance_commit(self):
        """Advance commit_index if majority has replicated."""
        # TODO: Implement
        pass
    
    def _send_append_entries(self, peer: str) -> List[Message]:
        """Send AppendEntries to a peer."""
        # TODO: Implement
        pass
