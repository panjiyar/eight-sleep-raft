# eight-sleep-raft

## Project Description

For simplicity:

1. Storage is an in-memory key-value store
2. Sync tick-based timing instead of real timers to deterministically simulate time/crashes/partitions to test edge cases - look at SimNetwork on test_raft.py
3. Direct method calls to a node instead of messages sent over the network
4. Node membership is fixed and stored on the node. In the real world, we could use some configuration managing service.

To get a high level understanding of this raft implementation, I'd recommmend looking at https://github.com/panjiyar/eight-sleep-raft/blob/54583c883e493ce6acd52481f27baf3a85d76c18/raft.py
It gives a good overview of the node definition, and if that's clear, then the rest is just implementation details.

I chose to do TDD (test driven development) for this project, so you'll see a lot of tests. Raft is a solved problem, so its easy to write tests for it. The tests are more to ensure that the implementation is correct.

## Prerequisites

- Python 3

## Setup

```bash
# Clone the repo
git clone https://github.com/panjiyar/eight-sleep-raft
cd eight-sleep-raft

# Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate
```

## Running Tests

```bash
pytest test_raft.py
```
