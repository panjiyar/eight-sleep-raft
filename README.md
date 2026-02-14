# eight-sleep-raft

## Project Description

For simplicity:

1. Storage is an in-memory key-value store
2. Sync tick-based timing instead of real timers to deterministically simulate time/crashes/partitions to test edge cases - look at SimNetwork on test_raft.py
3. Direct method calls to a node instead of messages sent over the network
4. Node membership is fixed and stored on the node. In the real world, we could use some configuration managing service.

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

# Install dependencies
pip install -r requirements.txt
```

## Running Tests

```bash
pytest test_raft.py
```
