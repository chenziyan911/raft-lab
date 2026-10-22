// scenarios/heartbeat.js
// Raft heartbeats: the leader sends empty AppendEntries messages so that
// followers keep recognising it and do not start new elections.
// Self-contained and tick-based (no real timers) so the output is deterministic.
 
const HEARTBEAT_INTERVAL = 2; // leader sends a heartbeat every 2 ticks
 
class Network {
  deliver(from, to, type) {
    console.log(`NETWORK DELIVERED: ${from} -> ${to} ${type}`);
  }
}
 
class Node {
  constructor(id, electionTimeout) {
    this.id = id;
    this.state = "FOLLOWER";
    this.term = 0;
    this.leaderId = null;
    this.online = true;
    this.lastHeartbeat = 0; // tick when a valid heartbeat last arrived
    this.lastSent = 0; // tick when this node (as leader) last sent one
    this.electionTimeout = electionTimeout;
  }
 
  receiveHeartbeat(msg, tick) {
    if (msg.term < this.term) return false; // stale leader, ignore
    this.term = msg.term;
    this.state = "FOLLOWER";
    this.leaderId = msg.leaderId;
    this.lastHeartbeat = tick;
    console.log(`${this.id} RESET ELECTION TIMER (LEADER ${msg.leaderId}, TERM ${this.term})`);
    return true;
  }
}
 
class Cluster {
  constructor(nodes, network) {
    this.nodes = nodes;
    this.network = network;
  }
 
  get majority() {
    return Math.floor(this.nodes.length / 2) + 1;
  }
 
  leader() {
    return this.nodes.find((n) => n.state === "LEADER" && n.online);
  }
 
  sendHeartbeats(leader, tick) {
    console.log(`${leader.id} -> HEARTBEAT (TERM ${leader.term})`);
    for (const peer of this.nodes) {
      if (peer === leader || !peer.online) continue;
      this.network.deliver(leader.id, peer.id, "HEARTBEAT");
      peer.receiveHeartbeat({ term: leader.term, leaderId: leader.id }, tick);
    }
    leader.lastSent = tick;
  }
 
  startElection(candidate, tick) {
    candidate.state = "CANDIDATE";
    candidate.term += 1;
    candidate.lastHeartbeat = tick;
    console.log(`${candidate.id} ELECTION TIMEOUT: no heartbeat received`);
    console.log(`${candidate.id} -> CANDIDATE (TERM ${candidate.term})`);
 
    let votes = 1; // candidate votes for itself
    for (const peer of this.nodes) {
      if (peer === candidate || !peer.online) continue;
      this.network.deliver(candidate.id, peer.id, "REQUESTVOTE");
      if (candidate.term > peer.term) {
        peer.term = candidate.term;
        votes += 1;
      }
    }
    console.log(`Votes = ${votes}/${this.nodes.length}`);
    if (votes >= this.majority) {
      candidate.state = "LEADER";
      candidate.leaderId = candidate.id;
      candidate.lastSent = tick;
      console.log(`${candidate.id} -> LEADER`);
    }
  }
 
  tick(t) {
    const leader = this.leader();
    if (leader && t - leader.lastSent >= HEARTBEAT_INTERVAL) {
      this.sendHeartbeats(leader, t);
    }
    for (const n of this.nodes) {
      if (!n.online || n.state === "LEADER") continue;
      if (t - n.lastHeartbeat >= n.electionTimeout) {
        this.startElection(n, t);
        return true; // election started, stop this phase
      }
    }
    return false;
  }
}



const network = new Network();
const A = new Node("A", 6);
const B = new Node("B", 5);
const C = new Node("C", 8); // staggered timeouts: A times out before C
const cluster = new Cluster([A, B, C], network);
 
console.log("\n====================");
console.log("SCENARIO: HEARTBEAT");
console.log("====================\n");
 
// Start from an already elected leader (result of the leader_election scenario)
B.state = "LEADER";
B.term = 1;
B.leaderId = "B";
A.term = 1;
A.leaderId = "B";
C.term = 1;
C.leaderId = "B";
console.log("B is LEADER (TERM 1)\n");
 
console.log("--- Phase 1: leader alive, heartbeats sent ---");
for (let t = 1; t <= 8; t++) {
  console.log(`[tick ${t}]`);
  cluster.tick(t);
}
console.log("\nOBSERVATION: followers kept resetting their timers, so no election occurred.");
 
console.log("\n--- Phase 2: leader crashes, heartbeats stop ---");
B.online = false;
console.log("B has failed.\n");
for (let t = 9; t <= 30; t++) {
  console.log(`[tick ${t}]`);
  if (cluster.tick(t)) break;
}
 
console.log("\nOBSERVATION:");
console.log("Heartbeats carry no new data, but they renew the leader's authority.");
console.log("Without them, a follower times out and starts a new election.");
 