/*

 Actors were defined in the 1973 [paper](https://arxiv.org/vc/arxiv/papers/1008/1008.1459v8.pdf) by Carl Hewitt but have been popularized by the Erlang language, and used for example at Ericsson with great success to build highly concurrent and reliable telecom systems.

## Refs

* [Swift Actors Proposal](https://github.com/DougGregor/swift-evolution/blob/actors/proposals/nnnn-actors.md)
* [JADE - an open source platform for peer-to-peer agent based applications](https://jade.tilab.com/)
    * trading example https://jade.tilab.com/doc/tutorials/JADEProgramming-Tutorial-for-beginners.pdf
* [AKKA](https://doc.akka.io/docs/akka/snapshot/general/index.html)

Streams

* [Reactive Streams](https://www.reactive-streams.org/)
    * https://github.com/reactive-streams/reactive-streams-js/
* [kafks vs akka streams](https://dzone.com/articles/comparing-akka-streams-kafka-streams-and-spark-str)

## The Actor Model

What is an Actor? https://doc.akka.io/docs/akka/snapshot/general/actors.html

actors form hierarchies and are the smallest unit when building an application

a computational model that expresses exactly what it means for computation to be distributed. The processing units—Actors—can only communicate by exchanging messages and upon reception of a message an Actor can do the following three fundamental actions:

1. send a finite number of messages to Actors it knows
2. create a finite number of new Actors
3. designate the behavior to be applied to the next message

An actor is a container for State, Behavior, a Mailbox, Child Actors and a Supervisor Strategy. All of this is encapsulated behind an Actor Reference. One noteworthy aspect is that actors have an explicit lifecycle, they are not automatically destroyed when no longer referenced

the actor model is a mathematical model of concurrent computation that treats "actors" as the universal primitives of concurrent digital computation. In response to a message that it receives, an actor can make local decisions, create more actors, send more messages, and determine how to respond to the next message received.

[Objects vs Boxes vs Actors vs Agents](https://www.macs.hw.ac.uk/~rs46/posts/2014-02-03-objects-boxes-actors-agents.html#:~:text=In%20response%20to%20a%20message,specified%20agents%20at%20its%20core.)

Actors should be like nice co-workers: do their job efficiently without bothering everyone else needlessly and avoid hogging resources. Translated to programming this means to process events and generate responses (or more requests) in an event-driven manner. Actors should not block (i.e. passively wait while occupying a Thread) on some external entity—which might be a lock, a network socket, etc.—unless it is unavoidable; in the latter case see Blocking Needs Careful Management.
Do not pass mutable objects between actors. In order to ensure that, prefer immutable messages. If the encapsulation of actors is broken by exposing their mutable state to the outside, you are back in normal Java concurrency land with all the drawbacks.
Actors are made to be containers for behavior and state, embracing this means to not routinely send behavior within messages (which may be tempting using Scala closures). One of the risks is to accidentally share mutable state between actors, and this violation of the actor model unfortunately breaks all the properties which make programming in actors such a nice experience.
The top-level actor of the actor system is the innermost part of your Error Kernel, it should only be responsible for starting the various sub systems of your application, and not contain much logic in itself, prefer truly hierarchical systems. This has benefits with respect to fault-handling (both considering the granularity of configuration and the performance) and it also reduces the strain on the guardian actor, which is a single point of contention if over-used.

* [Supervision and Monitoring](https://doc.akka.io/docs/akka/snapshot/general/supervision.html)
* Actor Reference
* Actor Path & Address  (https://doc.akka.io/docs/akka/snapshot/general/addressing.html)
* State
* Behavior
* Mailbox
* Child Actors
* Supervisor Strategy ( Fault Tolerance strategies )
* Routers https://doc.akka.io/docs/akka/snapshot/typed/routers.html
* Dispatchers https://doc.akka.io/docs/akka/snapshot/typed/dispatchers.html
* Actor Termination ( “dead letter mailbox”  )

Examples
 * chat room (https://doc.akka.io/docs/akka/snapshot/typed/actors.html#a-more-complex-example)

## Location Transparency

https://doc.akka.io/docs/akka/snapshot/general/remoting.html

### Distributed by Default

Everything in Akka is designed to work in a distributed setting: all interactions of actors use purely message passing and everything is asynchronous.

ensure that all functions are available equally when running within a single JVM or on a cluster of hundreds of machines. The key for enabling this is to go from remote to local by way of optimization instead of trying to go from local to remote by way of generalization. See this classic paper for a detailed discussion on why the second approach is bound to fail.
https://doc.akka.io/docs/misc/smli_tr-94-29.pdf

-->  all messages sent over the wire must be serializable.

Another consequence is that everything needs to be aware of all interactions being fully asynchronous, which in a computer network might mean that it may take several minutes for a message to reach its recipient (depending on configuration). It also means that the probability for a message to be lost is much higher than within one JVM, where it is close to zero (still: no hard guarantee!).

### Peer-to-Peer vs. Client-Server

The design of remoting is driven by two (related) design decisions:

Communication between involved systems is symmetric: if a system A can connect to a system B then system B must also be able to connect to system A independently.
The role of the communicating systems are symmetric in regards to connection patterns: there is no system that only accepts connections, and there is no system that only initiates connections.
The consequence of these decisions is that it is not possible to safely create pure client-server setups with predefined roles (violates assumption 2). For client-server setups it is better to use HTTP or Akka I/O.

### Marking Points for Scaling Up with Routers

see [Routers](https://doc.akka.io/docs/akka/snapshot/typed/routers.html)

### Behavior

 Behavior means a function which defines the actions to be taken in reaction to the message at that point in time, say forward a request if the client is authorized, deny it otherwise.

 behavior may change over time, e.g. because different clients obtain authorization over time, or because the actor may go into an “out-of-service” mode and later come back.


## Persistence

Event Sourcing
Replicated Event Sourcing
(Projection)
Command Query Responsibility Segregation (CQRS

## Cluster

## Streams

Actors might prove to be an appealing alternative to objects if you are in need of scalability or concurrency in your agents. I

## What is the difference between actors (Akka) and agents (JADE) in distributed systems? [closed]
https://stackoverflow.com/questions/15295504/what-is-the-difference-between-actors-akka-and-agents-jade-in-distributed-sy

    their Agents do the serial message-processing part of actors, but they lack supervision and therefore fault-tolerance, and they seem to encourage either blocking or polling while Akka’s actors are fully event-driven and hence consume less resources (threads)

https://www.macs.hw.ac.uk/~rs46/posts/2013-06-22-overloaded-agents.html

### Multi-Agent Systems are Concurrent or Distributed Systems

The actor programming model is popularised by the successful Erlang language. An observation often overlooked is the commonality between actors and agents. They both communicate with message passing, they have independent beliefs (isolated internal state), and are scheduled either independently. StackOverflow is a good place to seek clarification on the differences:

* ["Is it reasonable to view highly autonomous actors as agents?"](https://stackoverflow.com/questions/1161179/is-it-reasonable-to-view-highly-autonomous-actors-as-agents/1161717#1161717)
    Yes, there are differences. For very simple agents, actors and agents might be the same thing. However, by "autonomous agents" one, or, at least, I, usually assume something like, for example, a Belief-Desire-Intention model, where the agent models internally an abstraction of the environment it finds itself in, and the agents it interacts with, so that it can make plans on how to interact with that environment to achieve it's goals.
    While an actor can sure have all this, a single agent might just as well be composed of multiple actors, acting jointly to handle different parts of the BDI framework. An actor is, for all intents, a scheduling unit. If your agents are essentially linear and single-thread, they fit. If they do parallel work internally, you want multiple actors for each agent.
    An actor is not an agent in the same way that a thread is not an agent. However either may be used in order to realise the goal of creating an autonomous agent. Case in point, JADE's Agents sit on top of Java Threads.


What can established parallel programming models and well-engineered distributed systems offer the agent community? A great deal, in fact. Three popular programming models for parallelism are the actor model in Erlang (J Armstrong, 1996), the continuation-passing style in Cilk (R Blumofe, 1996) and parallel message passing with MPI (W Gropp, 1999). Moreover, mult-agent system implementations can benefit from distributed systems literature e.g. load balancing agents (from (A Zain, 2005)) and automatic recovery of dead agents (from (M Logan, 2010)).

### Erlang VM: A industry quality distributed VM for Actors (aka Agents)

Take the Erlang virtual machine as a case study. It can be deployed at large scale, on clusters or wide are networks, and the Erlang runtime system supports the high level programming features e.g. supervision behaviours for fault tolerance. That is to say actors can:

* be started dynamically
* live short or long lives
* communicate with other actors with message passing
* be proactive or reactive
* be automatically restarted if they fail.

This sounds like a very useful VM for a multi-agent system, right?


*/