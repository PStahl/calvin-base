# Fault tolerance

## What is this?

Calvin lets you replicate an actor. By doing so, you achieve increased redundancy
and thereby increased reliability. In case of runtimes failing, or leaving the
network, having multiple replicas on different runtimes allows for the computations
to continue, and result being produced, despite the event of failure.

## Fan-in Fan-out connectivity model

When replicating an actor, there will be new ports and new port endpoints created.

For example, if we have three actors A, B1 and C:<br />
A - - > B1 --> C
and then replicate B1, thereby creating a new identical copy, B2, we will have:<br />
         --> B1 -- <br />
       /           \ <br />
A -- <               --> C <br />
       \           / <br />
         --> B2 -- <br />


## Example

See example/README for an example.

## Replacing ReliabilityCalculator and TaskScheduler

The default reliability calculator uses the runtimes
mean-time-between-failure to calculate their reliability, and
the default task scheduler sorts the nodes by reliability.

An example of the calvin.conf file:
{<br />
    "global": {<br />
        "reliability_calculator": "ReliabilityCalculator",<br />
        "task_scheduler": "ReliabilityScheduler",<br />
        "default_mtbf": "20",<br />
        "default_replication_time": "1.0",<br />
    }<br />
}
The values "reliability_calcultor" and "task_scheduler" can be changed to other
classes which then shoukd be created in calvin/runtime/north and implement
functions "get_reliability" and "sort" respectively. See the default classes for
examples.

The default reliability calculator uses the values "default_mtbf" and "default_replication_time"
before such times has been registered and stored in the storage.
