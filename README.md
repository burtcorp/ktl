# ktl

`ktl`, short for "Kafka Control Tool", is a command-line tool that attempts to
make the management of Kafka clusters that host a lot of topics and partitions
easier.

## Motivation

The main motivation for creating `ktl` was to automate, as much as possible, the
process of moving partition leadership between brokers when adding, removing or
replacing brokers, as (unfortunately) the tools bundled with Kafka are rather
manual and quite cumbersome to use when one host a lot (> 1000) of topics /
partitions.

I highly recommend that you read the [official documentation](https://kafka.apache.org/documentation.html#operations)
regarding operating Kafka before reading further, as quite a lot of the
terminology is explained in the documentation and not necessarily explained in
any great depth in this README but merely mentioned.

## Usage

`ktl` supports a couple of different commands for common tasks such as
performing preferred replica elections, shuffling partition leadership, checking
consumer lag, creating topics, and so on.
Most of the commands supports the usage of regular expressions for filtering
which topics that will be acted upon.
For example it's possible to perform preferred replica elections or reassign
partitions for a subset of topics.

```shell
$ ktl --help
Commands:
  ktl cluster SUBCOMMAND ...ARGS   # Commands for managing cluster(s)
  ktl consumer SUBCOMMAND ...ARGS  # Commands for managing consumers
  ktl help [COMMAND]               # Describe available commands or one specific command
  ktl topic SUBCOMMAND ...ARGS     # Commands for managing topics
```

### Partition reassignments

A "**partition reassignment**" (or just "reassignment" for short) is basically a
JSON document that describes which brokers that should be leader and/or replica
for certain partitions.
There are currently three subcommands of the `cluster` command that will generate
reassignments: `migrate-broker`, `shuffle` and `decommission-broker`.

When starting a new reassignment, `ktl` will write the reassignment JSON to a
`reassign` znode under a `/ktl`prefix in ZooKeeper, so that it's possible to
track the progress of a reassignment.

The `shuffle` subcommand can either perform a random reassignment of partitions,
or it can use [rendezvous hashing](http://en.wikipedia.org/wiki/Rendezvous_hashing),
which will minimize the number of partitions that has to move between replicas
when adding or removing brokers.

To start a random reassignment of the replica assignment of partitions matching
`^test.*`, but leaving the rest alone:
```shell
$ ktl cluster shuffle '^test.*' -z localhost:2181/test
```
To do the same thing but using rendezvous hashing:
```shell
$ ktl cluster shuffle '^test.*' -R -z localhost:2181/test
```

#### Dealing with ZooKeeper's znode limit

If the resulting reassignment JSON is greater than 1 MB (which is usually the
hard limit for a single znode), `ktl` will split the reassignment into smaller
slices and store the "overflow" in ZooKeeper under a `overflow` znode (in
properly-sized slices) so that it's available for further invocations.

The previously mentioned "reassignment" commands also take an optional `-l /
--limit` parameter to limit the number of partitions that will be included in
the reassignment, as it can be beneficial to perform a reassignment in steps
rather than all at once, as moving replicas for a lot of partitions will slow
down your producers and consumers.
The default is however to reassign as many partitions as possible.

Note that `ktl` will not wait for the reassignment to finish, but to proceed
with the next slice of the reassignment is just a matter of running the same
command again and it'll prompt you whether to continue you were you left off
or generate a new reassignment.

### Managing topics

While the main motivation for `ktl` was to deal with partition reassignments,
there are also some commands for managing topics, most of which merely wraps the
`kafka-topics.sh` tool that is bundled with Kafka, but with a slightly different
interface.

The most notable difference is the `reaper` subcommand that marks topics that
are considered empty for deletion, i.e. where all partitions of a topic have
the same earliest and latest offset.
The `reaper` subcommand also supports the usage of regular expressions, so to
mark all topics matching `^test-\w+'` for deletion:

```shell
$ ktl topic reaper '^test-\w+' -z localhost:2181/test
```

The default is to remove all empty topics, so be cautious.

## Copyright

© 2015 Burt AB, see LICENSE.txt (BSD 3-Clause).
