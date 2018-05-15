Carlyle
---

[![Build Status](https://travis-ci.org/paradoxical-io/carylyle.svg?branch=master)](https://travis-ci.org/paradoxical-io/carylyle)

A queue batch track fanout service

## What is Carlyle?

Carlyle solves the problem of determistically knowing when a batch of items in a queue is completed.  Carlyle makes tracking fan-out events sourced from a single event possible.

## Get Carlyle

```
docker pull paradoxical-io/carlyle
```

https://hub.docker.com/r/paradoxical/carlyle/

## Requirements

- Redis (optional)
- MySQL 5.7+

### Run Carlyle

```
docker run -it -p 8888:8888 -p 9990:9990\
  --network carlyle_default \
  paradoxical/carlyle:1.0-SNAPSHOT server
```

#### Commands

- `server`: Runs the server
- `migrate-db`: Creates or migrates the backing datastore

#### Env Flags

- `REDIS_URL`
- `REDIS_PORT` (default 6379)
- `REDIS_REQUEST_TIMEOUT` (default 10 seconds, format of "X seconds")
- `DB_JDBC_URL`
- `DB_USERNAME`
- `DB_PASSWORD`
- `DB_MAX_POOL_SIZE` (default 10)
- `DB_IDLE_TIMEOUT_DURATION` (default 5 seconds, format of "X seconds")

### API Format

For custom clients, it is important to understand that the payload response from adding items to a batch is of the form

```
{
   "id": <uuid>,
   "upto": Long
}
```

This one block should be _expanded_ on the client side to be unique id's of the form

```
batch_id:<uuid of id>:0
batch_id:<uuid of id>:1
batch_id:<uuid of id>:2
batch_id:<uuid of id>:...
batch_id:<uuid of id>:<upto -1>
```

I.e. 1 payload response maps to `upto` expanded ids.  Messages should be given one of these unique ID's.

Carlyle is optimized to work in chunks of 64, so it is _best_ to try and maximize the calls to "addItems" in at least 64 items. There's no harm in doing less, it's just extra storage and wire overhead. 

## Why Carlyle?

> As late as 1775, the most exhaustive English dictionary yet written contained no word to describe the act of standing in line. In 1837, in a history of the French Revolution, Thomas Carlyle carefully defined the method through which revolutionaries waited their turn, writing that "the Bakers shops have got their Queues, or Tails; their long strings of purchasers, arranged in a tail, so that the first to come be the first served."

## How does Carlyle work?

Wen working in any resilient distributed system invariably queues come into play. You fire events to be handled into a queue, and you can horizontally scale workers out to churn through events.

One thing though that is difficult to do is to answer a question of when is a batch of events finished? This is a common scenario when you have a single event causing a fan-out of other events. Imagine you have an event called ProcessCatalog and a catalog may have N catalog items. The first handler for ProcessCatalog may pull some data and fire N events for catalog items. You may want to know when all catalog items are processed by downstream listeners for the particular event.

It may seem easy though right? Just wait for the last item to come through. Ah, but distributed queues are loosely ordered. If an event fails and retries what used to be the last event is no longer the last.

What about having a counter somewhere? Well the same issue arises on event failure. If an event decrements the counter twice (because it was retried) now the counter is no longer deterministic.

To do this thoroughly you need to track every individual item and ack it as its processed. That would work but it could be really costly. Imagine tracking 1B items some data store for every batch!

Lets pretend we go this route though, what else do we need to do? We need the concept of a batch, and items to track in the batch. If we have a workflow of opening a batch (creating a new one), adding items to it, then sealing the batch (no more items can be added to it) we can safely determine if the batch is empty. Without the concept of a close/seal you can have situations where you open a batch, fire off N events, N events are consumed ack’d by downstream systems, then you close the batch. When all N are ack’d by downstream systems they can’t know that there are no more incoming items since the batch is still open. Only when the batch is closed can you tell if the batch has been fully consumed. To that note, both the producer AND consumer need to check if the batch is closed. In the previous example, if all N items are ack’d before the batch is closed, when you go to close the batch it needs to return back that the batch is empty! In the other situation if the batch is closed, then all N items are ack’d the last item to be ack’d needs to return that the batch is empty.

Back to the problem of storing and transferring N batch item ID’s though. What if instead of storing each item you leveraged a bitfield representing a set of items? Now instead of N items you only need N bits to logically track every item. But you also may now need to send N logical ID’s back to the client. We can also get around that by knowing that anytime you add a batch of items to a batch, for example, adding 1000 items to batch id 1, that this sub group batch can be inserted as a unique hash corresponding to a bitfield set to all 1’s with 1000 bits (any extra bits set to 0 and ignored).

Returning to the client all you need to send back is the hash and how many items are related to that hash. Now deterministic id’s can be created client side that are of the form `batchId:hash:index`. When a client goes back to ack a message (or batch of messages) the ID contains enough information to:

1 .Locate all other hashes related to the batch

2. Get the bitfield for the hash

3. Flip the appropriate bit represented by the index

Something maybe like this

```
case class BatchItemGroupId(value: UUID) extends UuidValue

object BatchItemId {
  def apply(batchId: BatchId, batchItemGroupId: BatchItemGroupId, index: Long): BatchItemId = BatchItemId(s"${batchId.value}:$batchItemGroupId:$index")

  def generate(batchId: BatchId, batchItemGroupId: BatchItemGroupId, numberOfItems: Int): Iterable[BatchItemId] = {
    Stream.from(0, step = 1).take(numberOfItems).map(idx => apply(batchId, batchItemGroupId, idx))
  }

  def generate(batchId: BatchId, batchItemGroupId: List[BatchItemGroupInsert]): Iterable[BatchItemId] = {
    batchItemGroupId.toStream.flatMap(group => generate(batchId, group.id, group.upto))
  }
}

case class BatchItemId(value: String) extends StringValue {
  val (batchId, batchItemGroupId, index) = value.split(':').toList match {
    case batch::hash::index::_ => (BatchId(batch.toLong), BatchItemGroupId(UUID.fromString(hash)), index.toLong)
    case _ => throw new RuntimeException(s"Invalid batch item id format $value")
  }
}


case class BatchId(value: Long) extends LongValue
```

We also need an abstraction on top of a byte array that lets us toggle bits in it. It also lets you count how many bits are set. We’ll need to know that so we can answer the question of “is this subgroup hash empty”.

If we’re smart about it we can compress the group size using bitfields stored as long's in MySQL. If MySQL supported arbitrary long blobs with atomic bit operations we could compress even _further_!

Now that we have that, all our sql needs to do is given a subgroup batch, pull the bitfield, put the bitfield into a BitGroup, and flip the appropriate bits using atomic bit shifting operations. To determine if a batch is complete, all we need to know is if there exists _any_ subgroup that has a 1 set.  Given we initialize all the bits to 1 in the subgroups we track (and all unused bits left at 0), the prescense of a nonzero value means the batch is still pending.


One last thing to think about before we call it a day though. There are all sorts of situations where batches can be opened but never closed. Imagine a client opens a batch, adds items to it, then dies. It may retry and create a new batch and add new items, but there is effectively an orphaned batch. To make our queue batcher work long term we need some asynchronous cleanup. You arbitrarily decide that any non-closed batches with no activity for N days get automatically deleted. Same with any closed-batches with no activity (maybe at a different interval). This lets the system deal with batches/events that are never going to complete.

Package this all up into a service with an API and blamo! Efficient queue batch tracking!

Estimated vs Exact batching
---

Estimated batching uses a redis counter keyed off of batch id to decrement a counter. This is consider an estimate becuase it suffers from potential double counting.

Exact batching is described above, and stores a multiplexed bit field for every item in the batch. It is exact, but has more overhead.
