# your task is to create a presentation formed as a web-application which visualizes concepts via web interface with dynamic behaviour for representing content.

# the subject is kafka.
i want this presentation to be a learning guide which i will present to other engineers as a cradle of experience.
this presentation should be a guideline on which patterns and approaches to use to implement different use cases in kafka in financial industry (investment bank) applications.
this presentation is also a guideline for application engineers which patterns to use to address kafka limitations on delivery semantics
the slides should be animated to show where the data moving is how - if there are multiple components and arrow(s) between them, the slinde should dynamically show that the message is flowing via the arrow, that it lands to a database table
the presentation should be compact enough, just to show several patterns and explaining why it's done this day (pros and cons)

# subjects we need to care about:
- data retention and archiving - very important in investment banks (14 days retention of topics proposed as default, to allow enough time for RCA)
- data consistency - no data loss, no data duplication
- data evolution - backwards-compatible schema changes
- resiliency - in my case, i am using kafka as a platform; when switching to another azure cloud region, there has to be a runbook to preserve data consistency. f.e. replay messages from the point when primary region failed

# pre-requisites
- java
- spring boot 4
- resiliency 4j if needed

topics which i would like to cover in this presentation:

# approximate slide contents
1. kafka consumption
- different spring boot kafka listeners
- financial applications often prefer 1 read -> 1 process, without batches. most simple scenario
- next level is batch read. batch read from topic pattern. but batch read us prone to errors. all or nothing vs partial offset commit
- concurrent listener - present way to take address commit offset failures - batch of 100 was read, processing is multithreaded using executor, - failed at 51 - need to commit all messages up to 51, don't commit rest - synchronize processing threads to achieve this scenario.
- idempotency implementation
  - best approach is check constraint for uniqueness upon insert
  - kafka doesn't support 2x transactions
  - using cache for uniqueness check is just optimization approach - but somebody will have to maintain the cache. 2 different approaches: uniqueness cache, negative cache (misses)
  - other approaches shortly represented as well, and their disadvantages are noted
  - database commit should come first, the kafka offset should come second, explain why
  - if the service writes to the database, and also wants to publish message to another topic - transactional outbox is the solution for consistency (to be presented in the next chapters)
- measuring kafka consumer performance in azure (appinsights + micrometer + prometheus + grafana)
- normal kafka consumer TPS with PostgreSQL
- insert should be optimized - DB indexes organization
- co-location of kafka consumer and DB is important - cross-region lag adds to latency`
- it should be explained why it's important to persist such attributes as topic, consumer group id, partition id, partition offset - in case if replay is needed (f.e. when switching to DR region - offsets and partitons will exactly let you know that's the RPO - looking at offsets in DB vs offsets in failed region or failed region DB - you will exactly know which messagesw were not consumed)
- idempotency is especially important when switching to DR - using offsets persisted to DB, you can exactly identify how many messages you need to replay from the topic(s)
- monitoring aspect - throughput (TPS), success/error rate, alerts triggered only when success/error rate is above threshold - all this important to monitor starting from performance tests, not just on production
- monitoring should clearly show which part is diractly kafka vs database operation performance or other action performance (f.e. consumer also stores to azure ADLS - that trace should be clearly visible)
- monitoring - consumer lag (per partition), group rebalancing statistics, is extremely important to monitor to do proper RCA
- explaing how to get consumer lag and rebalancing metric in java / spring bboot (using kadka admin client api as an option)
- message schema should have a version in the header, that's important for blue-green deployment and backwards-compatibility
- dead-letter topic must be present
- if order of messages is not important, dead-letter topic publishing is easy
- if order is important, then the presentation should show a solution: if particular messages failed, all subsequent messages should also fail until reprosessed in correct order - track that in DB layer.
- message sent to DLT should ideally reach out to the upstream so that it can receive notification something went wrong, and react by fixing an invalid message by republishing it. Common pattern - create 1 common topic for all upstreams to subscribe to, instead of enforcing upstream consumers to subscribe to individual topic DLTs.
2. kafka publishing
- importance of specifying partition key if order is important. how to choose partition key
- what happens if publisher doesn't publish partition key (could lead to specific partition becoming hot - double check that). SOmetimes engineers just specify NULL as partition key - what are side-effects it can lead to?
- idempotent kafka publisher, how to make sure published doesn't publish duplicates
- transactional outbox - if consistency is important, then transactional outbox can solve that but with the cost of duplicate messages probability
- 2 main options for transactional outbox - debezium, and periodically running spring boot process (scheduled every 30 seconds f.e.) which reads from transactional outbox table and publishes to the topic. for quick results and simplicity, spring boot is preferred. 
3. kafka availability and resiliency
- kafka publishes offsets for all topics to its internal topic, which is replicated across the cluster (if cluster is stretched across multiple azure regions, then the lag is suprt minimal; but still there can be some lags; so real source of truth is offset persisted to the database)
- a system which chases for RPO=0, must be ready to replay messages to kafka topics. describe ways to do this (manual script to move back the offset, custom UI support application, etc.)
- without persisting topics, consumer groups, partition ids, and offsets for partitions - the disaster recovery and consustency check becomes very complex and non-deterministic
4. other thoughts
- having all that problems with consistency, was kafka choice worth it? maybe choosing azure event hub or other FIFO broker would help better. explain why
- kafka will require observability on it's platform - brokers performance, topic performance, availability of brokers, etc.. Application teams must have that information
- Emphasize usage of spring boot admin api to observe core metrics of kafka

did i miss anything else to mention? ask me