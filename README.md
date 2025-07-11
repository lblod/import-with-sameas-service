# import-with-sameas-service

Microservice that performs four tasks:

* Renames 'unknown' domain names into names that are more appropriate for
  applications such as Loket;
* Adds UUIDs to individuals in the data;
* Executes TTL files and inserts (with or without deletes) the data in the
  triplestore as part of the publishing step.
* Executes deletes from TTL files as a separate step.

All tasks are conform the
[job-controller-service](https://github.com/lblod/job-controller-service)
model.

## Installation

### docker-compose

To add the service to your stack, add the following snippet to
`docker-compose.yml`.


```yaml
harvesting-sameas:
  image: lblod/import-with-sameas-service:2.1.7
    environment:
      RENAME_DOMAIN: "http://data.lblod.info/id/"
      TARGET_GRAPH: "http://mu.semte.ch/graphs/harvesting"
    volumes:
      - ./data/files:/share
      - ./config/same-as-service/:/config
```

### delta-notifier

Add the following snippet to the configuration of the `delta-notifier`. This
notifies the service of any scheduled tasks in the stack.

```javascript
{
  match: {
    predicate: {
      type: 'uri',
      value: 'http://www.w3.org/ns/adms#status'
    },
    object: {
      type: 'uri',
      value: 'http://redpencil.data.gift/id/concept/JobStatus/scheduled'
    }
  },
  callback: {
    method: 'POST',
    url: 'http://harvesting-sameas/delta'
  },
  options: {
    resourceFormat: 'v0.0.1',
    gracePeriod: 1000,
    ignoreFromSelf: true
  }
}
```

## Configuration

### Mirroring config

The configuration file is called `config.json` and should be placed in the
configuration folder that is chosen in the `docker-compose.yml` file (see
above). An example configuration file might look like this:

```json
{
  "known-domains": [
    "data.vlaanderen.be",
    "mu.semte.ch",
    "data.europa.eu",
    "purl.org",
    "www.ontologydesignpatterns.org",
    "www.w3.org",
    "xmlns.com",
    "www.semanticdesktop.org",
    "schema.org",
    "centrale-vindplaats.lblod.info"
  ],
  "protocols-to-rename": [
    "http:",
    "https:",
    "ftp:",
    "ftps:"
  ]
}
```

This contains the known domain names that do not need to be translated. Others
will be queried from the triplestore and translated accordingly.

### Environment variables

The service also allows for configuration by means of the following environment
variables:

* **HIGH_LOAD_DATABASE_ENDPOINT**: *(default: `http://virtuoso:8890/sparql`)* the direct virtuoso endpoint
  to bypass mu auth when performances are required
* **TARGET_GRAPH**: *(default: `http://mu.semte.ch/graphs/public`)* the graph
  where the imported data will be written.
* **RENAME_DOMAIN**: *(default: `http://centrale-vindplaats.lblod.info/id/`)*
  the domain to rename the URIs when they don't belong to a known domain.
* **SLEEP_TIME**: *(default: 1000)* time in milliseconds to wait between
  retries of SPARQL insert or delete batches in case of failures. After this
  wait time, the batch is reduced in size and tried again.
* **BATCH_SIZE**: *(default: 100)* maximum size of a batch (the amount of
  triples per SPARQL request).
* **RETRY_WAIT_INTERVAL**: *(default: 30000)* time in milliseconds to wait
  after inserts or deletes have completely failed. The whole process is
  repeated untill it is clear that a clean recovery is not possible anymore.
* **MAX_RETRIES**: *(default: 10)* maximum number of retries of failing
  requests. The goal is to make sure that there are enough retries, spaced
  apart in time well enough, to span mu-authorization and triplestore restarts.
* **ROLLBACK_ENABLED**: *(default: true)* enables or disables automatic rollback
  on task failure. When set to false, failed tasks will not attempt rollback and
  will be marked as failed immediately, requiring manual cleanup if needed.

## Reference

### Task operations

Refer to the job-controller configuration to see how a task fits in the
pipeline. The service gets triggered by tasks with `task:operation` being one
of the following:

```
<http://lblod.data.gift/id/jobs/concept/TaskOperation/mirroring>
<http://lblod.data.gift/id/jobs/concept/TaskOperation/add-uuids>
<http://lblod.data.gift/id/jobs/concept/TaskOperation/execute-diff-deletes>
<http://lblod.data.gift/id/jobs/concept/TaskOperation/publishHarvestedTriples>
<http://lblod.data.gift/id/jobs/concept/TaskOperation/publishHarvestedTriplesWithDeletes>
```

### Task statuses

These are the statusses Tasks and Jobs get throughout their lifecycle:

```
STATUS_BUSY = 'http://redpencil.data.gift/id/concept/JobStatus/busy';
STATUS_SCHEDULED = 'http://redpencil.data.gift/id/concept/JobStatus/scheduled';
STATUS_SUCCESS = 'http://redpencil.data.gift/id/concept/JobStatus/success';
STATUS_FAILED = 'http://redpencil.data.gift/id/concept/JobStatus/failed';
```

### Example of renaming

Original triples:

```
<https://bertem.meetingburger.net/gr/6c8a0a3c-c9b6-4d47-82d0-8643ea501cb2/notulen>
  <http://centrale-vindplaats.lblod.info/ns/predicates/e3230ef0-ee88-11ea-8b2a-6179a3bcc5f8>
    "2020-05-26T18:13:00+2" .
<https://bertem.meetingburger.net/gr/6c8a0a3c-c9b6-4d47-82d0-8643ea501cb2/notulen>
  <http://centrale-vindplaats.lblod.info/ns/predicates/e328db50-ee88-11ea-8b2a-6179a3bcc5f8>
    "2020-05-26T19:23:00+2" .
<https://bertem.meetingburger.net/gr/6c8a0a3c-c9b6-4d47-82d0-8643ea501cb2/notulen#puntbesluit7e63f1fb-3136-4fd5-8761-43a2d85271e6>
  <http://centrale-vindplaats.lblod.info/ns/predicates/e3331480-ee88-11ea-8b2a-6179a3bcc5f8>
    "2020-06-30T22:00:00+2" .
```

Renamed triples:

```
<http://centrale-vindplaats.lblod.info/id/22DAB336-5519-4309-A74A-DD616F211CA2>
  <http://centrale-vindplaats.lblod.info/ns/predicates/e3230ef0-ee88-11ea-8b2a-6179a3bcc5f8>
    "2020-05-26T18:13:00+2" .
<http://centrale-vindplaats.lblod.info/id/22DAB336-5519-4309-A74A-DD616F211CA2>
  <http://www.w3.org/2002/07/owl#sameAs>
    <https://bertem.meetingburger.net/gr/6c8a0a3c-c9b6-4d47-82d0-8643ea501cb2/notulen>
<http://centrale-vindplaats.lblod.info/id/22DAB336-5519-4309-A74A-DD616F211CA2>
  <http://centrale-vindplaats.lblod.info/ns/predicates/e328db50-ee88-11ea-8b2a-6179a3bcc5f8>
    "2020-05-26T19:23:00+2" .
<http://centrale-vindplaats.lblod.info/id/C42EFDA8-124E-4E2A-8959-8673201CE48C>
  <http://centrale-vindplaats.lblod.info/ns/predicates/e3331480-ee88-11ea-8b2a-6179a3bcc5f8>
    "2020-06-30T22:00:00+2" .
<http://centrale-vindplaats.lblod.info/id/C42EFDA8-124E-4E2A-8959-8673201CE48C>
  <http://www.w3.org/2002/07/owl#sameAs>
    <https://bertem.meetingburger.net/gr/6c8a0a3c-c9b6-4d47-82d0-8643ea501cb2/notulen#puntbesluit7e63f1fb-3136-4fd5-8761-43a2d85271e6>
```

### API

#### POST `delta`

This is the endpoint that is configured in the `delta-notifier`. It returns a
status `200` as soon as possible, and then interprets the JSON body to filter
out tasks with the corrert operation and status to process and processes them
one by one.

#### POST `find-and-start-unfinished-tasks`

This will scan the triplestore for tasks that are not finished yet (`busy` or
`scheduled`) and restart them one by one. This can help to recover from
failures. The scanning and restarting is also done on startup of the service.
This does not require a body and the returned status will be `200 OK`.

#### POST `force-retry-task`

This endpoint can be used to manually retry a task. It does not matter what
state the task is in. The task can even be in failed state. It will be retried
anyway.

**Body**

Send a JSON body with the task URI, e.g.:

```http
Content-Type: application/json

{
  "uri": "http://redpencil.data.gift/id/task/e975b290-de53-11ed-a0b5-f70f61f71c42"
}
```

**Response**

`400 Bad Request`

This means that the task URI could not be found in the request.

`200 OK`

The task will be retried immediately after.

## "Transaction"-based publishing

There is no such thing as transactions with SPARQL and RDF, but this service
really needed a way to confidently say that either all triples of a task had
been published or none of them. We're not after locking database access for
other actors, nor after making updates to the triplestore to happen all at
once, but rather to provide some form of consistency.

Some tasks, e.g., require a few triples to be deleted and some triples to be
inserted. Imagine that an insert causes an error and the rest of the inserts
are not properly processed. The triplestore would be left in an inconsistent
state, missing some triples. With "transaction"-based publishing, we attempt to
detect that failure and rollback all the succesful inserts and deletes. The
deletes are inserted again and the inserts are deleted again, so the
triplestore is exactly how it was found before the publishing. This is the only
level of transactions we're hoping to achieve.

**Limitations**

Since transactions are not a real thing in SPARQL and transactions need
database support to be properly implemented, this "transaction"-based
publishing is really just an approximation with best efforts. On failure, the
queries are retried many times such that the retries should span database and
mu-authorization restarts or any other malfunction in the stack of temporary
nature. If database access is down for much longer than expected, the task
fails ungracefully, but even that failure will not be able to be written to the
database in which case the task will remain in a `busy` state and will be
picked up on the next startup of the service or when the service receives a
POST on `find-and-start-unfinished-tasks`.
