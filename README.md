# import-with-sameas-service

Microservice that performs three tasks:

* Renames 'unknown' domain names into names that are more appropriate for
  applications such as Loket;
* Adds UUIDs to individuals in the data;
* Executes TTL files and inserts the data in the triplestore.

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

* **TARGET_GRAPH**: *(default: `http://mu.semte.ch/graphs/public`)* the graph
  where the imported data will be written.
* **RENAME_DOMAIN**: *(default: `http://centrale-vindplaats.lblod.info/id/`)*
  the domain to rename the URIs when they don't belong to a known domain.

## Reference

### Task operations

Refer to the job-controller configuration to see how a task fits in the
pipeline. The service gets triggered by tasks with `task:operation` being one
of the following:

```
<http://lblod.data.gift/id/jobs/concept/TaskOperation/mirroring>
<http://lblod.data.gift/id/jobs/concept/TaskOperation/add-uuids>
<http://lblod.data.gift/id/jobs/concept/TaskOperation/publishHarvestedTriples>
```

### Task statuses

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
