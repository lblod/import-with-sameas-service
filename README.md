# import-with-sameas-service
Microservice that fetchs data from an intermediate graph, renames the unknown uris from that data and imports then to a target graph.
Microservice that injects the mirrored data into its final destination.

## Instalation
To add the service to your stack, add the following snippet to docker-compose.yml

```
harvesting-sameas:
  image: lblod/import-with-sameas-service:0.1.0
    environment:
      RENAME_DOMAIN: "http://data.lblod.info/id/"
      TARGET_GRAPH: "http://mu.semte.ch/graphs/harvesting"
    volumes:
      - ./data/files:/share
```

## Configuration

### Delta

```
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

### Environment variables
The service has the following environment variables to be configured
- TARGET_GRAPH : The graph where the final data will be written, defaults to `http://mu.semte.ch/graphs/public`
- RENAME_DOMAIN : The domain to rename the uris when they don't belong to a known domain, it defaults to `http://centrale-vindplaats.lblod.info/id/`

## How-to Guides

### How to trigger the service
Refer to job-controller configuration to see how its task fits in the job.
The service gets triggered by task with `task:operation`
```
<http://lblod.data.gift/id/jobs/concept/TaskOperation/mirroring>
<http://lblod.data.gift/id/jobs/concept/TaskOperation/importCentraleVindplaats>
```


## Reference

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
 <https://bertem.meetingburger.net/gr/6c8a0a3c-c9b6-4d47-82d0-8643ea501cb2/notulen> <http://centrale-vindplaats.lblod.info/ns/predicates/e3230ef0-ee88-11ea-8b2a-6179a3bcc5f8> "2020-05-26T18:13:00+2".
 <https://bertem.meetingburger.net/gr/6c8a0a3c-c9b6-4d47-82d0-8643ea501cb2/notulen> <http://centrale-vindplaats.lblod.info/ns/predicates/e328db50-ee88-11ea-8b2a-6179a3bcc5f8> "2020-05-26T19:23:00+2".
 <https://bertem.meetingburger.net/gr/6c8a0a3c-c9b6-4d47-82d0-8643ea501cb2/notulen#puntbesluit7e63f1fb-3136-4fd5-8761-43a2d85271e6> <http://centrale-vindplaats.lblod.info/ns/predicates/e3331480-ee88-11ea-8b2a-6179a3bcc5f8> "2020-06-30T22:00:00+2".
```

Renamed triples:
```
<http://centrale-vindplaats.lblod.info/id/22DAB336-5519-4309-A74A-DD616F211CA2> <http://centrale-vindplaats.lblod.info/ns/predicates/e3230ef0-ee88-11ea-8b2a-6179a3bcc5f8> "2020-05-26T18:13:00+2".
<http://centrale-vindplaats.lblod.info/id/22DAB336-5519-4309-A74A-DD616F211CA2> <http://www.w3.org/2002/07/owl#sameAs> <https://bertem.meetingburger.net/gr/6c8a0a3c-c9b6-4d47-82d0-8643ea501cb2/notulen>
<http://centrale-vindplaats.lblod.info/id/22DAB336-5519-4309-A74A-DD616F211CA2> <http://centrale-vindplaats.lblod.info/ns/predicates/e328db50-ee88-11ea-8b2a-6179a3bcc5f8> "2020-05-26T19:23:00+2".
<http://centrale-vindplaats.lblod.info/id/C42EFDA8-124E-4E2A-8959-8673201CE48C> <http://centrale-vindplaats.lblod.info/ns/predicates/e3331480-ee88-11ea-8b2a-6179a3bcc5f8> "2020-06-30T22:00:00+2".
<http://centrale-vindplaats.lblod.info/id/C42EFDA8-124E-4E2A-8959-8673201CE48C> <http://www.w3.org/2002/07/owl#sameAs> <https://bertem.meetingburger.net/gr/6c8a0a3c-c9b6-4d47-82d0-8643ea501cb2/notulen#puntbesluit7e63f1fb-3136-4fd5-8761-43a2d85271e6>
```
