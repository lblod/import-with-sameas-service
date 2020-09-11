# import-with-sameas-service
Microservice that fetchs data from an intermediate graph, renames the unknown uris from that data and imports then to a target graph.

## Instalation
To add the service to your stack, add the following snippet to docker-compose.yml

```
harvesting-sameas:
  image: lblod/import-with-sameas-service:0.0.2
  volumes:
    - ./data/exports:/exports
  labels:
    - "logging=true"
  restart: always
  logging: *default-logging
```

## Configuration

### Delta
First of all you have to trigger the delta endpoint for the microservice when a new task with the status `ready-for-sameas` gets inserted, in order to do that you have to add the delata service to the stack and add the following configuration to `config/delta/rules.js` on your app

```
{
  match: {
    predicate: {
      type: 'uri',
      value: 'http://www.w3.org/ns/adms#status'
    },
    object: {
      type: 'uri',
      value: 'http://lblod.data.gift/harvesting-statuses/ready-for-sameas'
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

In order to trigger the service you just have to create a harvesting task with the following data

```
PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

INSERT DATA {
  GRAPH <http://mu.semte.ch/graphs/public> {
      ?taskUri a harvesting:HarvestingTask; 
        adms:status <http://lblod.data.gift/harvesting-statuses/ready-for-sameas>;
        ext:graph ?graph
  }
}
```

Being ?taskUri and unique identifier for the task, and ?graph the graph where the data is located
After the task is finished you will find all the data renamed on the target graph.

## Reference

### Task statuses

| Name | URI | Meaning |
|---|---|--|
| Ready for Sameas | http://lblod.data.gift/harvesting-statuses/ready-for-sameas | The task is ready to be picked up by the service |
| Ongoing | http://lblod.data.gift/harvesting-statuses/importing-with-sameas | The task is currently being processed |
| Success | http://lblod.data.gift/harvesting-statuses/success | The task has been completed without problems |
| Failure | http://lblod.data.gift/harvesting-statuses/failure | The service had a problem completing the task |

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