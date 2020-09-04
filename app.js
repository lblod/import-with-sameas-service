import {app, errorHandler, uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeDateTime} from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import fs from 'fs'
import {Delta} from "./lib/delta";


import bodyParser from 'body-parser';

const KNOWN_DOMAINS = [
  'data.lblod.info',
  'data.vlaanderen.be',
  'mu.semte.ch',
  'data.europa.eu',
  'purl.org',
  'www.ontologydesignpatterns.org',
  'www.w3.org',
  'xmlns.com',
  'www.semanticdesktop.org',
  'schema.org',
  'centrale-vindplaats.lblod.info'
]

const PROTOCOLS_TO_RENAME = [
  'http:',
  'https:',
  'ftp:',
  'ftps:'
]

const TASK_READY_FOR_SAMEAS = 'http://lblod.data.gift/harvesting-statuses/ready-for-sameas';
const TASK_ONGOING = 'http://lblod.data.gift/harvesting-statuses/importing-with-sameas';
const TASK_SUCCESS = 'http://lblod.data.gift/harvesting-statuses/success';
const TASK_FAILURE = 'http://lblod.data.gift/harvesting-statuses/failure';
const TARGET_GRAPH = process.env.TARGET_GRAPH || 'http://mu.semte.ch/graphs/public';

app.use(bodyParser.json({
  type: function (req) {
    return /^application\/json/.test(req.get('content-type'));
  }
}));

app.get('/', function (req, res) {
  res.send('Hello harvesting-url-mirror');
});

app.post('/delta', async function (req, res, next) {
  console.log('Delta reached')
  try {
    const tasks = new Delta(req.body).getInsertsFor('http://www.w3.org/ns/adms#status', TASK_READY_FOR_SAMEAS);
    if (!tasks.length) {
      console.log('Delta dit not contain harvesting-tasks that are ready for import with sameas, awaiting the next batch!');
      return res.status(204).send();
    }
    console.log(`Starting import with sameas for harvesting-tasks: ${tasks.join(`, `)}`);
    for (let task of tasks) {
      try {
        await updateTaskStatus(task, TASK_ONGOING);
        await rename();
        await updateTaskStatus(task, TASK_SUCCESS);
      }catch (e){
        console.log(`Something unexpected went wrong while handling delta harvesting-task <${task}>`);
        console.error(e);
        try {
          await updateTaskStatus(task, TASK_FAILURE);
        } catch (e) {
          console.log(`Failed to update state of task <${task}> to failure state. Is the connection to the database broken?`);
          console.error(e);
        }
      }
    }
    return res.status(200).send().end();
  } catch (e) {
    console.log(`Something unexpected went wrong while handling delta harvesting-tasks!`);
    console.error(e);
    return next(e);
  }
});

async function rename() {
  const queryResult = await query(`
    SELECT DISTINCT ?subject ?predicate ?object WHERE {
      GRAPH ${sparqlEscapeUri(TARGET_GRAPH)}> {
        ?subject ?predicate ?object
      }
    }
  `)
  const triples = queryResult.results.bindings
  const triplesRenamed = renameTriples(triples)
  const fileName = await writeToFile(triplesRenamed)
}


function renameTriples(triples) {
  const namesDict = {}
  const renamedTriples = []
  triples.forEach(triple => {
    const {subject, predicate, object} = triple
    const renamedTriple = {}
    if(subject.type == 'uri') {
      if(namesDict[subject.value]) {
        renamedTriple.subject = { value: namesDict[subject.value], type: 'uri'}
      } else if(needsToBeRenamed(subject.value)) {
        const {sameAsTriple, newUri} = renameUri(subject.value, namesDict)
        renamedTriples.push(sameAsTriple)
        renamedTriple.subject = { value: newUri, type: 'uri'}
        namesDict[subject.value] = newUri
      } else {
        renamedTriple.subject = subject
      }
    } else {
      renamedTriple.subject = subject
    }
    renamedTriple.predicate = predicate
    if(subject.type == 'uri') {
      if(namesDict[object.value]) {
        renamedTriple.object = { value: namesDict[object.value], type: 'uri'}
      } else if(needsToBeRenamed(object.value)) {
        const {sameAsTriple, newUri} = renameUri(object.value, namesDict)
        renamedTriples.push(sameAsTriple)
        renamedTriple.object = { value: newUri, type: 'uri'}
        namesDict[object.value] = newUri
      } else {
        renamedTriple.object = object
      }
    } else {
      renamedTriple.subject = subject
    }
    renamedTriples.push(renamedTriple)
  });
  return renamedTriples;
}

function needsToBeRenamed(uri) {
  try {
    const {hostname, protocol} = new URL(uri)
    return hostname && protocol && PROTOCOLS_TO_RENAME.includes(protocol) && !KNOWN_DOMAINS.includes(hostname)
  } catch(e) {
    return false;
  }
}

function renameUri(oldUri) {
  const newUri = `http://centrale-vindplaats.lblod.info/id/${uuid()}`
  const sameAsTriple = {
    subject: {value: newUri, type: 'uri'},
    predicate: {value: 'http://www.w3.org/2002/07/owl#sameAs', type: 'uri'},
    object: {value: oldUri, type: 'uri'}
  }
  return {sameAsTriple, newUri}
}

async function writeToFile(triples) {
  const tripleStrings = triples.map((triple) => {
    const subject = processPart(triple.subject)
    const predicate = processPart(triple.predicate)
    const object = processPart(triple.object)
    return `${subject} ${predicate} ${object}.`
  })
  const fileName = `export-${uuid()}.ttl`
  const fileContent =  tripleStrings.join('\n')
  while(tripleStrings.length) {
    const batch = tripleStrings.splice(0, 100);
    await update(`
      INSERT DATA {
        GRAPH <http://mu.semte.ch/graphs/public> {
            ${batch.join('\n')}
        }
      }
    `);
  }
  fs.writeFile(`/exports/${fileName}`, fileContent, (err) => {
    if(err) throw err
    console.log(err)
    console.log('File saved ' + fileName)
  })
  return fileName
}

function processPart(part) {
  if(part.type === 'uri') {
    if(part.value === '#') return '<http://void>'
    return sparqlEscapeUri(part.value)
  } else if (part.type === 'literal') {
    return sparqlEscapeString(part.value)
  } else if(part.type === 'typed-literal') {
    return `${sparqlEscapeString(part.value)}^^<${part.datatype}>`
  }
}

async function updateTaskStatus(uri, status) {
  const q = `
    PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
    PREFIX adms: <http://www.w3.org/ns/adms#>

    DELETE {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} adms:status ?status .
      }
    } WHERE {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} adms:status ?status .
      }
    }

    ;

    INSERT {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} adms:status ${sparqlEscapeUri(status)} .
      }
    } WHERE {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} a harvesting:HarvestingTask .
      }
    }

  `;

  await update(q);
}



app.use(errorHandler);