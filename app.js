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

/**
 * Starts the renaming process
 *
 */
async function rename() {
  const queryResult = await query(`
    SELECT DISTINCT ?subject ?predicate ?object WHERE {
      GRAPH ${sparqlEscapeUri(TARGET_GRAPH)} {
        ?subject ?predicate ?object
      }
    }
  `)
  const triples = queryResult.results.bindings
  const triplesRenamed = await renameTriples(triples)
  const fileName = await writeTriples(triplesRenamed)
}

/**
 * Takes an array of triples and renames the uris that are not from a known domain
 *
 * @param triples the triples to be renamed
 */
async function renameTriples(triples) {
  const namesDict = {}
  const renamedTriples = []
  await Promise.all(triples.map(async triple => {
    const {subject, predicate, object} = triple
    const renamedTriple = {}
    if(subject.type == 'uri') {
      if(namesDict[subject.value]) {
        renamedTriple.subject = { value: namesDict[subject.value], type: 'uri'}
      } else if(needsToBeRenamed(subject.value)) {
        const {sameAsTriple, newUri} = await renameUri(subject.value, namesDict)
        if(sameAsTriple) {
          renamedTriples.push(sameAsTriple)
        }
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
        const {sameAsTriple, newUri} = await renameUri(object.value, namesDict)
        if(sameAsTriple) {
          renamedTriples.push(sameAsTriple)
        }
        renamedTriple.object = { value: newUri, type: 'uri'}
        namesDict[object.value] = newUri
      } else {
        renamedTriple.object = object
      }
    } else {
      renamedTriple.subject = subject
    }
    renamedTriples.push(renamedTriple)
  }));
  return renamedTriples;
}

/**
 * Check if an uri needs to be renamed
 *
 * @param uri the uri to check
 */
function needsToBeRenamed(uri) {
  try {
    const {hostname, protocol} = new URL(uri)
    return hostname && protocol && PROTOCOLS_TO_RENAME.includes(protocol) && !KNOWN_DOMAINS.includes(hostname)
  } catch(e) {
    return false;
  }
}

/**
 * Creates a new uri and returns it with a triple to be inserted in the database to interpret this new uri
 *
 * @param oldUri the uri to be renamed
 */
async function renameUri(oldUri) {
  const queryResult = await query(`
    SELECT ?newURI WHERE {
      ?newURI <http://www.w3.org/2002/07/owl#sameAs> ${sparqlEscapeUri(oldUri)}
    }
  `);
  if(queryResult.results.bindings && queryResult.results.bindings[0]) {
    return { sameAsTriple: undefined, newUri: queryResult.results.bindings[0].newURI.value };
  } else {
    const newUri = `http://centrale-vindplaats.lblod.info/id/${uuid()}`

    const sameAsTriple = {
      subject: {value: newUri, type: 'uri'},
      predicate: {value: 'http://www.w3.org/2002/07/owl#sameAs', type: 'uri'},
      object: {value: oldUri, type: 'uri'}
    }
    return {sameAsTriple, newUri}
  }
}

/**
 * Writes the triples to the database and to a ttl file
 *
 * @param triples the triples to be written
 */
async function writeTriples(triples) {
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

/**
 * Convert a part of a triple to its string representation
 *
 * @param part the part to be converted
 */
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

/**
 * Updates the status of a task
 *
 * @param uri the uri of the task to be updated
 * @param status the uri of the status to be set
 */
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