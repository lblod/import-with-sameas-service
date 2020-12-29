import { sparqlEscapeUri, sparqlEscapeString, uuid } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { TASK_HARVESTING_MIRRORING,
         STATUS_BUSY,
         STATUS_FAILED,
         STATUS_SUCCESS,
         KNOWN_DOMAINS,
         PROTOCOLS_TO_RENAME,
         RENAME_DOMAIN,
         PREFIXES
       } from '../constants';

import { writeTtlFile } from "./file-helpers";
import { loadTask, isTask, updateTaskStatus, appendTaskError } from './task';
import { parseResult } from './utils';

export async function run( deltaEntry ){
  if(! await isTask(deltaEntry) ) return;

  const task = await loadTask(deltaEntry);

  if(!(await isMirroringTask(task))) return;

  try {
    updateTaskStatus(task, STATUS_BUSY);

    const triples = await getTriplesInGraph(task);
    if(triples.length){
      const renamedTriples = await renameTriples(triples);
      const ntTriples = sameAsTriplesToNT(renamedTriples);

      const fileContainer = { id: uuid() };
      fileContainer.uri = `http://redpencil.data.gift/id/dataContainers/${fileContainer.id}`;
      const mirroredFile = await writeTtlFile( task.graph , ntTriples.join('\n'), 'mirrored-triples.ttl');
      await appendTaskResultFile(task, fileContainer, mirroredFile);

      const sameAsGraph = `http://mu.semte.ch/graphs/harvesting/tasks/mirroring/${task.id}`;
      await writeTriplesToGraph(sameAsGraph, ntTriples);

      const graphContainer = { id: uuid() };
      graphContainer.uri = `http://redpencil.data.gift/id/dataContainers/${graphContainer.id}`;
      await appendTaskResultGraph(task, graphContainer, sameAsGraph);
    }

    updateTaskStatus(task, STATUS_SUCCESS);
  }
  catch(e){
    console.error(e);
    await appendTaskError(task, e.message);
    await updateTaskStatus(task, STATUS_FAILED);
  }
}

function isMirroringTask(task){
   return task.operation == TASK_HARVESTING_MIRRORING;
}

/**
 * Starts the renaming process
 *
 */

async function getTriplesInGraph(task){
  const queryGraph = `
     ${PREFIXES}
     SELECT DISTINCT ?graph WHERE {
        GRAPH ?g {
          BIND(${sparqlEscapeUri(task.task)} as ?task).
          ?task task:inputContainer ?container.
          ?container task:hasGraph ?graph.
        }
     }
  `;
  const graphData = parseResult(await query(queryGraph))[0];

  const queryTriples = `
    SELECT DISTINCT ?subject ?predicate ?object WHERE {
      GRAPH ${sparqlEscapeUri(graphData.graph)} {
        ?subject ?predicate ?object
      }
    }
  `;

  const result = await query(queryTriples);
  if(!(result.results && result.results.bindings.length)){
    return [];
  }
  else {
    return result.results.bindings;
  }
}

/**
 * Takes an array of triples and renames the uris that are not from a known domain
 * TODO: cleanup
 * @param triples the triples to be renamed
 */
async function renameTriples(triples) {
  const namesDict = {};
  const renamedTriples = [];
  for(let i = 0; i < triples.length; i++) {
    const triple = triples[i];
    const {subject, predicate, object} = triple;
    const renamedTriple = {};
    if(subject.type == 'uri') {
      if(namesDict[subject.value]) {
        renamedTriple.subject = { value: namesDict[subject.value], type: 'uri'};
      } else if(needsToBeRenamed(subject.value)) {
        const {sameAsTriple, newUri} = await renameUri(subject.value, namesDict);
        if(sameAsTriple) {
          renamedTriples.push(sameAsTriple);
        }
        renamedTriple.subject = { value: newUri, type: 'uri'};
        namesDict[subject.value] = newUri;
      } else {
        renamedTriple.subject = subject;
      }
    } else {
      renamedTriple.subject = subject;
    }
    renamedTriple.predicate = predicate;
    if(subject.type == 'uri') {
      if(namesDict[object.value]) {
        renamedTriple.object = { value: namesDict[object.value], type: 'uri'};
      } else if(needsToBeRenamed(object.value)) {
        const {sameAsTriple, newUri} = await renameUri(object.value, namesDict);
        if(sameAsTriple) {
          renamedTriples.push(sameAsTriple);
        }
        renamedTriple.object = { value: newUri, type: 'uri'};
        namesDict[object.value] = newUri;
      } else {
        renamedTriple.object = object;
      }
    } else {
      renamedTriple.subject = subject;
    }
    renamedTriples.push(renamedTriple);
  }
  return renamedTriples;
}

/**
 * Check if an uri needs to be renamed
 *
 * @param uri the uri to check
 */
function needsToBeRenamed(uri) {
  try {
    const {hostname, protocol} = new URL(uri);
    return hostname && protocol && PROTOCOLS_TO_RENAME.includes(protocol) && !KNOWN_DOMAINS.includes(hostname);
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
    const newUri = `${RENAME_DOMAIN}${uuid()}`;

    const sameAsTriple = {
      subject: {value: newUri, type: 'uri'},
      predicate: {value: 'http://www.w3.org/2002/07/owl#sameAs', type: 'uri'},
      object: {value: oldUri, type: 'uri'}
    };
    return {sameAsTriple, newUri};
  }
}

function sameAsTriplesToNT(triples){
  return triples.map((triple) => {
    const subject = processPart(triple.subject);
    const predicate = processPart(triple.predicate);
    const object = processPart(triple.object);
    return `${subject} ${predicate} ${object}.`;
  });
}

/**
 * Writes the triples to the database and to a ttl file
 *
 * @param triples the triples to be written
 */
async function writeTriplesToGraph(graph, triples) {
  const tripleStrings = [...triples];
  while(tripleStrings.length) {
    const batch = tripleStrings.splice(0, 100);
    await update(`
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(graph)} {
            ${batch.join('\n')}
        }
      }
    `);
  }
}

/**
 * Convert a part of a triple to its string representation
 *
 * @param part the part to be converted
 */
function processPart(part) {
  if(part.type === 'uri') {
    if(part.value === '#') return '<http://void>';
    return sparqlEscapeUri(part.value);
  } else if (part.type === 'literal') {
    return sparqlEscapeString(part.value);
  } else if(part.type === 'typed-literal') {
    return `${sparqlEscapeString(part.value)}^^<${part.datatype}>`;
  }
  return null;
}

async function appendTaskResultGraph(task, container, graphUri){
  const queryStr = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(task.graph)} {
        ${sparqlEscapeUri(container.uri)} a nfo:DataContainer.
        ${sparqlEscapeUri(container.uri)} mu:uuid ${sparqlEscapeString(container.id)}.
        ${sparqlEscapeUri(container.uri)} task:hasGraph ${sparqlEscapeUri(graphUri)}.
        ${sparqlEscapeUri(task.task)} task:resultsContainer ${sparqlEscapeUri(container.uri)}.
      }
    }
  `;

  await update(queryStr);

}

async function appendTaskResultFile(task, container, fileUri){
  const queryStr = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(task.graph)} {
        ${sparqlEscapeUri(container.uri)} a nfo:DataContainer.
        ${sparqlEscapeUri(container.uri)} mu:uuid ${sparqlEscapeString(container.id)}.
        ${sparqlEscapeUri(container.uri)} task:hasFile ${sparqlEscapeUri(fileUri)}.
        ${sparqlEscapeUri(task.task)} task:resultsContainer ${sparqlEscapeUri(container.uri)}.
      }
    }
  `;

  await update(queryStr);

}
