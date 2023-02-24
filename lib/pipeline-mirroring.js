import * as mu from 'mu';
import * as mas from '@lblod/mu-auth-sudo';
import * as cts from '../constants';
import * as file from './file-helpers';
import * as tsk from './task';
import * as grph from './graph';
import * as uti from './utils';

export async function run(task) {
  try {
    await tsk.updateTaskStatus(task, cts.STATUS_BUSY.value);

    const triples = await grph.getTriples(task);
    const renamedTriples = await renameTriples(triples || []);
    const ntTriples = uti.triplesToNT(renamedTriples);

    const fileContainer = { id: mu.uuid() };
    fileContainer.uri = `http://redpencil.data.gift/id/dataContainers/${task.id}`;
    const mirroredFile = await file.writeTtlFile(
      task.graph,
      ntTriples.join('\n'),
      'mirrored-triples.ttl'
    );
    await appendTaskResultFile(task, fileContainer, mirroredFile);

    // No need as we use file during the publishing step
    //const sameAsGraph = `http://mu.semte.ch/graphs/harvesting/tasks/mirroring/${task.id}`;
    //await writeTriplesToGraph(sameAsGraph, ntTriples, BATCH_SIZE);

    const graphContainer = { id: mu.uuid() };
    graphContainer.uri = `http://redpencil.data.gift/id/dataContainers/${graphContainer.id}`;
    await grph.appendTaskResultGraph(task, graphContainer, fileContainer.uri);

    await tsk.updateTaskStatus(task, cts.STATUS_SUCCESS.value);
  } catch (e) {
    console.error(e);
    await tsk.appendTaskError(task, e.message);
    await tsk.updateTaskStatus(task, cts.STATUS_FAILED.value);
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
  for (let i = 0; i < triples.length; i++) {
    const triple = triples[i];
    const { subject, predicate, object } = triple;
    const renamedTriple = {};
    if (subject.type == 'uri') {
      if (namesDict[subject.value]) {
        renamedTriple.subject = {
          value: namesDict[subject.value],
          type: 'uri',
        };
      } else if (needsToBeRenamed(subject.value)) {
        const { sameAsTriple, newUri } = await renameUri(
          subject.value,
          namesDict
        );
        if (sameAsTriple) {
          renamedTriples.push(sameAsTriple);
        }
        renamedTriple.subject = { value: newUri, type: 'uri' };
        namesDict[subject.value] = newUri;
      } else {
        renamedTriple.subject = subject;
      }
    } else {
      renamedTriple.subject = subject;
    }
    renamedTriple.predicate = predicate;
    if (subject.type == 'uri') {
      if (namesDict[object.value]) {
        renamedTriple.object = { value: namesDict[object.value], type: 'uri' };
      } else if (needsToBeRenamed(object.value)) {
        const { sameAsTriple, newUri } = await renameUri(
          object.value,
          namesDict
        );
        if (sameAsTriple) {
          renamedTriples.push(sameAsTriple);
        }
        renamedTriple.object = { value: newUri, type: 'uri' };
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
    const { hostname, protocol } = new URL(uri);
    return (
      hostname &&
      protocol &&
      cts.PROTOCOLS_TO_RENAME.includes(protocol) &&
      !cts.KNOWN_DOMAINS.includes(hostname)
    );
  } catch (e) {
    return false;
  }
}

/**
 * Creates a new uri and returns it with a triple to be inserted in the database to interpret this new uri
 *
 * @param oldUri the uri to be renamed
 */
async function renameUri(oldUri) {
  const queryResult = await mas.querySudo(`
    SELECT ?newURI WHERE {
      ?newURI
        <http://www.w3.org/2002/07/owl#sameAs> ${mu.sparqlEscapeUri(oldUri)} .
    }
  `);
  if (queryResult.results.bindings && queryResult.results.bindings[0]) {
    const newUri = queryResult.results.bindings[0].newURI.value;
    const sameAsTriple = {
      subject: { value: newUri, type: 'uri' },
      predicate: { value: 'http://www.w3.org/2002/07/owl#sameAs', type: 'uri' },
      object: { value: oldUri, type: 'uri' },
    };
    return { sameAsTriple, newUri };
  } else {
    const newUri = `${cts.RENAME_DOMAIN}${mu.uuid()}`;
    const sameAsTriple = {
      subject: { value: newUri, type: 'uri' },
      predicate: { value: 'http://www.w3.org/2002/07/owl#sameAs', type: 'uri' },
      object: { value: oldUri, type: 'uri' },
    };
    return { sameAsTriple, newUri };
  }
}

async function appendTaskResultFile(task, container, fileUri) {
  const queryStr = `
    ${cts.SPARQL_PREFIXES}
    INSERT DATA {
      GRAPH ${mu.sparqlEscapeUri(task.graph)} {
        ${mu.sparqlEscapeUri(container.uri)}
          a nfo:DataContainer ;
          mu:uuid ${mu.sparqlEscapeString(container.id)} ;
          task:hasFile ${mu.sparqlEscapeUri(fileUri)} .
        ${mu.sparqlEscapeUri(task.task)}
          task:resultsContainer ${mu.sparqlEscapeUri(container.uri)} .
      }
    }
  `;

  await mas.updateSudo(queryStr);
}
