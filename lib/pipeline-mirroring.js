import { sparqlEscapeUri, sparqlEscapeString, uuid } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import {
  STATUS_BUSY,
  STATUS_FAILED,
  STATUS_SUCCESS,
  KNOWN_DOMAINS,
  PROTOCOLS_TO_RENAME,
  RENAME_DOMAIN
} from '../constants';

import { writeTtlFile } from "./file-helpers";
import { updateTaskStatus, appendTaskError } from './task';
import { getTriples, appendTaskResultGraph } from './graph';
import { triplesToNT } from './utils';
const OWL_SAME_AS = "http://www.w3.org/2002/07/owl#sameAs";
export async function run(task) {
  try {
    await updateTaskStatus(task, STATUS_BUSY);

    const triples = await getTriples(task);
    const renamedTriples = await renameTriples(triples || []);
    const ntTriples = triplesToNT(renamedTriples);

    const fileContainer = { id: uuid() };
    fileContainer.uri = `http://redpencil.data.gift/id/dataContainers/${task.id}`;
    const mirroredFile = await writeTtlFile(task.graph, ntTriples.join('\n'), 'mirrored-triples.ttl');
    await appendTaskResultFile(task, fileContainer, mirroredFile);

    const graphContainer = { id: uuid() };
    graphContainer.uri = `http://redpencil.data.gift/id/dataContainers/${graphContainer.id}`;
    await appendTaskResultGraph(task, graphContainer, fileContainer.uri);

    await updateTaskStatus(task, STATUS_SUCCESS);
  }
  catch (e) {
    console.error(e);
    await appendTaskError(task, e.message);
    await updateTaskStatus(task, STATUS_FAILED);
  }
}

/**
 * Takes an array of triples and renames the uris that are not from a known domain
 * TODO: cleanup
 * @param triples the triples to be renamed
 */
async function renameTriples(triples) {
  const checkAndRename = async (part, namesDict, renamedTriples = []) => {
    if (part.type == 'uri') {
      if (namesDict[part.value]) {
        return { value: namesDict[part.value], type: 'uri' };
      }
      else if (needsToBeRenamed(part.value)) {
        const { sameAsTriple, newUri } = await renameUri(part.value, namesDict);
        if (sameAsTriple) {
          renamedTriples.push(sameAsTriple);
        }
        namesDict[part.value] = newUri;
        return { value: newUri, type: 'uri' };
      } else {
        return part;
      }
    }
    return part;
  };
  const namesDict = {};
  const renamedTriples = [];

  for (const triple of triples) {
    const { subject, predicate, object } = triple;
    const renamedTriple = {};

    renamedTriple.subject = await checkAndRename(subject, namesDict, renamedTriples);
    renamedTriple.object = await checkAndRename(object, namesDict, renamedTriples);
    renamedTriple.predicate = predicate;
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
    return hostname && protocol && PROTOCOLS_TO_RENAME.includes(protocol) && !KNOWN_DOMAINS.includes(hostname);
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
  const queryResult = await query(`
    SELECT ?newURI WHERE {
      ?newURI <${OWL_SAME_AS}> ${sparqlEscapeUri(oldUri)}
    }
  `);
  if (queryResult.results.bindings && queryResult.results.bindings[0]) {
    return { sameAsTriple: undefined, newUri: queryResult.results.bindings[0].newURI.value };
  } else {
    const newUri = `${RENAME_DOMAIN}${uuid()}`;

    const sameAsTriple = {
      subject: { value: newUri, type: 'uri' },
      predicate: { value: 'http://www.w3.org/2002/07/owl#sameAs', type: 'uri' },
      object: { value: oldUri, type: 'uri' }
    };
    return { sameAsTriple, newUri };
  }
}

async function appendTaskResultFile(task, container, fileUri) {
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
