import * as mu from 'mu';
import * as mas from '@lblod/mu-auth-sudo';
import * as cts from '../constants';
import * as file from './file-helpers';
import * as tsk from './task';
import * as grph from './graph';
import * as uti from './utils';
import * as rst from 'rdf-string-ttl';
import * as sjp from 'sparqljson-parse';
import * as N3 from 'n3';
import { NAMESPACES as ns } from '../constants';
import { BASES as bs } from '../constants';
const { namedNode, literal, quad } = N3.DataFactory;

export async function run(task) {
  try {
    await tsk.updateTaskStatus(task, cts.STATUS_BUSY);

    const triples = await grph.getTriples(task);
    const renamedTriples = await renameTriples(triples);
    const renamedTriplesString = await uti.storeToTtl(renamedTriples);

    const fileContainer = {
      id: literal(mu.uuid()),
      node: bs.dataContainer(task.id.value),
    };
    const mirroredFile = await file.writeTtlFile(
      task.graph,
      renamedTriplesString,
      'mirrored-triples.ttl'
    );
    await appendTaskResultFile(task, fileContainer, mirroredFile);

    // No need as we use file during the publishing step
    //const sameAsGraph = `http://mu.semte.ch/graphs/harvesting/tasks/mirroring/${task.id}`;
    //await writeTriplesToGraph(sameAsGraph, ntTriples, BATCH_SIZE);

    const graphContainer = { id: literal(mu.uuid()) };
    graphContainer.node = bs.dataContainer(graphContainer.id.value);

    await grph.appendTaskResultGraph(task, graphContainer, fileContainer.node);
    await tsk.updateTaskStatus(task, cts.STATUS_SUCCESS);
  } catch (e) {
    console.error(e);
    await tsk.appendTaskError(task, e.message);
    await tsk.updateTaskStatus(task, cts.STATUS_FAILED);
  }
}

/**
 * Takes an array of triples and renames the uris that are not from a known domain
 * @param triples the triples to be renamed
 */
async function renameTriples(store) {
  const namesDict = {};
  const renamedStore = new N3.Store();
  for (const quad of store) {
    const renamedTriple = {};
    if (namesDict[quad.subject.value]) {
      renamedTriple.s = namesDict[quad.subject.value];
    } else if (needsToBeRenamed(quad.subject.value)) {
      const { sameAsTriple, newUri } = await renameUri(quad.subject);
      renamedStore.addQuad(sameAsTriple);
      namesDict[quad.subject.value] = newUri;
      renamedTriple.s = newUri;
    } else {
      renamedTriple.s = quad.subject;
    }
    renamedTriple.p = quad.predicate;
    if (quad.object.termType === 'NamedNode') {
      if (namesDict[quad.object.value]) {
        renamedTriple.o = namesDict[quad.object.value];
      } else if (needsToBeRenamed(quad.object.value)) {
        const { sameAsTriple, newUri } = await renameUri(quad.object);
        renamedStore.addQuad(sameAsTriple);
        namesDict[quad.subject.value] = newUri;
        renamedTriple.o = newUri;
      } else {
        renamedTriple.o = quad.object;
      }
    } else {
      renamedTriple.o = quad.object;
    }
    renamedStore.addQuad(renamedTriple.s, renamedTriple.p, renamedTriple.o);
  }
  return renamedStore;
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
  PREFIX owl: ${rst.termToString(ns.owl``)}
    SELECT ?newURI WHERE {
      ?newURI owl:sameAs ${rst.termToString(literal(oldUri.value))} .
    } LIMIT 1
  `);
  const parser = new sjp.SparqlJsonParser();
  const parsedResults = parser.parseJsonResults(queryResult);
  const newUri =
    parsedResults.length > 0
      ? parsedResults[0].newUri
      : namedNode(`${cts.RENAME_DOMAIN}${mu.uuid()}`);
  return {
    sameAsTriple: quad(newUri, ns.owl`sameAs`, oldUri),
    newUri,
  };
}

async function appendTaskResultFile(task, container, fileUri) {
  return mas.updateSudo(`
    ${cts.SPARQL_PREFIXES}
    INSERT DATA {
      GRAPH ${rst.termToString(task.graph)} {
        ${rst.termToString(container.node)}
          a nfo:DataContainer ;
          mu:uuid ${rst.termToString(container.id)} ;
          task:hasFile ${rst.termToString(fileUri)} .
        ${rst.termToString(task.task)}
          task:resultsContainer ${rst.termToString(container.node)} .
      }
    }`);
}
