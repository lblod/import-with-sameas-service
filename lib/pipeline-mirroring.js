import { uuid } from 'mu';
import { querySudo } from '@lblod/mu-auth-sudo';
import { URL } from 'url';
import { makeEmptyFile, appendTempFile, writeTtlFile } from './file-helpers';
import {
  updateTaskStatus,
  appendTaskError,
  appendTaskResultFile,
} from './task';
import {
  getTriplesInFileAndApplyByBatch,
  appendTaskResultGraph,
} from './graph';
import { storeToTtl } from './utils';
import { termToString } from 'rdf-string-ttl';
import { SparqlJsonParser } from 'sparqljson-parse';
import { createHash } from 'crypto';
import { DataFactory, Store } from 'n3';
import {
  BASES as bs,
  NAMESPACES as ns,
  STATUS_BUSY,
  STATUS_SUCCESS,
  STATUS_FAILED,
  PROTOCOLS_TO_RENAME,
  KNOWN_DOMAINS,
  RENAME_DOMAIN,
  PREDICATES_TO_IGNORE_FOR_RENAME,
  HIGH_LOAD_DATABASE_ENDPOINT,
} from '../constants';
const { namedNode, literal, quad } = DataFactory;

/**
 * Run the mirroring pipeline. It loads triples from the triplestore or from
 * files depending on the way the inputContainer is set up and transforms the
 * URIs that are not in a known domain to ones that are by creating a new one
 * and by pointing all properties to that one. A new triple indicating equality
 * (`owl:sameAs`) with the old URI will be added to the triplestore. It also
 * updates the task along the process.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} task - Represents the task for wich to start the process.
 * @returns {undefined} Nothing
 */
export async function run(task, signal= {}) {
  try {
    if(signal.aborted) {
      throw new Error('task aborted');
    }
    await updateTaskStatus(task, STATUS_BUSY);
    const mappingCache = {};

    const tempFiles = new Map();
    await getTriplesInFileAndApplyByBatch(task, async (store, derivedFrom) => {
      if(signal.aborted) {
        throw new Error('task aborted');
      }
      if (!tempFiles.has(derivedFrom)) {
        const tempTtlFile = `/share/mirrored-triples-${uuid()}.ttl`;
        makeEmptyFile(tempTtlFile);

        tempFiles.set(derivedFrom, tempTtlFile);
      }
      const tempTtlFile = tempFiles.get(derivedFrom);
      const renamedTriples = await renameTriples(store, mappingCache);
      const renamedTriplesString = await storeToTtl(renamedTriples);
      await appendTempFile(renamedTriplesString, tempTtlFile);
    });

    const fileContainer = {
      id: literal(uuid()),
      node: bs.dataContainer(task.id.value),
    };
    // No need as we use file during the publishing step
    //const sameAsGraph = `http://mu.semte.ch/graphs/harvesting/tasks/mirroring/${task.id}`;
    //await writeTriplesToGraph(sameAsGraph, ntTriples, BATCH_SIZE);
    for (const [derivedFrom, tempTtlFile] of tempFiles.entries()) {
      const mirroredFile = await writeTtlFile(
        task.graph,
        tempTtlFile,
        'mirrored-triples.ttl',
        derivedFrom,
      );
      await appendTaskResultFile(task, fileContainer, mirroredFile);
    }
    const graphContainer = { id: literal(uuid()) };
    graphContainer.node = bs.dataContainer(graphContainer.id.value);

    await appendTaskResultGraph(task, graphContainer, fileContainer.node);
    await updateTaskStatus(task, STATUS_SUCCESS);
  } catch (e) {
    console.error(e);
    await appendTaskError(task, e.message);
    await updateTaskStatus(task, STATUS_FAILED);
  }
}

/**
 * Transforms the triples that are not in a known domain. This includes the
 * subject and the object. The input triples are traversed and if a triple is
 * not in a known domain (configurable) a new URI is created for that subject.
 * URIs are kept in a map during the process for speeding it up, but also to be
 * able to translate URI in object position that have been seen before. New
 * triples are added (`owl:sameAs`) to the old URI to indicate equivalence.
 *
 * @async
 * @function
 * @param {N3.Store} store - Store of triples to be translated.
 * @returns {N3.Store} Store of triples with translated subjects and objects,
 * with added triples to indicate equality with the previous URI.
 */
async function renameTriples(store, namesDict) {
  const renamedStore = new Store();
  for (const quad of store) {
    const renamedTriple = {};
    if (namesDict[quad.subject.value]) {
      // URI seen and translated before. Take that result.
      renamedTriple.s = namesDict[quad.subject.value];
    } else if (needsToBeRenamed(quad.subject.value)) {
      // Not translated before, but is necessary.
      const { sameAsTriple, newUri } = await renameUri(quad.subject);
      // Add `owl:sameAs` triple to the result store.
      renamedStore.addQuad(sameAsTriple);
      // Keep the translation for later reference.
      namesDict[quad.subject.value] = newUri;
      renamedTriple.s = newUri;
    } else {
      // No translation needed.
      renamedTriple.s = quad.subject;
    }
    renamedTriple.p = quad.predicate;
    if (
      quad.object.termType === 'NamedNode' &&
      !shouldIgnorePredicateForRename(quad.predicate)
    ) {
      if (namesDict[quad.object.value]) {
        renamedTriple.o = namesDict[quad.object.value];
      } else if (needsToBeRenamed(quad.object.value)) {
        const { sameAsTriple, newUri } = await renameUri(quad.object);
        renamedStore.addQuad(sameAsTriple);
        namesDict[quad.object.value] = newUri;
        renamedTriple.o = newUri;
      } else {
        renamedTriple.o = quad.object;
      }
    } else {
      renamedTriple.o = quad.object;
    }
    // Finally add the translated triple to the store.
    renamedStore.addQuad(renamedTriple.s, renamedTriple.p, renamedTriple.o);
  }
  return renamedStore;
}

/**
 * Checks if a URI needs to be translated using the config.
 *
 * @function
 * @param {String} uri - The URI (of a subject).
 * @returns {Boolean} Whether or not the URI needs to be translated.
 */
function needsToBeRenamed(uri) {
  try {
    const { hostname, protocol } = new URL(uri);
    return (
      hostname &&
      protocol &&
      PROTOCOLS_TO_RENAME.includes(protocol) &&
      !KNOWN_DOMAINS.includes(hostname)
    );
  } catch {
    return false;
  }
}

function shouldIgnorePredicateForRename(predicate) {
  return PREDICATES_TO_IGNORE_FOR_RENAME.includes(predicate.value);
}

/**
 * Renames the URI into a new one in a known domain. First fetches an existing
 * URI from the triplestore before creating a new random one.
 *
 * @async
 * @function
 * @param {NamedNode} oldUri - The old URI that needs to be translated.
 * @returns {Object} An object with properties `sameAsTriple` (with a triple
 * representing equivalence with the old URI if translated) and `newUri` (the
 * new URI).
 */
async function renameUri(oldUri) {
  const connectionOptions = {
    sparqlEndpoint: HIGH_LOAD_DATABASE_ENDPOINT,
    mayRetry: true,
  };
  const queryResult = await querySudo(
    `
  PREFIX owl: ${termToString(ns.owl``)}
    SELECT ?newURI WHERE {
      ?newURI owl:sameAs ${termToString(oldUri)} .
    } LIMIT 1
  `,
    {},
    connectionOptions,
  );
  const parser = new SparqlJsonParser();
  const parsedResults = parser.parseJsonResults(queryResult);
  let newUri;
  if (parsedResults.length > 0) {
    newUri = parsedResults[0].newURI;
  } else {
    const hash = createHash('sha512');
    hash.update(oldUri.value);
    const digest = hash.digest('base64url');
    newUri = namedNode(`${RENAME_DOMAIN}${digest}`);
  }
  return {
    sameAsTriple: quad(newUri, ns.owl`sameAs`, oldUri),
    newUri,
  };
}
