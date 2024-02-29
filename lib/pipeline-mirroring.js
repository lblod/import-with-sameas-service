import * as mu from 'mu';
import * as mas from '@lblod/mu-auth-sudo';
import * as file from './file-helpers';
import * as tsk from './task';
import * as grph from './graph';
import * as uti from './utils';
import * as rst from 'rdf-string-ttl';
import * as sjp from 'sparqljson-parse';
import * as N3 from 'n3';
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
const { namedNode, literal, quad } = N3.DataFactory;

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
export async function run(task) {
  try {
    await tsk.updateTaskStatus(task, STATUS_BUSY);
    const mappingCache = {};

    const tempFiles1 = new Map();
    await grph.getTriplesInFileAndApplyByBatch(
      task,
      async (store, derivedFrom) => {
        if (!tempFiles1.has(derivedFrom)) {
          const tempTtlFile = `/share/mirrored-triples-${mu.uuid()}.ttl`;
          file.makeEmptyFile(tempTtlFile);

          tempFiles1.set(derivedFrom, tempTtlFile);
        }
        const tempTtlFile = tempFiles1.get(derivedFrom);
        const renamedTriples = await renameSubjects(store, mappingCache);
        const renamedTriplesString = await uti.storeToTtl(renamedTriples);
        await file.appendTempFile(renamedTriplesString, tempTtlFile);
      }
    );

    const tempFiles2 = new Map();
    await grph.getTriplesFromTempFilesAndApplyByBatch(
      tempFiles1,
      async (store, derivedFrom) => {
        if (!tempFiles2.has(derivedFrom)) {
          const tempTtlFile = `/share/mirrored-triples-${mu.uuid()}.ttl`;
          file.makeEmptyFile(tempTtlFile);

          tempFiles2.set(derivedFrom, tempTtlFile);
        }
        const tempTtlFile = tempFiles2.get(derivedFrom);
        const renamedTriples = await renameObjects(store, mappingCache);
        const renamedTriplesString = await uti.storeToTtl(renamedTriples);
        await file.appendTempFile(renamedTriplesString, tempTtlFile);
      }
    );

    const tempFiles3 = new Map();
    const reverseMappingCache = uti.reverseCache(mappingCache);
    await grph.getTriplesFromTempFilesAndApplyByBatch(
      tempFiles2,
      async (store, derivedFrom) => {
        if (!tempFiles3.has(derivedFrom)) {
          const tempTtlFile = `/share/mirrored-triples-${mu.uuid()}.ttl`;
          file.makeEmptyFile(tempTtlFile);

          tempFiles3.set(derivedFrom, tempTtlFile);
        }
        const tempTtlFile = tempFiles3.get(derivedFrom);
        const renamedTriples = await addSameAsTriples(
          store,
          reverseMappingCache
        );
        const renamedTriplesString = await uti.storeToTtl(renamedTriples);
        await file.appendTempFile(renamedTriplesString, tempTtlFile);
      }
    );

    //Remove intermediary temporary files
    for (const tempFile of tempFiles1.values()) await file.removeFile(tempFile);
    for (const tempFile of tempFiles2.values()) await file.removeFile(tempFile);

    const fileContainer = {
      id: literal(mu.uuid()),
      node: bs.dataContainer(task.id.value),
    };
    // No need as we use file during the publishing step
    //const sameAsGraph = `http://mu.semte.ch/graphs/harvesting/tasks/mirroring/${task.id}`;
    //await writeTriplesToGraph(sameAsGraph, ntTriples, BATCH_SIZE);
    for (const [derivedFrom, tempTtlFile] of tempFiles3.entries()) {
      const mirroredFile = await file.writeTtlFile(
        task.graph,
        tempTtlFile,
        'mirrored-triples.ttl',
        derivedFrom
      );
      await tsk.appendTaskResultFile(task, fileContainer, mirroredFile);
    }
    const graphContainer = { id: literal(mu.uuid()) };
    graphContainer.node = bs.dataContainer(graphContainer.id.value);

    await grph.appendTaskResultGraph(task, graphContainer, fileContainer.node);
    await tsk.updateTaskStatus(task, STATUS_SUCCESS);
  } catch (e) {
    console.error(e);
    await tsk.appendTaskError(task, e.message);
    await tsk.updateTaskStatus(task, STATUS_FAILED);
  }
}

/**
 * Transforms the triples where their subjects are not in a known domain. The
 * input triples are traversed and if their subject is not in a known domain
 * (configurable) a new URI is created for that subject. URIs are kept in a map
 * during the process for speeding up, but also to be able to translate URIs in
 * object position that have been seen before (later).
 *
 * @async
 * @function
 * @param {N3.Store} store - Store of triples to be translated.
 * @param {Object} namesDict - A regular JavaScript object that is passed
 * around as a cache for subjects to their new URI. Keys are the old URIs and
 * the values are the new mirrored URIs.
 * @returns {N3.Store} Store of triples with translated subjects.
 */
async function renameSubjects(store, namesDict) {
  const renamedStore = new N3.Store();
  for (const quad of store) {
    const renamedTriple = {};
    if (namesDict[quad.subject.value]) {
      // URI seen and translated before. Take that result.
      // Small performance improvement while streaming files with random cuts,
      // because we are only going through the subjects.
      renamedTriple.s = namesDict[quad.subject.value];
    } else if (needsToBeRenamed(quad.subject.value)) {
      // Not translated before, but is necessary.
      const newUri = await renameUri(quad.subject);
      // Keep the translation for later reference.
      namesDict[quad.subject.value] = newUri;
      renamedTriple.s = newUri;
    } else {
      // No translation needed.
      renamedTriple.s = quad.subject;
    }
    renamedTriple.p = quad.predicate;
    renamedTriple.o = quad.object;
    renamedStore.addQuad(renamedTriple.s, renamedTriple.p, renamedTriple.o);
  }
  return renamedStore;
}

/**
 * Transforms the triples where their objects are not in a known domain. The
 * input triples are traversed and if their object is not in a known domain
 * (configurable) the cache object is used to link it to the new URI generated
 * in a previous traversal for the subjects (see `renameSubjects`).
 *
 * @async
 * @function
 * @param {N3.Store} store - Store of triples to be translated.
 * @param {Object} namesDict - A regular JavaScript object that is passed
 * around as a cache for subjects to their new URI. Keys are the old URIs and
 * the values are the new mirrored URIs.
 * @returns {N3.Store} Store of triples with translated objects.
 */
async function renameObjects(store, namesDict) {
  const renamedStore = new N3.Store();
  for (const quad of store) {
    const renamedTriple = {};
    renamedTriple.s = quad.subject;
    renamedTriple.p = quad.predicate;
    if (
      quad.object.termType === 'NamedNode' &&
      !shouldIgnorePredicateForRename(quad.predicate)
    ) {
      if (namesDict[quad.object.value]) {
        renamedTriple.o = namesDict[quad.object.value];
      } else if (needsToBeRenamed(quad.object.value)) {
        const newUri = await renameUri(quad.object);
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
 * Adds to the input triples a triple to link via `owl:sameAs` from the
 * equivalent URI that has been generated in a previous part of the pipeline
 * (see `renameSubjects`, and `renameObjects`) to the original URI.
 *
 * @async
 * @function
 * @param {N3.Store} store - Store of triples to be translated.
 * @param {Object} reverseNamesDict - A regular JavaScript object that is
 * passed around as a cache for subjects to their new URI, but this time they
 * are swapped around. Keys are the new mirrored triples and the values are the
 * old URIs.
 * @returns {N3.Store} Store of triples with several `owl:sameAs` triples added
 * for the subjects in this store.
 */
async function addSameAsTriples(store, reverseNamesDict) {
  const subjects = store.getSubjects();
  for (const subject of subjects)
    if (reverseNamesDict[subject.value])
      store.addQuad(subject, ns.owl`sameAs`, reverseNamesDict[subject.value]);
  return store;
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
  } catch (e) {
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
  const queryResult = await mas.querySudo(
    `
  PREFIX owl: ${rst.termToString(ns.owl``)}
    SELECT ?newURI WHERE {
      ?newURI owl:sameAs ${rst.termToString(oldUri)} .
    } LIMIT 1
  `,
    {},
    connectionOptions
  );
  const parser = new sjp.SparqlJsonParser();
  const parsedResults = parser.parseJsonResults(queryResult);
  const newUri =
    parsedResults.length > 0
      ? parsedResults[0].newURI
      : namedNode(`${RENAME_DOMAIN}${mu.uuid()}`);
  return newUri;
}
