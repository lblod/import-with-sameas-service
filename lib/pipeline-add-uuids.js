import { v4 as uuidv4, v5 as uuidv5 } from 'uuid';
import { querySudo } from '@lblod/mu-auth-sudo';
import { DataFactory } from 'n3';
import { termToString } from 'rdf-string-ttl';
import { SparqlJsonParser } from 'sparqljson-parse';
import { LRUCache } from 'lru-cache';
import {
  STATUS_BUSY,
  STATUS_SUCCESS,
  STATUS_FAILED,
  HIGH_LOAD_DATABASE_ENDPOINT,
  NAMESPACES,
  BASES,
} from '../constants';
import { appendTempFile, writeTtlFile, makeEmptyFile } from './file-helpers';
import {
  updateTaskStatus,
  appendTaskResultFile,
  appendTaskError,
} from './task';
import {
  getTriplesInFileAndApplyByBatch,
  appendTaskResultGraph,
} from './graph';
import { storeToTtl } from './utils';

const { literal } = DataFactory;
const connectionOptions = {
  sparqlEndpoint: HIGH_LOAD_DATABASE_ENDPOINT,
  mayRetry: true,
};

const JSON_PARSER = new SparqlJsonParser();
const UUID_CACHE = new LRUCache({
  max: 500000,
  fetchMethod: async (subject) => {
    const response = await querySudo(
      `
      PREFIX mu: ${termToString(NAMESPACES.mu``)}
      SELECT ?uuid WHERE {
        ${termToString(subject)} mu:uuid ?uuid .
      } LIMIT 1
    `,
      {},
      connectionOptions
    );
    const id =
      JSON_PARSER.parseJsonResults(response)[0]?.uuid ||
      literal(uuidv5(subject.value, uuidv5.URL));
    return id;
  },
});

/**
 * Run the pipeline for adding UUIDs. It loads triples from the triplestore or
 * from files depending on the way the inputContainer is set up and creates a
 * new UUID for every subject if none exists. If one already exists, it is
 * added to the resultsContainer. It also updates the task along the process.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} task - Represents the task for wich to start the process.
 * @returns {undefined} Nothing
 */
export async function run(task) {
  try {
    await updateTaskStatus(task, STATUS_BUSY);
    const tempFiles = new Map();

    await getTriplesInFileAndApplyByBatch(
      task,
      async (tripleStore, derivedFrom) => {
        if (!tempFiles.has(derivedFrom)) {
          const tempTtlFile = `/share/complemented-triples-${uuidv4()}.ttl`;
          await makeEmptyFile(tempTtlFile); // make an empty file so if there is no triple to complement, don't break the job
          tempFiles.set(derivedFrom, tempTtlFile);
        }
        const tempTtlFile = tempFiles.get(derivedFrom);
        const complementedTripleStore = await addMuUUIDs(tripleStore);
        const complementedTripleString = await storeToTtl(
          complementedTripleStore
        );
        await appendTempFile(complementedTripleString, tempTtlFile);
      }
    );
    const fileContainer = {
      id: literal(uuidv4()),
      node: BASES.dataContainer(task.id.value),
    };
    for (const [derivedFrom, tempTtlFile] of tempFiles.entries()) {
      const mirroredFile = await writeTtlFile(
        task.graph,
        tempTtlFile,
        'complemented-triples.ttl',
        derivedFrom
      );
      await appendTaskResultFile(task, fileContainer, mirroredFile);
    }
    const graphContainer = { id: literal(uuidv4()) };
    graphContainer.node = BASES.dataContainer(graphContainer.id.value);
    await appendTaskResultGraph(task, graphContainer, fileContainer.node);
    await updateTaskStatus(task, STATUS_SUCCESS);
  } catch (err) {
    console.error(err);
    await appendTaskError(task, err.message);
    await updateTaskStatus(task, STATUS_FAILED);
  }
}

/**
 * Collects all subjects from the store, fetches the existing UUID from the
 * triplestore for the subject. If the UUID exists. add it to the results or
 * otherwise it creates a new one.
 *
 * @async
 * @function
 * @param {N3.Store} store - Store to start from.
 * @returns {N3.Store} Same store as the input (really, same reference), but
 * with the UUID triples destructively (non-functional) added to it.
 */
async function addMuUUIDs(store) {
  // Get all (unique) subjects from store that have a type.
  const subjects = store.getSubjects(NAMESPACES.rdf`type`);
  for (const sub of subjects) {
    const id = await UUID_CACHE.fetch(sub);
    // Always push a UUID triple on the results.
    store.addQuad(sub, NAMESPACES.mu`uuid`, id);
  }
  return store;
}
