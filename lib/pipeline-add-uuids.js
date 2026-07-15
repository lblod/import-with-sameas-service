import { v5 as uuidv5 } from 'uuid';
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
import {
  appendTempFile,
  writeTtlFile,
  makeEmptyFile,
  removeFile,
} from './file-helpers';
import {
  updateTaskStatus,
  appendTaskResultFile,
  appendTaskError,
  getRecordedResultDerivedFroms,
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
      connectionOptions,
    );
    let id = literal(uuidv5(subject.value, uuidv5.URL));
    try {
      id = JSON_PARSER.parseJsonResults(response)[0]?.uuid || id;
    } catch (e) {
      console.log('warn, could not parse json result', e.message);
    }
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
export async function run(task, signal = {}) {
  // Temp files for the files currently being processed (not yet finalized),
  // keyed by their derivedFrom. Used for finalization and for cleanup of
  // unfinished work on error/abort.
  const tempFiles = new Map();
  // Deterministic so re-runs target the same container and stay idempotent.
  const fileContainer = {
    id: literal(uuidv5(`${task.id.value}/results`, uuidv5.URL)),
    node: BASES.dataContainer(task.id.value),
  };
  try {
    if (signal.aborted) {
      throw new Error('task aborted');
    }
    await updateTaskStatus(task, STATUS_BUSY);

    // Sources already finalized by a previous (interrupted) run. Fetched once
    // so the skip check below is in-memory rather than a query per input file.
    const completedDerivedFroms = await getRecordedResultDerivedFroms(task);

    await getTriplesInFileAndApplyByBatch(
      task,
      async (tripleStore, derivedFrom) => {
        if (signal.aborted) {
          throw new Error('task aborted');
        }
        if (!tempFiles.has(derivedFrom)) {
          // Deterministic name so a temp left behind by an interrupted run is
          // overwritten (truncated) here instead of leaking on disk.
          const tempTtlFile = `/share/complemented-triples-${uuidv5(
            `${task.id.value}/${derivedFrom.value}`,
            uuidv5.URL,
          )}.ttl`;
          await makeEmptyFile(tempTtlFile); // make an empty file so if there is no triple to complement, don't break the job
          tempFiles.set(derivedFrom, tempTtlFile);
        }
        const tempTtlFile = tempFiles.get(derivedFrom);
        const complementedTripleStore = await addMuUUIDs(tripleStore);
        const complementedTripleString = await storeToTtl(
          complementedTripleStore,
        );
        await appendTempFile(complementedTripleString, tempTtlFile);
      },
      {
        // Skip files already fully processed by a previous (interrupted) run so
        // a restart resumes instead of redoing work and duplicating output.
        shouldProcessFile: async (derivedFrom) => {
          if (signal.aborted) {
            throw new Error('task aborted');
          }
          return !completedDerivedFroms.has(derivedFrom.value);
        },
        // Finalize as soon as a file is fully read, so completed work is
        // durably recorded and survives a crash.
        onFileComplete: async (derivedFrom) => {
          if (signal.aborted) {
            throw new Error('task aborted');
          }
          const tempTtlFile = tempFiles.get(derivedFrom);
          if (!tempTtlFile) return;
          const mirroredFile = await writeTtlFile(
            task.graph,
            tempTtlFile,
            'complemented-triples.ttl',
            derivedFrom,
            task.jobId.value,
            'add-uuid',
          );
          await appendTaskResultFile(task, fileContainer, mirroredFile);
          // Finalized: the temp file has been renamed away, drop it from the
          // set of files needing cleanup.
          tempFiles.delete(derivedFrom);
        },
      },
    );

    const graphContainer = {
      id: literal(uuidv5(`${task.id.value}/results-graph`, uuidv5.URL)),
    };
    graphContainer.node = BASES.dataContainer(graphContainer.id.value);
    await appendTaskResultGraph(task, graphContainer, fileContainer.node);
    await updateTaskStatus(task, STATUS_SUCCESS);
  } catch (err) {
    console.error(err);
    await appendTaskError(task, err.message);
    await updateTaskStatus(task, STATUS_FAILED);
  } finally {
    // Best-effort cleanup of temp files for files that were not finalized
    // (e.g. on error or abort), so they don't leak on /share.
    for (const tempTtlFile of tempFiles.values()) {
      try {
        await removeFile(tempTtlFile);
      } catch {
        // already gone (e.g. renamed on finalize) or never created; ignore
      }
    }
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
