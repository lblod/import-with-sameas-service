import { uuid } from 'mu';
import {
  STATUS_BUSY,
  STATUS_SUCCESS,
  STATUS_FAILED,
  SPARQL_PREFIXES,
} from '../constants';
import {
  updateTaskStatus,
  appendTaskError,
  appendTaskResultFile,
} from './task';
import {
  appendTaskResultGraph,
  getTriplesInFileAndApplyByBatch,
  getDeletedTriplesInFileAndApplyByBatch,
} from './graph';
import { appendTempFile, writeTtlFile, makeEmptyFile } from './file-helpers';
import { v5 as uuidv5 } from 'uuid';
import { storeToTtl } from './utils';
import { Store, DataFactory } from 'n3';
import { BASES as bs } from '../constants';
import { NAMESPACES as ns } from '../constants';
const { literal } = DataFactory;
import { SparqlJsonParser } from 'sparqljson-parse';
import { termToString } from 'rdf-string-ttl';
import { querySudo } from '@lblod/mu-auth-sudo';
const parser = new SparqlJsonParser();
const modifiedOn = ns.task`modifiedOn`;
const modifiedBy = ns.task`modifiedBy`;
const modifiedByPred = termToString(ns.task`modifiedBy`);
const modifiedOnPred = termToString(ns.task`modifiedOn`);

/**
 * Run the publishing pipeline. It loads triples from the triplestore or from
 * files depending on the way the inputContainer is set up and writes those
 * triples to the triplestore. It also updates the task along the process.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} task - Represents the task for which to start the
 * process.
 * @param {Boolean} withDeletes - Also perform deletes for this task? This
 * boolean allows two pipelines to share all of their common code.
 * @returns {undefined} Nothing
 */
export async function run(task, signal = {}) {
  try {
    if (signal.aborted) {
      throw new Error('task aborted');
    }
    await updateTaskStatus(task, STATUS_BUSY);

    const tempInsertsFile = {};
    const tempDeletesFile = {};
    const insertUUID = uuidv5(`${task.id.value}/new-inserts`, uuidv5.URL);
    const deletesUUID = uuidv5(`${task.id.value}/to-remove`, uuidv5.URL);
    const intersectsUUID = uuidv5(`${task.id.value}/intersects`, uuidv5.URL);
    const defaultUUID = uuidv5(`${task.id.value}/default`, uuidv5.URL);
    const insertsFileContainer = {
      id: literal(insertUUID),
      node: bs.dataContainer(insertUUID),
    };
    const deletesFileContainer = {
      id: literal(deletesUUID),
      node: bs.dataContainer(deletesUUID),
    };
    const intersectsFileContainer = {
      id: literal(intersectsUUID),
      node: bs.dataContainer(intersectsUUID),
    };
    const defaultResultsContainer = {
      id: literal(defaultUUID),
      node: bs.dataContainer(defaultUUID),
    };

    const now = literal(new Date().toISOString(), ns.xsd`dateTime`);

    await getTriplesInFileAndApplyByBatch(task, async (store, derivedFrom) => {
      if (signal.aborted) {
        throw new Error('task aborted');
      }

      if (!tempInsertsFile.derivedFrom) {
        const insertsTempFile = `/share/new-insert-triples-${uuid()}.ttl`;
        await makeEmptyFile(insertsTempFile);
        tempInsertsFile.derivedFrom = derivedFrom;
        tempInsertsFile.tempResults = insertsTempFile;
      }
      const insertsTempFile = tempInsertsFile.tempResults;

      // All subjects with new inserts get a new `modifiedBy` and `modifiedOn`.
      const subjects = store.getSubjects();
      subjects.forEach((subject) => {
        store.addQuad(subject, modifiedOn, now);
        store.addQuad(subject, modifiedBy, task.job);
      });
      const storeInString = await storeToTtl(store);
      await appendTempFile(storeInString, insertsTempFile);
    });

    // All deletes get a new `modifiedBy` and `modifiedOn`, but also their old
    // `modified...` properties need to be removed.
    await getDeletedTriplesInFileAndApplyByBatch(
      task,
      async (store, derivedFrom) => {
        if (signal.aborted) {
          throw new Error('task aborted');
        }

        if (!tempDeletesFile.derivedFrom) {
          const deletesTempFile = `/share/to-remove-triples-${uuid()}.ttl`;
          await makeEmptyFile(deletesTempFile);
          tempDeletesFile.derivedFrom = derivedFrom;
          tempDeletesFile.tempResults = deletesTempFile;
        }
        const deletesTempFile = tempDeletesFile.tempResults;

        const subjects = store.getSubjects();
        const chunkedSubjects = listToChunks(subjects, 20);

        for (const chunk of chunkedSubjects) {
          const dataResponse = await querySudo(`
          CONSTRUCT {
            ?s ${modifiedOnPred} ?modifiedOn .
            ?s ${modifiedByPred} ?modifiedBy .
          }
          WHERE {
            VALUES ?s { ${chunk.map(termToString).join('\n')} }
            ?s ${modifiedOnPred} ?modifiedOn .
            ?s ${modifiedByPred} ?modifiedBy .
          }
        `);
          const parsedResults = parser.parseJsonResults(dataResponse);
          parsedResults.forEach((triple) =>
            store.addQuad(triple.s, triple.p, triple.o)
          );
          const storeInString = await storeToTtl(store);
          await appendTempFile(storeInString, deletesTempFile);
        }

        const insertStore = new Store();
        subjects.forEach((subject) => {
          insertStore.addQuad(subject, modifiedOn, now);
          insertStore.addQuad(subject, modifiedBy, task.job);
        });
        const insertStoreInString = await storeToTtl(insertStore);
        await appendTempFile(insertStoreInString, tempInsertsFile.tempResults);
      }
    );

    const intersectsFile = await previousIntersectsFile(task);

    if (tempInsertsFile.tempResults) {
      const insertsFile = await writeTtlFile(
        task.graph,
        tempInsertsFile.tempResults,
        'new-insert-triples.ttl',
        tempInsertsFile.derivedFrom,
        task.jobId.value,
        'tag-diff-modified'
      );
      await appendTaskResultFile(task, insertsFileContainer, insertsFile);
      await appendTaskResultGraph(
        task,
        defaultResultsContainer,
        insertsFileContainer.node
      );
    }

    if (tempDeletesFile.tempResults) {
      const deletesFile = await writeTtlFile(
        task.graph,
        tempDeletesFile.tempResults,
        'to-remove-triples.ttl',
        tempDeletesFile.derivedFrom,
        task.jobId.value,
        'tag-diff-modified'
      );
      await appendTaskResultFile(task, deletesFileContainer, deletesFile);
    }

    if (intersectsFile)
      await appendTaskResultFile(task, intersectsFileContainer, intersectsFile);

    await updateTaskStatus(task, STATUS_SUCCESS);
  } catch (e) {
    console.error(e);
    await appendTaskError(task, e.message);
    await updateTaskStatus(task, STATUS_FAILED);
  }
}

async function previousIntersectsFile(task) {
  const result = await querySudo(`
    ${SPARQL_PREFIXES}
    SELECT DISTINCT ?logicalFile
    WHERE {
      GRAPH ${termToString(task.graph)} {
        ${termToString(task.task)}
          a task:Task ;
          task:inputContainer ?inputContainer .
        ?inputContainer
          a nfo:DataContainer ;
          task:hasFile ?logicalFile .
        ?logicalFile
          a nfo:FileDataObject ;
          nfo:fileName "intersect-triples.ttl" .
      }
    }
    LIMIT 1`);
  const parsedResults = parser.parseJsonResults(result);
  return parsedResults[0]?.logicalFile;
}

/*****************************************************************************
 * Basic helpers
 ****************************************************************************/

function listToChunks(list, maxChunkSize) {
  const chunkCount = Math.ceil(list.length / maxChunkSize);
  const result = [];
  for (let i = 0; i < chunkCount; i++)
    result.push(list.slice(i * maxChunkSize, (i + 1) * maxChunkSize));
  return result;
}
