import { uuid } from 'mu';
import {
  STATUS_BUSY,
  STATUS_SUCCESS,
  STATUS_FAILED,
  TARGET_GRAPH,
  BATCH_SIZE,
  MAX_RETRIES,
  RETRY_WAIT_INTERVAL,
  ROLLBACK_ENABLED,
} from '../constants';
import { updateTaskStatus, appendTaskError } from './task';
import {
  getDeletedTriplesInFileAndApplyByBatch,
  deleteTriplesFromGraph,
  writeTriplesToGraph,
  appendTaskResultGraph,
} from './graph';
import { sleep } from './utils';
import { DataFactory } from 'n3';
import { BASES as bs } from '../constants';
const { literal } = DataFactory;

/**
 * Find the triples that need to be deleted for the given task and delete them
 * from the triplestore. These triple litely originate from the diff service.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} task - Represents the task for which to delete triples.
 * @returns {undefined} Nothing
 */
export async function run(task) {
  try {
    await updateTaskStatus(task, STATUS_BUSY);
    await executeDiffDeletes(task);
    const graphContainer = { id: literal(uuid()) };
    graphContainer.node = bs.dataContainer(graphContainer.id.value);
    await appendTaskResultGraph(task, graphContainer, TARGET_GRAPH);
    await updateTaskStatus(task, STATUS_SUCCESS);
  } catch (e) {
    try {
      console.error(e);
      if (ROLLBACK_ENABLED) {
        console.log('Attempting rollback...');
        await rollbackDeletes(task);
      } else {
        console.log('Rollback disabled, skipping rollback attempt');
      }
    } finally {
      await appendTaskError(task, e.message);
      await updateTaskStatus(task, STATUS_FAILED);
    }
  }
}

/**
 * Retrieves triples from a specific file for deletes and deletes those triples
 * in the target graph. This is retried on failure for a number of times before
 * giving up and signalling that potential changes need to be rolled back.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} task - Represents the task for which to delete triples.
 * @returns {undefined} Nothing
 */
export async function executeDiffDeletes(task, signal={}) {
  if (task?.diffDeletesRetries === undefined) task.diffDeletesRetries = 0;
  while (task.diffDeletesRetries < MAX_RETRIES) {
    if(signal.aborted) {
      throw new Error('task aborted');
    }
    task.diffDeletesRetries++;
    try {
      await getDeletedTriplesInFileAndApplyByBatch(
        task,
        async (deletedTriples) => {
          if(signal.aborted) {
            throw new Error('task aborted');
          }
          await deleteTriplesFromGraph(
            TARGET_GRAPH,
            deletedTriples,
            BATCH_SIZE,
          );
        },
      );

      break;
    } catch (e) {
      if (task.diffDeletesRetries >= MAX_RETRIES) throw e;
      await sleep(RETRY_WAIT_INTERVAL);
    }
  }
}

/**
 * If deletes have failed, do the opposite: insert those triples from the
 * triplestore. This is retried a number of times on failure before signalling
 * total failure. When this also fails, there is no clean recovery possible.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} task - Task for which to rollback any potential changes.
 * @returns {undefined} Nothing
 */
export async function rollbackDeletes(task) {
  if (task.diffDeletesRollbackRetries === undefined)
    task.diffDeletesRollbackRetries = 0;
  while (task.diffDeletesRollbackRetries < MAX_RETRIES) {
    task.diffDeletesRollbackRetries++;
    try {
      await getDeletedTriplesInFileAndApplyByBatch(
        task,
        async (deletedTriples) => {
          await writeTriplesToGraph(TARGET_GRAPH, deletedTriples, BATCH_SIZE);
        },
      );
      break;
    } catch (e) {
      if (task.diffDeletesRollbackRetries >= MAX_RETRIES) throw e;
      await sleep(RETRY_WAIT_INTERVAL);
    }
  }
}
