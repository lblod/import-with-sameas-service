import * as mu from 'mu';
import * as cts from '../constants';
import * as tsk from './task';
import * as grph from './graph';
import * as uti from './utils';
import * as N3 from 'n3';
import { BASES as bs } from '../constants';
const { literal } = N3.DataFactory;

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
    await tsk.updateTaskStatus(task, cts.STATUS_BUSY);
    await executeDiffDeletes(task);
    const graphContainer = { id: literal(mu.uuid()) };
    graphContainer.node = bs.dataContainer(graphContainer.id.value);
    await grph.appendTaskResultGraph(task, graphContainer, cts.TARGET_GRAPH);
    await tsk.updateTaskStatus(task, cts.STATUS_SUCCESS);
  } catch (e) {
    try {
      console.error(e);
      await rollbackDeletes(task);
    } finally {
      await tsk.appendTaskError(task, e.message);
      await tsk.updateTaskStatus(task, cts.STATUS_FAILED);
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
export async function executeDiffDeletes(task) {
  if (task?.diffDeletesRetries === undefined) task.diffDeletesRetries = 0;
  while (task.diffDeletesRetries < cts.MAX_RETRIES) {
    task.diffDeletesRetries++;
    try {
      await grph.getDeletedTriplesInFileAndApplyByBatch(
        task,
        async (deletedTriples, _derivedFrom) => {
          await grph.deleteTriplesFromGraph(
            cts.TARGET_GRAPH,
            deletedTriples,
            cts.BATCH_SIZE,
          );
        },
      );

      break;
    } catch (e) {
      if (task.diffDeletesRetries >= cts.MAX_RETRIES) throw e;
      await uti.sleep(cts.RETRY_WAIT_INTERVAL);
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
  while (task.diffDeletesRollbackRetries < cts.MAX_RETRIES) {
    task.diffDeletesRollbackRetries++;
    try {
      await grph.getDeletedTriplesInFileAndApplyByBatch(
        task,
        async (deletedTriples) => {
          await grph.writeTriplesToGraph(
            cts.TARGET_GRAPH,
            deletedTriples,
            cts.BATCH_SIZE,
          );
        },
      );
      break;
    } catch (e) {
      if (task.diffDeletesRollbackRetries >= cts.MAX_RETRIES) throw e;
      await uti.sleep(cts.RETRY_WAIT_INTERVAL);
    }
  }
}
