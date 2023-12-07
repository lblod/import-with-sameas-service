import * as mu from 'mu';
import * as cts from '../constants';
import * as tsk from './task';
import * as grph from './graph';
import * as edd from './pipeline-execute-diff-deletes';
import * as uti from './utils';
import * as N3 from 'n3';
import { BASES as bs } from '../constants';
const { literal } = N3.DataFactory;

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
export async function run(task, withDeletes) {
  try {
    await tsk.updateTaskStatus(task, cts.STATUS_BUSY);
    if (withDeletes) await edd.executeDiffDeletes(task);
    await publishTriples(task);
    const graphContainer = { id: literal(mu.uuid()) };
    graphContainer.node = bs.dataContainer(graphContainer.id.value);
    await grph.appendTaskResultGraph(task, graphContainer, cts.TARGET_GRAPH);
    await tsk.updateTaskStatus(task, cts.STATUS_SUCCESS);
  } catch (e) {
    try {
      console.error(e);
      await rollbackInserts(task);
      if (withDeletes) await edd.rollbackDeletes(task);
    } finally {
      await tsk.appendTaskError(task, e.message);
      await tsk.updateTaskStatus(task, cts.STATUS_FAILED);
    }
  }
}

/**
 * Retrieves triples from file or graph and publishes those triples in the
 * target graph. This is retried on failure for a number of times before giving
 * up and signalling that potential changes need to be rolled back.
 *
 * Some detour in moving data from one graph to another is needed, because:
 *   1. Mu-auth doesn't support COPY statements
 *   2. A simple insert/where generates too long running statements for the
 *      database. Batching is not legal SPARQL.
 *
 * @async
 * @function
 * @param {NamedNode} task - The task for which to get triples from.
 * @returns {undefined} Nothing
 */
async function publishTriples(task) {
  if (task?.publishRetries === undefined) task.publishRetries = 0;
  while (task.publishRetries < cts.MAX_RETRIES) {
    task.publishRetries++;
    try {
      await grph.getTriplesInFileAndApplyByBatch(
        task,
        async (tripleStore, _derivedFrom) => {
          await grph.writeTriplesToGraph(
            cts.TARGET_GRAPH,
            tripleStore,
            cts.BATCH_SIZE
          );
        }
      );
      break;
    } catch (e) {
      if (task.publishRetries >= cts.MAX_RETRIES) throw e;
      await uti.sleep(cts.RETRY_WAIT_INTERVAL);
    }
  }
}

/**
 * If inserts have failed, do the opposite: delete those triples from the
 * triplestore. This is retried a number of times on failure before signalling
 * total failure. When this also fails, there is no clean recovery possible.
 *
 * @async
 * @function
 * @param {NamedNode} task - Task for which to rollback any potential changes.
 * @returns {undefined} Nothing
 */
async function rollbackInserts(task) {
  if (task?.publishRollbackRetries === undefined)
    task.publishRollbackRetries = 0;
  while (task.publishRollbackRetries < cts.MAX_RETRIES) {
    task.publishRollbackRetries++;
    try {
      await grph.getTriplesInFileAndApplyByBatch(task, async (tripleStore) => {
        await grph.deleteTriplesFromGraph(
          cts.TARGET_GRAPH,
          tripleStore,
          cts.BATCH_SIZE
        );
      });

      break;
    } catch (e) {
      if (task.publishRollbackRetries >= cts.MAX_RETRIES) throw e;
      await uti.sleep(cts.RETRY_WAIT_INTERVAL);
    }
  }
}
