import { uuid } from 'mu';
import {
  STATUS_BUSY,
  STATUS_SUCCESS,
  STATUS_FAILED,
  TARGET_GRAPH,
  BATCH_SIZE,
  MAX_RETRIES,
  RETRY_WAIT_INTERVAL,
} from '../constants';
import { updateTaskStatus, appendTaskError } from './task';
import {
  getTriplesInFileAndApplyByBatch,
  writeTriplesToGraph,
  deleteTriplesFromGraph,
  appendTaskResultGraph,
} from './graph';
import {
  executeDiffDeletes,
  rollbackDeletes,
} from './pipeline-execute-diff-deletes';
import { sleep } from './utils';
import { DataFactory } from 'n3';
import { BASES as bs } from '../constants';
const { literal } = DataFactory;

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
    await updateTaskStatus(task, STATUS_BUSY);
    if (withDeletes) await executeDiffDeletes(task);
    await publishTriples(task);
    const graphContainer = { id: literal(uuid()) };
    graphContainer.node = bs.dataContainer(graphContainer.id.value);
    await appendTaskResultGraph(task, graphContainer, TARGET_GRAPH);
    await updateTaskStatus(task, STATUS_SUCCESS);
  } catch (e) {
    try {
      console.error(e);
      await rollbackInserts(task);
      if (withDeletes) await rollbackDeletes(task);
    } finally {
      await appendTaskError(task, e.message);
      await updateTaskStatus(task, STATUS_FAILED);
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
  while (task.publishRetries < MAX_RETRIES) {
    task.publishRetries++;
    try {
      await getTriplesInFileAndApplyByBatch(task, async (tripleStore) => {
        await writeTriplesToGraph(TARGET_GRAPH, tripleStore, BATCH_SIZE);
      });
      break;
    } catch (e) {
      if (task.publishRetries >= MAX_RETRIES) throw e;
      await sleep(RETRY_WAIT_INTERVAL);
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
  while (task.publishRollbackRetries < MAX_RETRIES) {
    task.publishRollbackRetries++;
    try {
      await getTriplesInFileAndApplyByBatch(task, async (tripleStore) => {
        await deleteTriplesFromGraph(TARGET_GRAPH, tripleStore, BATCH_SIZE);
      });

      break;
    } catch (e) {
      if (task.publishRollbackRetries >= MAX_RETRIES) throw e;
      await sleep(RETRY_WAIT_INTERVAL);
    }
  }
}
