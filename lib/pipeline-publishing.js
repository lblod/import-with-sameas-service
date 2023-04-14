import * as mu from 'mu';
import * as cts from '../constants';
import * as tsk from './task';
import * as grph from './graph';
import * as edd from './pipeline-execute-diff-deletes';
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
      await tsk.appendTaskError(task, e.message);
      await tsk.updateTaskStatus(task, cts.STATUS_FAILED);
    } catch (e2) {
      //TODO
    }
  }
}

/**
 * Some detour in moving data from one graph to another is needed, because:
 *   1. Mu-auth doesn't support COPY statements
 *   2. A simple insert/where generates too long running statements for the
 *      database. Batching is not legal SPARQL.
 *
 * @async
 * @function
 * @param {NamedNode} task -
 * @returns {undefined} Nothing
 */
async function publishTriples(task) {
  try {
    if (task?.publishRetries === undefined) task.publishRetries = 0;
    if (task.publishRetries < cts.MAX_RETRIES) task.publishRetries++;
    const tripleStore = await grph.getTriples(task);
    await grph.writeTriplesToGraph(
      cts.TARGET_GRAPH,
      tripleStore,
      cts.BATCH_SIZE
    );
  } catch (e) {
    if (task.publishRetries >= cts.MAX_RETRIES) throw e;
    setTimeout(async () => {
      await publishTriples(task);
    }, cts.RETRY_WAIT_INTERVAL);
  }
}

/**
 * TODO
 *
 * @async
 * @function
 * @param {NamedNode} task -
 * @returns {undefined} Nothing
 */
async function rollbackInserts(task) {
  try {
    if (task?.publishRollbackRetries === undefined)
      task.publishRollbackRetries = 0;
    if (task.publishRollbackRetries < cts.MAX_RETRIES)
      task.publishRollbackRetries++;
    const tripleStore = await grph.getTriples(task);
    await grph.deleteTriplesFromGraph(
      cts.TARGET_GRAPH,
      tripleStore,
      cts.BATCH_SIZE
    );
  } catch (e) {
    if (task.publishRollbackRetries >= cts.MAX_RETRIES) throw e;
    setTimeout(async () => {
      rollbackInserts(task);
    }, cts.RETRY_WAIT_INTERVAL);
  }
}
