import * as mu from 'mu';
import * as cts from '../constants';
import * as tsk from './task';
import * as grph from './graph';
import * as N3 from 'n3';
import { BASES as bs } from '../constants';
const { literal } = N3.DataFactory;

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
      await tsk.appendTaskError(task, e.message);
      await tsk.updateTaskStatus(task, cts.STATUS_FAILED);
    } catch (e2) {
      //TODO
    }
  }
}

/**
 * TODO
 */
export async function executeDiffDeletes(task) {
  try {
    if (task?.diffDeletesRetries === undefined) task.diffDeletesRetries = 0;
    if (task.diffDeletesRetries < cts.MAX_RETRIES) task.diffDeletesRetries++;
    const deletedTriples = await grph.getDeletedTriplesFromInputContainer(
    );
    await grph.deleteTriplesFromGraph(
      cts.TARGET_GRAPH,
      deletedTriples,
      cts.BATCH_SIZE
    );
  } catch (e) {
    if (task.diffDeletesRetries >= cts.MAX_RETRIES) throw e;
    setTimeout(async () => {
      executeDiffDeletes(task);
    }, cts.RETRY_WAIT_INTERVAL);
        task
  }
}

/**
 * TODO
 */
export async function rollbackDeletes(task) {
  try {
    if (task.diffDeletesRollbackRetries === undefined)
      task.diffDeletesRollbackRetries = 0;
    if (task.diffDeletesRollbackRetries < cts.MAX_RETRIES)
      task.diffDeletesRollbackRetries++;
    const deletedTriples = await tsk.getDeletedTriplesFromInputContainer(task);
    await grph.writeTriplesToGraph(
      cts.TARGET_GRAPH,
      deletedTriples,
      cts.BATCH_SIZE
    );
  } catch (e) {
    if (task.diffDeletesRollbackRetries >= cts.MAX_RETRIES) throw e;
    setTimeout(async () => {
      rollbackDeletes(task);
    }, cts.RETRY_WAIT_INTERVAL);
  }
}
