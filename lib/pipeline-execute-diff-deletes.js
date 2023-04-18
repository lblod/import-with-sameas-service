import * as mu from 'mu';
import * as cts from '../constants';
import * as tsk from './task';
import * as grph from './graph';
import * as uti from './utils';
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
  if (task?.diffDeletesRetries === undefined) task.diffDeletesRetries = 0;
  while (task.diffDeletesRetries < cts.MAX_RETRIES) {
    task.diffDeletesRetries++;
    try {
      const deletedTriples = await grph.getDeletedTriplesFromInputContainer(
        task
      );
      await grph.deleteTriplesFromGraph(
        cts.TARGET_GRAPH,
        deletedTriples,
        cts.BATCH_SIZE
      );
      break;
    } catch (e) {
      if (task.diffDeletesRetries >= cts.MAX_RETRIES) throw e;
      await uti.sleep(cts.RETRY_WAIT_INTERVAL);
    }
  }
}

/**
 * TODO
 */
export async function rollbackDeletes(task) {
  if (task.diffDeletesRollbackRetries === undefined)
    task.diffDeletesRollbackRetries = 0;
  while (task.diffDeletesRollbackRetries < cts.MAX_RETRIES) {
    task.diffDeletesRollbackRetries++;
    try {
      const deletedTriples = await grph.getDeletedTriplesFromInputContainer(
        task
      );
      await grph.writeTriplesToGraph(
        cts.TARGET_GRAPH,
        deletedTriples,
        cts.BATCH_SIZE
      );
      break;
    } catch (e) {
      if (task.diffDeletesRollbackRetries >= cts.MAX_RETRIES) throw e;
      await uti.sleep(cts.RETRY_WAIT_INTERVAL);
    }
  }
}
