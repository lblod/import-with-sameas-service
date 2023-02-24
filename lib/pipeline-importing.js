import * as mu from 'mu';
import * as cts from '../constants';
import * as tsk from './task';
import * as grph from './graph';
import * as uti from './utils';

export async function run(task) {
  try {
    await tsk.updateTaskStatus(task, cts.STATUS_BUSY.value);

    //Some detour in moving data from one graph to another is needed, cause:
    // 1. mu-auth doesn't support COPY statements
    // 2. a simple insert/where is generates too big statements for the database. Batching is not legal sparql
    const triples = await grph.getTriples(task);
    const ntTriples = uti.triplesToNT(triples);
    await grph.writeTriplesToGraph(cts.TARGET_GRAPH, ntTriples, cts.BATCH_SIZE);
    const graphContainer = { id: mu.uuid() };
    graphContainer.uri = `http://redpencil.data.gift/id/dataContainers/${graphContainer.id}`;
    await grph.appendTaskResultGraph(task, graphContainer, cts.TARGET_GRAPH);

    await tsk.updateTaskStatus(task, cts.STATUS_SUCCESS.value);
  } catch (e) {
    console.error(e);
    await tsk.appendTaskError(task, e.message);
    await tsk.updateTaskStatus(task, cts.STATUS_FAILED.value);
  }
}
