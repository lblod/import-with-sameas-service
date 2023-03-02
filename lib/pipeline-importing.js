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

    //Some detour in moving data from one graph to another is needed, cause:
    // 1. mu-auth doesn't support COPY statements
    // 2. a simple insert/where is generates too big statements for the database. Batching is not legal sparql
    const tripleStore = await grph.getTriples(task);
    await grph.writeTriplesToGraph(
      cts.TARGET_GRAPH,
      tripleStore,
      cts.BATCH_SIZE
    );
    const graphContainer = { id: literal(mu.uuid()) };
    graphContainer.node = bs.dataContainer(graphContainer.id.value);
    await grph.appendTaskResultGraph(task, graphContainer, cts.TARGET_GRAPH);

    await tsk.updateTaskStatus(task, cts.STATUS_SUCCESS);
  } catch (e) {
    console.error(e);
    await tsk.appendTaskError(task, e.message);
    await tsk.updateTaskStatus(task, cts.STATUS_FAILED);
  }
}
