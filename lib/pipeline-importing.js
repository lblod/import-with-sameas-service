import * as mu from 'mu';
import * as cts from '../constants';
import * as tsk from './task';
import * as grph from './graph';
import * as N3 from 'n3';
import { BASES as bs } from '../constants';
const { literal } = N3.DataFactory;

/**
 * Run the importing pipeline. It loads triples from the triplestore or from
 * files depending on the way the inputContainer is set up and writes those
 * triples to the triplestore. It also updates the task along the process.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} task - Represents the task for wich to start the process.
 * @returns {undefined} Nothing
 */
export async function run(task) {
  try {
    await tsk.updateTaskStatus(task, cts.STATUS_BUSY);

    // Some detour in moving data from one graph to another is needed, because:
    //   1. Mu-auth doesn't support COPY statements
    //   2. A simple insert/where generates too long running statements for the
    //      database. Batching is not legal SPARQL.
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
