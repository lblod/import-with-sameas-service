import * as mu from 'mu';
import * as N3 from 'n3';
import * as cts from '../constants';
import * as file from './file-helpers';
import * as tsk from './task';
import * as grph from './graph';
import * as uti from './utils';
import { NAMESPACES as ns } from '../constants';
import { BASES as bs } from '../constants';
const { literal } = N3.DataFactory;

/**
 * Run the pipeline for adding a harvesting tag, which is just a triple that
 * indicates that subject originates from harvesting. It loads triples from the
 * triplestore or from files depending on the way the inputContainer is set up
 * and adds triple for every subject. If one already exists, the data is added
 * to the resultsContainer. It also updates the task along the process.
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

    const tripleStore = await grph.getTriples(task);
    const complementedTripleStore = await addHarvestingTag(tripleStore);
    const complementedTripleString = await uti.storeToTtl(
      complementedTripleStore
    );
    const fileContainer = {
      id: literal(mu.uuid()),
      node: bs.dataContainer(task.id.value),
    };
    const mirroredFile = await file.writeTtlFile(
      task.graph,
      complementedTripleString,
      'complemented-triples.ttl'
    );
    await tsk.appendTaskResultFile(task, fileContainer, mirroredFile);
    const graphContainer = { id: literal(mu.uuid()) };
    graphContainer.node = bs.dataContainer(graphContainer.id.value);
    await grph.appendTaskResultGraph(task, graphContainer, fileContainer.node);
    await tsk.updateTaskStatus(task, cts.STATUS_SUCCESS);
  } catch (err) {
    console.error(err);
    await tsk.appendTaskError(task, err.message);
    await tsk.updateTaskStatus(task, cts.STATUS_FAILED);
  }
}

/**
 * Collects all subjects from the store and adds a triple to it describing that
 * this data has been harvested.
 *
 * @async
 * @function
 * @param {N3.Store} store - Store to start from.
 * @returns {N3.Store} Same store as the input (really, same reference), but
 * with the tag triples destructively (non-functional) added to it.
 */
async function addHarvestingTag(store) {
  // Get all (unique) subjects from store that have a type.
  const subjects = store.getSubjects(ns.rdf`type`);
  for (const sub of subjects)
    store.addQuad(sub, ns.prov`wasGeneratedBy`, ns.app`lblod-harvesting`);
  return store;
}
