import { uuid } from 'mu';
import { DataFactory } from 'n3';
import { STATUS_BUSY, STATUS_SUCCESS, STATUS_FAILED } from '../constants';
import { makeEmptyFile, appendTempFile, writeTtlFile } from './file-helpers';
import {
  updateTaskStatus,
  appendTaskError,
  appendTaskResultFile,
  getVendor,
} from './task';
import {
  getTriplesInFileAndApplyByBatch,
  appendTaskResultGraph,
} from './graph';
import { storeToTtl } from './utils';
import { NAMESPACES as ns, BASES as bs } from '../constants';
const { literal } = DataFactory;

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
export async function run(task, signal = {}) {
  try {
    if(signal.aborted) {
      throw new Error('task aborted');
    }
    await updateTaskStatus(task, STATUS_BUSY);

    const vendorForTask = await getVendor(task.task);

    const tempFiles = new Map();
    await getTriplesInFileAndApplyByBatch(
      task,
      async (tripleStore, derivedFrom) => {
        if(signal.aborted) {
          throw new Error('task aborted');
        }
        if (!tempFiles.has(derivedFrom)) {
          const tempTtlFile = `/share/complemented-triples-${uuid()}.ttl`;
          await makeEmptyFile(tempTtlFile);
          tempFiles.set(derivedFrom, tempTtlFile);
        }
        const tempTtlFile = tempFiles.get(derivedFrom);
        const complementedTripleStore = await addVendorTag(
          tripleStore,
          vendorForTask,
        );
        const complementedTripleString = await storeToTtl(
          complementedTripleStore,
        );
        await appendTempFile(complementedTripleString, tempTtlFile);
      },
    );

    const fileContainer = {
      id: literal(uuid()),
      node: bs.dataContainer(task.id.value),
    };
    for (const [derivedFrom, tempTtlFile] of tempFiles.entries()) {
      if(signal.aborted) {
        throw new Error('task aborted');
      }
      const mirroredFile = await writeTtlFile(
        task.graph,
        tempTtlFile,
        'complemented-triples.ttl',
        derivedFrom,
        task.jobId,
        'add-vendor-tag'
      );

      await appendTaskResultFile(task, fileContainer, mirroredFile);
    }
    const graphContainer = { id: literal(uuid()) };
    graphContainer.node = bs.dataContainer(graphContainer.id.value);
    await appendTaskResultGraph(task, graphContainer, fileContainer.node);
    await updateTaskStatus(task, STATUS_SUCCESS);
  } catch (err) {
    console.error(err);
    await appendTaskError(task, err.message);
    await updateTaskStatus(task, STATUS_FAILED);
  }
}

/**
 * Collects all subjects from the store and adds a triple to it describing that
 * this data has been harvested for a specific vendor.
 *
 * @async
 * @function
 * @param {N3.Store} store - Store to start from.
 * @returns {N3.Store} Same store as the input (really, same reference), but
 * with the tag triples destructively (non-functional) added to it.
 */
async function addVendorTag(store, vendor) {
  // Get all (unique) subjects from store that have a type.
  const subjects = store.getSubjects(ns.rdf`type`);
  for (const sub of subjects)
    store.addQuad(sub, ns.prov`wasAssociatedWith`, vendor);
  return store;
}
