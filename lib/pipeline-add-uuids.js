import * as mu from 'mu';
import * as mas from '@lblod/mu-auth-sudo';
import * as N3 from 'n3';
import * as cts from '../constants';
import * as file from './file-helpers';
import * as tsk from './task';
import * as grph from './graph';
import * as uti from './utils';
import * as rst from 'rdf-string-ttl';
import * as sjp from 'sparqljson-parse';
import { NAMESPACES as ns } from '../constants';
import { BASES as bs } from '../constants';
const { literal } = N3.DataFactory;

/**
 * Run the pipeline for adding UUIDs. It loads triples from the triplestore or
 * from files depending on the way the inputContainer is set up and creates a
 * new UUID for every subject if none exists. If one already exists, it is
 * added to the resultsContainer. It also updates the task along the process.
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

    const tempFiles = new Map();

    await grph.getTriplesInFileAndApplyByBatch(
      task,
      async (tripleStore, derivedFrom) => {
        if (!tempFiles.has(derivedFrom)) {
          tempFiles.set(
            derivedFrom,
            `/share/complemented-triples-${mu.uuid()}.ttl`,
          );
        }
        const tempTtlFile = tempFiles.get(derivedFrom);
        const complementedTripleStore = await addMuUUIDs(tripleStore);
        const complementedTripleString = await uti.storeToTtl(
          complementedTripleStore,
        );
        await file.appendTempFile(complementedTripleString, tempTtlFile);
      },
    );

    const fileContainer = {
      id: literal(mu.uuid()),
      node: bs.dataContainer(task.id.value),
    };

    for (const [derivedFrom, tempTtlFile] of tempFiles.entries()) {
      const mirroredFile = await file.writeTtlFile(
        task.graph,
        tempTtlFile,
        'complemented-triples.ttl',
        derivedFrom,
      );
      await tsk.appendTaskResultFile(task, fileContainer, mirroredFile);

      const graphContainer = { id: literal(mu.uuid()) };
      graphContainer.node = bs.dataContainer(graphContainer.id.value);
      await grph.appendTaskResultGraph(
        task,
        graphContainer,
        fileContainer.node,
      );
    }

    await tsk.updateTaskStatus(task, cts.STATUS_SUCCESS);
  } catch (err) {
    console.error(err);
    await tsk.appendTaskError(task, err.message);
    await tsk.updateTaskStatus(task, cts.STATUS_FAILED);
  }
}

/**
 * Collects all subjects from the store, fetches the existing UUID from the
 * triplestore for the subject. If the UUID exists. add it to the results or
 * otherwise it creates a new one.
 *
 * @async
 * @function
 * @param {N3.Store} store - Store to start from.
 * @returns {N3.Store} Same store as the input (really, same reference), but
 * with the UUID triples destructively (non-functional) added to it.
 */
async function addMuUUIDs(store) {
  // Get all (unique) subjects from store that have a type.
  const subjects = store.getSubjects(ns.rdf`type`);
  const parser = new sjp.SparqlJsonParser();
  for (const sub of subjects) {
    // Does the subject have a UUID already?
    const response = await mas.querySudo(`
      PREFIX mu: ${rst.termToString(ns.mu``)}
      SELECT ?uuid WHERE {
        ${rst.termToString(sub)} mu:uuid ?uuid .
      } LIMIT 1
    `);
    const uuid =
      parser.parseJsonResults(response)[0]?.uuid || literal(mu.uuid());
    // Always push a UUID triple on the results.
    store.addQuad(sub, ns.mu`uuid`, uuid);
  }
  return store;
}
