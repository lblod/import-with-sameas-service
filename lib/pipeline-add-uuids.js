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

export async function run(task) {
  try {
    await tsk.updateTaskStatus(task, cts.STATUS_BUSY);

    const tripleStore = await grph.getTriples(task);
    const complementedTripleStore = await addMuUUIDs(tripleStore);
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
    await appendTaskResultFile(task, fileContainer, mirroredFile);

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

async function appendTaskResultFile(task, container, fileUri) {
  return mas.updateSudo(`
    ${cts.SPARQL_PREFIXES}
    INSERT DATA {
      GRAPH ${rst.termToString(task.graph)} {
        ${rst.termToString(container.node)}
          a nfo:DataContainer ;
          mu:uuid ${rst.termToString(container.id)} ;
          task:hasFile ${rst.termToString(fileUri)} .
        ${rst.termToString(task.task)}
          task:resultsContainer ${rst.termToString(container.node)}.
      }
    }
  `);
}
