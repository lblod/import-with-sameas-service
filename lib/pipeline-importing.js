import { uuid, sparqlEscapeUri } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';

import {
  STATUS_BUSY,
  STATUS_FAILED,
  STATUS_SUCCESS,
  TARGET_GRAPH,
} from '../constants';

import { updateTaskStatus, appendTaskError } from './task';
import {
  writeTriplesToGraph,
  appendTaskResultGraph,
  getTriples
} from './graph';
import { triplesToNT, filterAsync, processPart } from './utils';
const BATCH_SIZE = process.env.BATCH_SIZE || 100;

export async function run(task) {
  try {
    await updateTaskStatus(task, STATUS_BUSY);

    //Some detour in moving data from one graph to another is needed, cause:
    // 1. mu-auth doesn't support COPY statements
    // 2. a simple insert/where is generates too big statements for the database. Batching is not legal sparql
    let triples = await getTriples(task);
    triples = await filterAsync(triples, (triple) => checkIfExist(TARGET_GRAPH, triple), true)

    const ntTriples = triplesToNT(triples);
    await writeTriplesToGraph(TARGET_GRAPH, ntTriples, BATCH_SIZE);
    const graphContainer = { id: uuid() };
    graphContainer.uri = `http://redpencil.data.gift/id/dataContainers/${graphContainer.id}`;
    await appendTaskResultGraph(task, graphContainer, TARGET_GRAPH);

    await updateTaskStatus(task, STATUS_SUCCESS);
  }
  catch (e) {
    console.error(e);
    await appendTaskError(task, e.message);
    await updateTaskStatus(task, STATUS_FAILED);
  }
}
async function checkIfExist(graph, triple) {
  const subject = processPart(triple.subject);
  const predicate = processPart(triple.predicate);
  const object = processPart(triple.object);
  const q = `
    ask {
      graph ${sparqlEscapeUri(graph)} {
        ${subject} ${predicate} ${object}.
      }
    }
  `;
  const resp = await query(q);
  return resp?.boolean;
};
