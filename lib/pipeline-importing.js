import { uuid } from 'mu';
import { STATUS_BUSY,
         STATUS_FAILED,
         STATUS_SUCCESS,
         TARGET_GRAPH,
       } from '../constants';

import {  updateTaskStatus, appendTaskError } from './task';
import { writeTriplesToGraph,
         appendTaskResultGraph,
         getTriplesInGraph
       } from './graph';
import { triplesToNT } from './utils';

export async function run( task ){
  try {
    await updateTaskStatus(task, STATUS_BUSY);

    //Some detour in moving data from one graph to another is needed, cause:
    // 1. mu-auth doesn't support COPY statements
    // 2. a simple insert/where is generates too big statements for the database. Batching is not legal sparql
    const triples = await getTriplesInGraph(task);
    const ntTriples = triplesToNT(triples);
    await writeTriplesToGraph(TARGET_GRAPH, ntTriples);
    const graphContainer = { id: uuid() };
    graphContainer.uri = `http://redpencil.data.gift/id/dataContainers/${graphContainer.id}`;
    await appendTaskResultGraph(task, graphContainer, TARGET_GRAPH);

    await updateTaskStatus(task, STATUS_SUCCESS);
  }
  catch(e){
    console.error(e);
    await appendTaskError(task, e.message);
    await updateTaskStatus(task, STATUS_FAILED);
  }
}
