import { uuid } from 'mu';
import { STATUS_BUSY,
         STATUS_FAILED,
         STATUS_SUCCESS,
         TARGET_GRAPH,
       } from '../constants';

import {  updateTaskStatus, appendTaskError } from './task';
import { getGraphsFromDataContainer, copyTriplesFromGraphToGraph, appendTaskResultGraph } from './graph';

export async function run( task ){
  try {
    await updateTaskStatus(task, STATUS_BUSY);

    const sourceGraph = (await getGraphsFromDataContainer(task.inputContainers[0]))[0].graph;
    await copyTriplesFromGraphToGraph(sourceGraph, TARGET_GRAPH);
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
