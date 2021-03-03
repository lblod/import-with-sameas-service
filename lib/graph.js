import { sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { PREFIXES } from '../constants';
import { parseResult } from './utils';

export async function getTriplesInGraph(task) {
  const queryGraph = `
     ${PREFIXES}
     SELECT DISTINCT ?graph WHERE {
        GRAPH ?g {
          BIND(${sparqlEscapeUri(task.task)} as ?task).
          ?task task:inputContainer ?container.
          ?container task:hasGraph ?graph.
        }
     }
  `;
  const graphData = parseResult(await query(queryGraph))[0];

  const queryTriples = `
    SELECT DISTINCT ?subject ?predicate ?object WHERE {
      GRAPH ${sparqlEscapeUri(graphData.graph)} {
        ?subject ?predicate ?object
      }
    }
  `;

  const result = await query(queryTriples);
  if (!(result.results && result.results.bindings.length)) {
    return [];
  } else {
    return result.results.bindings;
  }
}

/**
 * Writes the triples to the database and to a ttl file
 *
 * @param triples the triples to be written
 */
export async function writeTriplesToGraph(graph, triples, batchSize = 100) {
  const tripleStrings = [...triples];
  while (tripleStrings.length) {
    const batch = tripleStrings.splice(0, batchSize);
    const queryStr = `
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(graph)} {
          ${batch.join('\n')}
        }
      }
    `;
    try {
      await update(queryStr);
    } catch (e) {
      if (batchSize !== 1) {
        /**
         * NOTE:  If the query failed, their is a high probability that this is due to a large triple.
         *        Therefore we keep trying by batching the query into small chunks until we get stuck on an indigestible triple.
         */
        await writeTriplesToGraph(graph, batch, Math.ceil(batchSize / 2));
      } else {
        /**
         * NOTE: log the failing query for debugging.
         */
        console.warn('INSERT of a triple failed:');
        console.warn(queryStr);
        throw e;
      }
    }
  }
}

export async function appendTaskResultGraph(task, container, graphUri) {
  const queryStr = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(task.graph)} {
        ${sparqlEscapeUri(container.uri)} a nfo:DataContainer.
        ${sparqlEscapeUri(container.uri)} mu:uuid ${sparqlEscapeString(container.id)}.
        ${sparqlEscapeUri(container.uri)} task:hasGraph ${sparqlEscapeUri(graphUri)}.
        ${sparqlEscapeUri(task.task)} task:resultsContainer ${sparqlEscapeUri(container.uri)}.
      }
    }
  `;

  await update(queryStr);

}
