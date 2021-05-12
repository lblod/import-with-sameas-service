import { sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { PREFIXES } from '../constants';
import { parseResult } from './utils';

export async function getTriplesInGraph(task){
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

  const defaultLimitSize = 200;
  const countQuery = `SELECT (count(?s) as ?count) WHERE { GRAPH  ${sparqlEscapeUri(graphData.graph)} { ?s ?p ?o } }`;
  const countResult = await query(countQuery);


  let countTriples = 0;
  if (countResult.results && countResult.results.bindings.length){
    let {count} = countResult.results.bindings[0];
    countTriples = count.value;
  }
  const pagesCount = countTriples > defaultLimitSize ? Math.trunc(countTriples / defaultLimitSize) : defaultLimitSize;
  let triples = [];
  for (let page = 0; page <= pagesCount; page++) {
    const offset = page * defaultLimitSize;
    const queryTriples = `
        SELECT ?subject ?predicate ?object WHERE
          {
            {
              SELECT DISTINCT ?subject ?predicate ?object WHERE {
                GRAPH ${sparqlEscapeUri(graphData.graph)} {
                  ?subject ?predicate ?object
                }
              } ORDER BY ?subject ?predicate ?object
            }
          } limit ${defaultLimitSize} offset ${offset}
        `;

    const result = await query(queryTriples);
    if (!(result.results && result.results.bindings.length)) {
      break;
    }
    else {
      triples = triples.concat(result.results.bindings);
    }
  }
  return triples;

}

/**
 * Writes the triples to the database and to a ttl file
 *
 * @param triples the triples to be written
 */
export async function writeTriplesToGraph(graph, triples) {
  const tripleStrings = [...triples];
  while(tripleStrings.length) {
    const batch = tripleStrings.splice(0, 100);
    await update(`
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(graph)} {
            ${batch.join('\n')}
        }
      }
    `);
  }
}

export async function appendTaskResultGraph(task, container, graphUri){
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
