import { sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { PREFIXES } from '../constants';
import { sleep, parseResult } from './utils';
import fs from 'fs';
import { Parser as ParserN3 } from 'n3';

const MAX_RETRY = process.env.MAX_RETRY || 5;
const BATCH_SIZE = process.env.BATCH_SIZE || 100;
const SLEEP_TIME = process.env.SLEEP_TIME || 1000;

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

  const defaultLimitSize = 200;
  const countQuery = `SELECT (count(?s) as ?count) WHERE { GRAPH  ${sparqlEscapeUri(graphData.graph)} { ?s ?p ?o } }`;
  const countResult = await query(countQuery);


  let countTriples = 0;
  if (countResult.results && countResult.results.bindings.length) {
    let { count } = countResult.results.bindings[0];
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

export async function getTriples(task) {
  try {
    return await getTriplesInFile(task);
  } catch (e) {
    console.error("An error occurred, trying from graph", e);
    return await getTriplesInGraph(task);
  }
}

export async function getTriplesInFile(task) {
  const queryGraph = `
     ${PREFIXES}
     SELECT DISTINCT ?path WHERE {
        GRAPH ?g {
          BIND(${sparqlEscapeUri(task.task)} as ?task).
          ?task task:inputContainer ?container.
          ?container task:hasGraph ?graph.
          ?graph <http://redpencil.data.gift/vocabularies/tasks/hasFile> ?file.
          ?path <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#dataSource> ?file.
        }
     }
  `;
  const res = await query(queryGraph);
  const data = res.results.bindings[0];
  const path = data.path.value.replace('share://', '');
  return await parseFile(path);
}

export async function parseFile(path) {
  return new Promise((resolve, reject) => {
    const parser = new ParserN3();
    const triples = [];
    const rdfStream = fs.createReadStream(`/share/${path}`);
    parser.parse(rdfStream, (error, quad) => {
      if (error) {
        console.warn(error);
        reject(error);
      }
      else if (quad) {
        const subject = transformQuad(quad.subject);
        const predicate = transformQuad(quad.predicate);
        const object = transformQuad(quad.object);
        triples.push({
          subject: subject,
          predicate: predicate,
          object: object
        })
      }
      else {
        resolve(triples);
      }
    });
  });
}

function escapeNewLines(string) {
  return `${string.replace(/\n/g, function () { return ''; }).replace(/\r/g, function () { return ''; })}`;
}

function transformQuad(part) {
  const partType = part.termType;
  let value = part.value;
  const language = part.language;
  const dataType = part.datatype;
  const res = {};
  if (partType === 'NamedNode' || partType === 'BlankNode') {
    res.type = 'uri';
  } else if (partType === 'Literal') {
    if (dataType) {
      res.type = 'typed-literal';
      res.datatype = dataType.value;
    } else {
      res.type = 'literal';
    }
    if (language) {
      res.language = language;
    }
    value = escapeNewLines(value);
  }
  res.value = value;
  return res;
}


/**
 * Writes the triples to the database and to a ttl file
 *
 * @param triples the triples to be written
 */
export async function writeTriplesToGraph(graph, triples) {
  const tripleStrings = [...triples];

  while (tripleStrings.length) {
    const batch = tripleStrings.splice(0, BATCH_SIZE);
    await insertOrRetry(graph, batch);
  }
}

async function insertOrRetry(graph, batch) {
  let retryCount = 0;
  let error = new Error("Unknown error");
  let success = false;
  do {
    try {
      await update(`
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(graph)} {
            ${batch.join('\n')}
        }
      }
    `);
      success = true;
      break;
    }
    catch (e) {
      console.error(`an error occurred, retry count ${retryCount}, max retry ${MAX_RETRY}`);
      error = e;
      retryCount += 1;
      await sleep(SLEEP_TIME);
    }
  } while (!success && retryCount < MAX_RETRY);
  if (!success) {
    console.log("Reaching max retries. Check the logs for further details.");
    throw error;
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
