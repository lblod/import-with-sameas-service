import * as N3 from 'n3';
import * as uti from './utils';
import * as cts from '../constants';
import * as mas from '@lblod/mu-auth-sudo';
import * as rst from 'rdf-string-ttl';
import * as sjp from 'sparqljson-parse';
import fs from 'fs';

export async function getTriplesInGraph(task) {
  const graphResponse = await mas.updateSudo(`
     ${cts.SPARQL_PREFIXES}
     SELECT DISTINCT ?graph WHERE {
        GRAPH ?g {
          BIND(${rst.termToString(task.task)} as ?task).
          ?task task:inputContainer ?container.
          ?container task:hasGraph ?graph.
        }
     }
     LIMIT 1
  `);
  const parser = new sjp.SparqlJsonParser();
  const graphData = parser.parseJsonResults(graphResponse)[0];

  const countResponse = await mas.querySudo(`
    SELECT (count(?s) as ?count) WHERE {
      GRAPH ${rst.termToString(graphData.graph)} {
        ?s ?p ?o .
      }
    }
  `);

  const defaultLimitSize = 200;
  const countTriples = parser.parseJsonResults(countResponse)[0];
  const pagesCount =
    countTriples > defaultLimitSize
      ? Math.ceil(countTriples / defaultLimitSize)
      : defaultLimitSize;
  const store = new N3.Store();
  for (let page = 0; page < pagesCount; page++) {
    const offset = page * defaultLimitSize;
    const queryResponse = await mas.querySudo(`
      SELECT DISTINCT ?s ?p ?o WHERE {
        GRAPH ${rst.termToString(graphData.graph)} {
          ?s ?p ?o .
        }
      }
      LIMIT ${defaultLimitSize}
      OFFSET ${offset}
    `);
    const parsedResults = parser.parseJsonResults(queryResponse);
    parsedResults.forEach((result) => {
      store.addQuad(result.s, result.p, result.o, graphData.graph);
    });
  }
  return store;
}

export async function getTriples(task) {
  try {
    return getTriplesInFile(task);
  } catch (e) {
    console.error('An error occurred, trying from graph', e);
    return getTriplesInGraph(task);
  }
}

export async function getTriplesInFile(task) {
  const fileResponse = await mas.querySudo(`
    ${cts.SPARQL_PREFIXES}
    SELECT DISTINCT ?path WHERE {
      GRAPH ?g {
        BIND(${rst.termToString(task.task)} as ?task).
        ?task task:inputContainer ?container.
        ?container task:hasGraph ?graph.
        ?graph task:hasFile ?file.
        ?path nie:dataSource ?file.
      }
    }
    LIMIT 1
  `);
  const parser = new sjp.SparqlJsonParser();
  const file = parser.parseJsonResults(fileResponse)[0].path;
  const path = file.value.replace('share://', '');
  const store = new N3.Store();
  return new Promise((resolve, reject) => {
    const parser = new N3.Parser();
    const rdfStream = fs.createReadStream(`/share/${path}`);
    parser.parse(rdfStream, (error, quad) => {
      if (error) {
        console.warn(error);
        reject(error);
      } else if (quad) {
        store.addQuad(quad);
      } else {
        resolve(store);
      }
    });
  });
}

//TODO: maybe a rewrite without recursion? Recursion has limits in JavaScript,
//but even large numbers of triples won't create too much recursion (log₂(N)).
export async function writeTriplesToGraph(graph, store, batchSize = 100) {
  const triples = [...store];
  const pages = Math.ceil(store.size / batchSize);
  for (let page = 0; page < pages; page++) {
    const batch = triples.slice(page * batchSize, (page + 1) * batchSize);
    const triplesString = await uti.storeToTtl(batch);
    const queryStr = `
      INSERT DATA {
        GRAPH ${rst.termToString(graph)} {
          ${triplesString}
        }
      }`;
    try {
      await mas.updateSudo(queryStr);
    } catch (e) {
      if (batchSize > 1) {
        console.warn(
          `INSERT batch of triples failed. Retrying with smaller batch size ${
            batchSize / 2
          }`
        );
        await uti.sleep(cts.SLEEP_TIME);
        await writeTriplesToGraph(graph, batch, Math.ceil(batchSize / 2));
      } else {
        console.error('INSERT of a triple failed:');
        console.error(queryStr);
        //No throw because this would cause loops in the previous recursive
        //context to also interrupt and you would miss some data. Just log
        //errors, and try to insert as many triples possible by ignoring the
        //errors for now.
        //throw e;
      }
    }
  }
}

export async function appendTaskResultGraph(task, container, graph) {
  return mas.updateSudo(`
    ${cts.SPARQL_PREFIXES}
    INSERT DATA {
      GRAPH ${rst.termToString(task.graph)} {
        ${rst.termToString(container.node)}
          a nfo:DataContainer ;
          mu:uuid ${rst.termToString(container.id)} ;
          task:hasGraph ${rst.termToString(graph)} .
        ${rst.termToString(task.task)}
          task:resultsContainer ${rst.termToString(container.node)} .
      }
    }
  `);
}
