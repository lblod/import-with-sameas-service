import * as N3 from 'n3';
import * as uti from './utils';
import * as cts from '../constants';
import * as mas from '@lblod/mu-auth-sudo';
import * as rst from 'rdf-string-ttl';
import * as sjp from 'sparqljson-parse';
import * as stream from 'stream';
import fs from 'fs';

/**
 * Gets the triples related to a task. It first tries to get the triples from
 * the file related to the inputContainer, but if that fails, it tries to get
 * the files from the graph related to the inputContainer.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} task -
 * @returns {N3.Store}
 */
export async function getTriples(task) {
  try {
    return getTriplesInFile(task);
  } catch (e) {
    console.error('An error occurred, trying from graph', e);
    return getTriplesInGraph(task);
  }
}

/**
 * Fetches triples from a graph that is related to the inputContainer of the
 * given task. It first counts the number of triples before sending paginated
 * requests.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} task - Task for which to get the triples in the
 * inputContainer's graph.
 * @param {Integer} [defaultLimitSize = 200] - Page size in number of triples
 * per request.
 * @returns {N3.Store} A store with all the data than could be retreived.
 */
export async function getTriplesInGraph(task, defaultLimitSize = 200) {
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

/**
 * Given a task, the file from the graph in the inputContainer is selected and
 * its contents are loaded in a store.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} task - Represents the task you want the input data for in
 * the form of a file from the inputContainer.
 * @returns {N3.Store} A store with all the data in it that can be parsed from
 * the input file.
 */
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
  const path = file.value.replace('share://', '/share/');
  const store = new N3.Store();
  const consumer = new stream.Writable({
    write(quad, encoding, done) {
      store.addQuad(quad);
      done();
    },
    objectMode: true,
  });
  const streamParser = new N3.StreamParser();
  const rdfStream = fs.createReadStream(path);
  rdfStream.pipe(streamParser);
  streamParser.pipe(consumer);
  return new Promise((resolve, reject) => {
    consumer.on('close', () => {
      resolve(store);
    });
    consumer.on('error', reject);
  });
}

/**
 * Writes a store with triples to the triplestore in the specified graph. The
 * triples are split in smaller queries according to a batch size. If an insert
 * fails, the batch size is reduced and tried again.
 *
 * TODO: maybe a rewrite without recursion? Recursion has limits in JavaScript,
 * but even large numbers of triples won't create too much recursion (log₂(N))
 * so it's not a big problem here.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} graph - Graph in which to insert the data.
 * @param {N3.Store|Iterable} store - An N3.Store or other Iterable (like an
 * Array) that contains RDF.js quads.
 * @param {Integer} [batchSize = 100] - This is used to split the collection of
 * triples in a series of queries.
 * @returns {undefined} Nothing.
 */
export async function writeTriplesToGraph(graph, store, batchSize = 100) {
  const triples = [...store];
  const pages = Math.ceil(store.size / batchSize);
  for (let page = 0; page < pages; page++) {
    const batch = triples.slice(page * batchSize, (page + 1) * batchSize);
    const triplesString = await uti.storeToSparql(batch);
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
        // Throw an error on any failed insertion after retrying as much as
        // needed.
        throw e;
      }
    }
  }
}

/**
 * On the given task, add in the resultsContainer a pointer to a graph. Write
 * this data to the given graph.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} task - Task that you want to set the resultsContainer of.
 * @param {Object(id: Literal, node: NamedNode)} container - Object containing
 * the UUID and the URI of the resultsContainer you want to add the graph to.
 * @param {NamedNode} graph - Graph in which to write this data.
 * @returns {undefined} Nothing.
 */
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
    }`);
}
