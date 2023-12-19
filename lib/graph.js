import * as N3 from 'n3';
import * as uti from './utils';
import * as cts from '../constants';
import * as mas from '@lblod/mu-auth-sudo';
import * as rst from 'rdf-string-ttl';
import * as sjp from 'sparqljson-parse';
import * as stream from 'stream';
import fs from 'fs';
import readline from 'readline';
import { Readable } from 'stream';
import { HIGH_LOAD_DATABASE_ENDPOINT } from '../constants';
const connectionOptions = {
  sparqlEndpoint: HIGH_LOAD_DATABASE_ENDPOINT,
  mayRetry: true,
};
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
  const graphResponse = await mas.updateSudo(
    `
     ${cts.SPARQL_PREFIXES}
     SELECT DISTINCT ?graph WHERE {
        GRAPH ?g {
          BIND(${rst.termToString(task.task)} as ?task).
          ?task task:inputContainer ?container.
          ?container task:hasGraph ?graph.
        }
     }
     LIMIT 1
  `,
    {},
    connectionOptions
  );
  const parser = new sjp.SparqlJsonParser();
  const graphData = parser.parseJsonResults(graphResponse)[0];

  const countResponse = await mas.querySudo(
    `
    SELECT (count(?s) as ?count) WHERE {
      GRAPH ${rst.termToString(graphData.graph)} {
        ?s ?p ?o .
      }
    }
  `,
    {},
    connectionOptions
  );

  const countTriples = parser.parseJsonResults(countResponse)[0];
  const pagesCount =
    countTriples > defaultLimitSize
      ? Math.ceil(countTriples / defaultLimitSize)
      : defaultLimitSize;
  const store = new N3.Store();
  for (let page = 0; page < pagesCount; page++) {
    const offset = page * defaultLimitSize;

    const queryResponse = await mas.querySudo(
      `
      SELECT DISTINCT ?s ?p ?o WHERE {
        GRAPH ${rst.termToString(graphData.graph)} {
          ?s ?p ?o .
        }
      }
      LIMIT ${defaultLimitSize}
      OFFSET ${offset}
    `,
      {},
      connectionOptions
    );
    const parsedResults = parser.parseJsonResults(queryResponse);
    parsedResults.forEach((result) => {
      store.addQuad(result.s, result.p, result.o, graphData.graph);
    });
  }
  return store;
}
class ArrayReadableStream extends Readable {
  constructor(array, options = {}) {
    super(options);
    this.array = array;
    this.index = 0;
  }

  _read() {
    if (this.index >= this.array.length) {
      this.push(null);
      return;
    }

    const chunk = this.array[this.index];
    this.push(chunk);
    this.index++;
  }
}

function streamToN3Store(arrayStream) {
  const store = new N3.Store();
  const consumer = new stream.Writable({
    write(quad, _encoding, done) {
      store.addQuad(quad);
      done();
    },
    objectMode: true,
  });
  const streamParser = new N3.StreamParser();
  arrayStream.pipe(streamParser);
  streamParser.pipe(consumer);
  return new Promise((resolve, reject) => {
    consumer.on('close', () => {
      resolve(store);
    });
    consumer.on('error', reject);
  });
}

export async function fetchFileInputContainerAndApplyBatch(
  queryInputContainer,
  callback
) {
  const fileResponse = await mas.querySudo(
    queryInputContainer,
    {},
    connectionOptions
  );
  const parser = new sjp.SparqlJsonParser();

  const files = parser.parseJsonResults(fileResponse);

  let buf = [];
  const sizeBuf = 2000;
  for (const f of files) {
    const path = f.path.value.replace('share://', '/share/');
    if (path.endsWith('.gz')) {
      throw Error(
        'Input file cannot be a gzipped file. That probably means there was a duplicate task.'
      );
    }
    const fileStream = fs.createReadStream(path);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity,
    });
    for await (const line of rl) {
      buf.push(line); // assuming N-TRIPLES notation
      if (buf.length == sizeBuf) {
        const arrayStream = new ArrayReadableStream(buf);
        const store = await streamToN3Store(arrayStream);
        await callback(store, f.derivedFrom);
        buf = [];
      }
    }
    if (buf.length) {
      const arrayStream = new ArrayReadableStream(buf);
      const store = await streamToN3Store(arrayStream);
      await callback(store, f.derivedFrom);
      buf = [];
    }
  }
}
export async function getTriplesInFileAndApplyByBatch(
  task,
  callback = async () => {}
) {
  const countFn = async () => {
    const result = await mas.querySudo(`
  ${cts.SPARQL_PREFIXES}
  SELECT (count(distinct ?path) as ?count)   WHERE {
      GRAPH ?g {
        BIND(${rst.termToString(task.task)} as ?task).
        ?task task:inputContainer ?container.
        ?container task:hasGraph ?graph.
        ?graph task:hasFile ?file.
        ?file prov:wasDerivedFrom ?derivedFrom.
        ?path nie:dataSource ?file.
      }
    }


  `);
    if (result.results.bindings.length) {
      return result.results.bindings[0].count.value;
    } else {
      return 0;
    }
  };
  const defaultLimitSize = 1000;
  const queryFn = async (limitSize, offset) => {
    await fetchFileInputContainerAndApplyBatch(
      `
    ${cts.SPARQL_PREFIXES}
    SELECT ?path ?derivedFrom  WHERE {
      SELECT DISTINCT ?path ?derivedFrom  WHERE {
        GRAPH ?g {
          BIND(${rst.termToString(task.task)} as ?task).
          ?task task:inputContainer ?container.
          ?container task:hasGraph ?graph.
          ?graph task:hasFile ?file.
          ?file prov:wasDerivedFrom ?derivedFrom.
          ?path nie:dataSource ?file.
        }
      } order by ?path
    } limit ${limitSize} offset ${offset}
    
  `,
      callback
    );
  };
  const count = await countFn(task);
  const pagesCount =
    count > defaultLimitSize ? Math.ceil(count / defaultLimitSize) : 1;

  for (let page = 0; page <= pagesCount; page++) {
    await queryFn(defaultLimitSize, page * defaultLimitSize);
  }
}
export async function getDeletedTriplesInFileAndApplyByBatch(
  task,
  callback = async () => {}
) {
  //This is still based on the filename "to-remove-triples.ttl"! This should
  //change in the future, but there is no other way to correctly address that
  //file only yet, besides via its filename.

  const countFn = async () => {
    const result = await mas.querySudo(`
  ${cts.SPARQL_PREFIXES}
  SELECT (count(distinct ?path) as ?count)   WHERE {
      GRAPH ?g {
      ${rst.termToString(task.task)}
        a task:Task ;
        task:inputContainer ?inputContainer .

      ?inputContainer
        a nfo:DataContainer ;
        task:hasFile ?logicalFile .

      ?logicalFile
        a nfo:FileDataObject ;
        prov:wasDerivedFrom ?derivedFrom;
        nfo:fileName "to-remove-triples.ttl" .

      ?path
        a nfo:FileDataObject ;
        nie:dataSource ?logicalFile .
      }
    }
  `);
    if (result.results.bindings.length) {
      return result.results.bindings[0].count.value;
    } else {
      return 0;
    }
  };

  const defaultLimitSize = 1000;
  const queryFn = async (limitSize, offset) => {
    const queryInputContainer = `
    ${cts.SPARQL_PREFIXES}
    SELECT ?path ?derivedFrom WHERE {
      SELECT DISTINCT ?path ?derivedFrom WHERE {
        GRAPH ?g {
          ${rst.termToString(task.task)}
            a task:Task ;
            task:inputContainer ?inputContainer .

          ?inputContainer
            a nfo:DataContainer ;
            task:hasFile ?logicalFile .

          ?logicalFile
            a nfo:FileDataObject ;
            prov:wasDerivedFrom ?derivedFrom;
            nfo:fileName "to-remove-triples.ttl" .

          ?path
            a nfo:FileDataObject ;
            nie:dataSource ?logicalFile .
        }
      } order by ?path
    } limit ${limitSize} offset ${offset}

   
  `;
    await fetchFileInputContainerAndApplyBatch(queryInputContainer, callback);
  };
  const count = await countFn(task);
  const pagesCount =
    count > defaultLimitSize ? Math.ceil(count / defaultLimitSize) : 1;

  for (let page = 0; page <= pagesCount; page++) {
    await queryFn(defaultLimitSize, page * defaultLimitSize);
  }
}

/**
 * Given a task, the file from the graph in the inputContainer is selected and
 * its contents are loaded in a store.
 *
 * @public
 * @deprecated use getTriplesInFileAndApplyByBatch instead
 * @async
 * @function
 * @param {NamedNode} task - Represents the task you want the input data for in
 * the form of a file from the inputContainer.
 * @returns {N3.Store} A store with all the data in it that can be parsed from
 * the input file.
 */
export async function getTriplesInFile(task) {
  const fileResponse = await mas.querySudo(
    `
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
  `,
    {},
    connectionOptions
  );
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
 * Queries the triple store for the information about the file containing the
 * deleted triples from the diff service.
 *
 * @public
 * @async
 * @deprecated use getDeletedTriplesInFileAndApplyByBatch instead
 * @function
 * @param {NamedNode} task - RDF term representing the task.
 * @returns {NamedNode} The RDF term prepresenting the physical file containing
 * the removed triples.
 */
export async function getDeletedTriplesFromInputContainer(task) {
  //This is still based on the filename "to-remove-triples.ttl"! This should
  //change in the future, but there is no other way to correctly address that
  //file only yet, besides via its filename.
  const response = await mas.querySudo(
    `
    ${cts.SPARQL_PREFIXES}
    SELECT DISTINCT ?physicalFile WHERE {
      ${rst.termToString(task.task)}
        a task:Task ;
        task:inputContainer ?inputContainer .

      ?inputContainer
        a nfo:DataContainer ;
        task:hasFile ?logicalFile .

      ?logicalFile
        a nfo:FileDataObject ;
        nfo:fileName "to-remove-triples.ttl" .

      ?physicalFile
        a nfo:FileDataObject ;
        nie:dataSource ?logicalFile .
    }
    LIMIT 1
  `,
    {},
    connectionOptions
  );
  const sparqlJsonParser = new sjp.SparqlJsonParser();
  const parsedResults = sparqlJsonParser.parseJsonResults(response);
  const file = parsedResults[0]?.physicalFile;
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
 * @param {Integer} [batchSize] - This is used to split the collection of
 * triples in a series of queries.
 * @returns {undefined} Nothing.
 */
export async function writeTriplesToGraph(
  graph,
  store,
  batchSize = cts.BATCH_SIZE
) {
  const triples = [...store];
  const pages = Math.ceil(triples.length / batchSize);
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
      await mas.updateSudo(queryStr, {}, connectionOptions);
    } catch (e) {
      if (batchSize > 1) {
        console.warn(
          `INSERT batch of triples failed. Retrying with smaller batch size ${Math.ceil(
            batchSize / 2
          )}`
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

/*
 * Deletes triples in the given store from the given graph using batching.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} graph - Graph in which to insert the data.
 * @param {N3.Store|Iterable} store - An N3.Store or other Iterable (like an
 * Array) that contains RDF.js quads.
 * @param {Integer} [batchSize] - This is used to split the collection of
 * triples in a series of queries.
 * @param {Boolean} [skipFailures] - If failures need to be skipped. On true,
 * the process is continued for as many triples as possible, even on failing
 * requests. On false, failing to insert a triple means failure of the whole
 * task. On failure, batches are decreased in size until single triples if
 * necessary.
 * @returns {undefined} Nothing
 */
export async function deleteTriplesFromGraph(
  graph,
  store,
  batchSize = cts.BATCH_SIZE,
  skipFailures = false
) {
  const triples = [...store];
  const originalBatchSize = batchSize;
  let start = 0;
  while (start < triples.length) {
    try {
      const batch = triples.slice(start, start + batchSize);
      await deleteTriplesFromGraphWithoutBatching(graph, batch);
      start += batchSize;
      batchSize = originalBatchSize;
    } catch (err) {
      if (batchSize > 1) {
        batchSize = Math.ceil(batchSize / 2);
      } else {
        if (skipFailures) {
          // When we are allowed to skip a failing triple:
          start++;
        } else {
          // When every failure needs to crash this task:
          const tripleString = [
            rst.termToString(triples[start].subject),
            rst.termToString(triples[start].predicate),
            rst.termToString(triples[start].object),
          ].join(' ');
          throw new Error(
            `The following triple could not be removed from the triplestore:\n\t${tripleString}\nThis might be because of a network issue, a syntax issue or because the triple is too long.`
          );
        }
      }
    }
  }
}

/**
 * Deletes an N3 Store from the triplestore.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} graph - Target graph where the triples should be deleted
 * from.
 * @param {N3.Store|Iterable} store - Store or other iterable collection
 * containing the data that needs to be removed.
 * @returns {undefined} Nothing. (Might return the response object of a REST
 * call to the triplestore to remove the data.)
 */
async function deleteTriplesFromGraphWithoutBatching(graph, store) {
  if (store.size && store.size <= 0) return;
  if (store.length && store.length <= 0) return;

  const toRemoveString = await uti.storeToSparql(store);
  return mas.updateSudo(
    `
    DELETE DATA {
      GRAPH ${rst.termToString(graph)} {
        ${toRemoveString}
      }
    }
  `,
    {},
    connectionOptions
  );
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
