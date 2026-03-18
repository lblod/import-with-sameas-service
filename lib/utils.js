import * as N3 from 'n3';
import * as cts from '../constants';

const COMMON_PREFIXES = [
  ['s0:', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'],
  ['s1:', 'http://www.w3.org/ns/org#'],
  ['s2:', 'http://www.w3.org/2000/01/rdf-schema#'],
  ['s3:', 'http://www.w3.org/2001/XMLSchema#'],
  ['s4:', 'http://xmlns.com/foaf/0.1/'],
  ['s5:', 'http://purl.org/dc/elements/1.1/'],
  ['s6:', 'http://purl.org/dc/terms/'],
  ['s7:', 'http://www.w3.org/2004/02/skos/core#'],
  ['s8:', 'http://www.w3.org/ns/prov#'],
  ['s9:', 'http://schema.org/'],
  ['q0:', 'http://www.w3.org/ns/dcat#'],
  ['q1:', 'http://www.w3.org/ns/adms#'],
  ['q2:', 'http://mu.semte.ch/vocabularies/core/'],
  ['q3:', 'http://data.vlaanderen.be/ns/besluit#'],
  ['q4:', 'http://data.vlaanderen.be/ns/mandaat#'],
  ['q5:', 'http://data.europa.eu/eli/ontology#'],
  ['q6:', 'http://publications.europa.eu/ontology/euvoc#'],
  ['q7:', 'https://data.vlaanderen.be/ns/mobiliteit#'],
  ['q8:', 'http://w3id.org/ldes#'],
];

/**
 * this replace absolute uris with prefix. It makes the query more compact
 * and should in theory speed up insertions as our requests are lighter
 * @param {*} statements array of {subject, predicate, object}
 * @returns
 */
export function prepareStatements(statements) {
  let newStmts = statements.map((stmt) => {
    if (
      stmt.predicate.replace(/^<|>$/g, '') ===
      'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
    ) {
      stmt.predicate = 'a';
    }
    return stmt;
  });
  const usablePrefixes = COMMON_PREFIXES.filter(([_, uri]) =>
    statements.some(
      ({ subject, predicate, object }) =>
        subject.includes(uri) ||
        predicate.includes(uri) ||
        (!object.startsWith('"') && object.includes(uri))
    )
  );
  usablePrefixes.forEach(([prefix, uri]) => {
    const regex = new RegExp(
      `<${uri}([^>#][^>]*)>|${uri}([^\\s>#][^\\s>]*)`,
      'g'
    );
    newStmts = newStmts.map(({ subject, predicate, object }) => {
      return {
        subject: subject.replace(regex, `${prefix}$1$2`),
        predicate: predicate.replace(regex, `${prefix}$1$2`),
        object: object.startsWith('"')
          ? object
          : object.replace(regex, `${prefix}$1$2`),
      };
    });
  });
  return {
    usedPrefixes: usablePrefixes
      .map(([prefix, uri]) => `PREFIX ${prefix} <${uri}>`)
      .join('\n'),
    newStmts,
  };
}

/**
 * Returns a function that transforms a list of triples to an INSERT DATA query
 * It also compacts the triples by calling the prepareStatements function
 * @param {string} graph the graph for the query
 * @returns 
 */
export const insertQueryTemplate = (graph) => (statements) => {
  let { usedPrefixes, newStmts } = prepareStatements(statements);
  return `
  ${usedPrefixes}
  INSERT DATA {
    GRAPH <${graph}> {
      ${statementsToNTriples(newStmts)}
    }
  }`;
};

function statementsToNTriples(statements) {
  return [...new Set(statements)]
    .map(
      ({ subject, predicate, object }) => `${subject} ${predicate} ${object}.`
    )
    .join(''); // probably no need to join with a \n as triples are delimitted by a dot
}

/**
 * Convert a store of quads into a string that can be inserted in a SPARQL
 * query.
 *
 * @public
 * @function
 * @param {N3.Store|Iterable} store - A collection (N3.Store or Array, or
 * something else that can iterated on) that contains RDF.js quads.
 * @returns {String} The triples in SPARQL body form. (No graphs and no
 * prefixes.)
 */
export async function storeToSparql(store) {
  return await storeToString(store, true);
}

/**
 * Convert a store of quads into a string that that is better suited to writing
 * to a file.
 *
 * @public
 * @function
 * @param {N3.Store|Iterable} store - A collection (N3.Store or Array, or
 * something else that can iterated on) that contains RDF.js quads.
 * @returns {String} The triples in Turtle form. (No graphs, but it uses
 * prefixes at the top to make the file more human readable.)
 */
export async function storeToTtl(store) {
  return await storeToString(store, true);
}

/**
 * Converts a store of quads into a string, with or without prefixes.
 *
 * @function
 * @param {N3.Store|Iterable} store - A collection (N3.Store or Array, or
 * something else that can iterated on) that contains RDF.js quads.
 * @param {Boolean} [forSparql = true] - If true, the result will be formatted
 * in N-Triples syntax (without prefixes) so it can be used inside a SPARQL
 * query. If false, the result will contain prefix definitions at the top of
 * the file that will be used throughout the file and will be formatted in
 * more dense Turtle syntax.
 *
 * **NOTE:** realistically, the difference between N-Triples and Turtle syntax
 * is not big. Some datatypes are less explicit in Turtle, such as booleans,
 * which causes mu-authorization to fail.
 */
async function storeToString(store, forSparql = true) {
  // Use the WRITER_PREFIXES, there is much more in them than what is needed
  // for the SPARQL queries.
  const options = forSparql
    ? { format: 'N-Triples' }
    : { format: 'Turtle', prefixes: cts.WRITER_PREFIXES };
  const writer = new N3.Writer(options);
  store.forEach((q) => writer.addQuad(q.subject, q.predicate, q.object));

  return new Promise((resolve, reject) => {
    writer.end((err, results) => {
      if (err) reject(err);
      else resolve(results);
    });
  });
}

/**
 * Simple. Sleep for the given amount of milliseconds.
 *
 * @public
 * @async
 * @function
 * @param {Integer} ms - The amount of milliseconds to sleep for.
 * @returns {undefined} Nothing. (Note that this is an async function, so you
 * should `await` it or use the returned promise to wait for its resolution.)
 */
export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
