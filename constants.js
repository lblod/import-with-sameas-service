import envvar from 'env-var';
import * as fs from 'fs';
import * as N3 from 'n3';
const { namedNode } = N3.DataFactory;

/**
 * Prefixes used in SPARQL queries (mostly).
 *
 * @private
 * @constant
 */
const PREFIXES = {
  rdf: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
  xsd: 'http://www.w3.org/2001/XMLSchema#',
  owl: 'http://www.w3.org/2002/07/owl#',
  mu: 'http://mu.semte.ch/vocabularies/core/',
  task: 'http://redpencil.data.gift/vocabularies/tasks/',
  prov: 'http://www.w3.org/ns/prov#',
  oslc: 'http://open-services.net/ns/core#',
  dct: 'http://purl.org/dc/terms/',
  adms: 'http://www.w3.org/ns/adms#',
  nie: 'http://www.semanticdesktop.org/ontologies/2007/01/19/nie#',
  ext: 'http://mu.semte.ch/vocabularies/ext/',
  cogs: 'http://vocab.deri.ie/cogs#',
  nfo: 'http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#',
  dbpedia: 'http://dbpedia.org/ontology/',
  jobstat: 'http://redpencil.data.gift/id/concept/JobStatus/',
  tasko: 'http://lblod.data.gift/id/jobs/concept/TaskOperation/',
  app: 'http://lblod.data.gift/id/app/',
};

/**
 * Some extra prefixes mostly used for writing the data to TTL files in the
 * cleanest possible way.
 *
 * @private
 * @constant
 */
const EXTRA_PREFIXES = {
  lblod: 'http://data.lblod.info/id/',
  ere: 'http://data.lblod.info/vocabularies/erediensten/',
  mandaat: 'http://data.vlaanderen.be/ns/mandaat#',
  org: 'http://www.w3.org/ns/org#',
  mandaten: 'http://data.lblod.info/id/mandaten/',
  schema: 'http://schema.org/',
  person: 'http://www.w3.org/ns/person#',
  foaf: 'http://xmlns.com/foaf/0.1/',
  persoon: 'https://data.vlaanderen.be/ns/persoon#',
  skos: 'http://www.w3.org/2004/02/skos/core#',
  locn: 'http://www.w3.org/ns/locn#',
  contacthub: 'http://data.lblod.info/vocabularies/contacthub/',
  adres: 'https://data.vlaanderen.be/ns/adres#',
  positiesBedienaar: 'http://data.lblod.info/id/positiesBedienaar/',
  vlaanderen: 'https://data.vlaanderen.be/id/',
  country: 'http://publications.europa.eu/resource/authority/country/',
  gender: 'http://publications.europa.eu/resource/authority/human-sex/',
};

/**
 * These prefixes are used for creating nem individuals of data in a given
 * namespace.
 *
 * @private
 * @constant
 */
const BASE = {
  dataContainer: 'http://redpencil.data.gift/id/dataContainers/',
  error: 'http://redpencil.data.gift/id/jobs/error/',
  files: 'http://data.lblod.info/id/files/',
};

/**
 * This object is produced on application startup.
 * It is an object with the same keys as the PREFIXES, but every value is now a
 * template function to produce a namedNode with the given ID in the selected
 * namespace.
 * E.g. if you execute
 *   NAMESPACES.rdf`type`
 * you get the same as
 *   namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
 *
 * @public
 * @constant
 * @type {Object}
 * @param {String} pred - Every value in this object is a fuction that takes an
 * identifier to produce a full URI.
 * @returns {NamedNode} Represents the created individual by its URI.
 */
export const NAMESPACES = (() => {
  const all = {};
  for (const key in PREFIXES)
    all[key] = (pred) => namedNode(`${PREFIXES[key]}${pred}`);
  return all;
})();

/**
 * This object is produced on application startup.
 * It is an object with the same keys as the BASES, but every value is now a
 * template function to produce a namedNode with the given ID in the selected
 * namespace.
 *
 * @see NAMESPACES
 * @public
 * @constant
 * @type {Object}
 * @param {String} pred - Every value in this object is a fuction that takes an
 * identifier to produce a full URI.
 * @returns {NamedNode} Represents the created individual by its URI.
 */
export const BASES = (() => {
  const all = {};
  for (const key in BASE) all[key] = (pred) => namedNode(`${BASE[key]}${pred}`);
  return all;
})();

/**
 * This string contains all the prefixes, ready for use in a SPARQL query.
 *
 * @public
 * @constant
 * @type {String}
 */
export const SPARQL_PREFIXES = (() => {
  const all = [];
  for (const key in PREFIXES) all.push(`PREFIX ${key}: <${PREFIXES[key]}>`);
  return all.join('\n');
})();

/**
 * This object contains all the prefixes a TTL writer might want to use to
 * create prefixed output. It is in the same format as the PREFIXES and
 * EXTRA_PREFIXES.
 *
 * @see PREFIXES, EXTRA_PREFIXES
 * @public
 * @constant
 * @type {Object}
 */
export const WRITER_PREFIXES = (() => {
  const prefs = {};
  for (const key in PREFIXES) prefs[key] = PREFIXES[key];
  for (const key in EXTRA_PREFIXES) prefs[key] = EXTRA_PREFIXES[key];
  return prefs;
})();

// Read and parse JSON config from the `config` folder. Production config will
// override the config that is already in the folder.
const CONFIG_JSON = JSON.parse(fs.readFileSync('/config/config.json'));

// Other constants

export const KNOWN_DOMAINS = CONFIG_JSON['known-domains'] || [];
export const PROTOCOLS_TO_RENAME = CONFIG_JSON['protocols-to-rename'] || [];
export const PREDICATES_TO_IGNORE_FOR_RENAME =
  CONFIG_JSON['predicates-to-ignore'] || [];

export const STATUS_BUSY = NAMESPACES.jobstat`busy`;
export const STATUS_SCHEDULED = NAMESPACES.jobstat`scheduled`;
export const STATUS_SUCCESS = NAMESPACES.jobstat`success`;
export const STATUS_FAILED = NAMESPACES.jobstat`failed`;

export const TASK_TYPE = NAMESPACES.task`Task`;
export const ERROR_TYPE = NAMESPACES.oslc`Error`;

export const TASK_HARVESTING_MIRRORING = NAMESPACES.tasko`mirroring`;
export const TASK_PUBLISH_HARVESTED_TRIPLES = NAMESPACES.tasko`publishHarvestedTriples`;
export const TASK_PUBLISH_HARVESTED_TRIPLES_WITH_DELETES = NAMESPACES.tasko`publishHarvestedTriplesWithDeletes`;
export const TASK_EXECUTE_DIFF_DELETES = NAMESPACES.tasko`execute-diff-deletes`;
export const TASK_HARVESTING_ADD_UUIDS = NAMESPACES.tasko`add-uuids`;
export const TASK_HARVESTING_ADD_TAG = NAMESPACES.tasko`add-harvesting-tag`;

// Environment variables

export const TARGET_GRAPH = namedNode(
  envvar
    .get('TARGET_GRAPH')
    .default('http://mu.semte.ch/graphs/public')
    .asUrlString()
);
export const RENAME_DOMAIN = envvar
  .get('RENAME_DOMAIN')
  .default('http://centrale-vindplaats.lblod.info/id/')
  .asUrlString();

export const SLEEP_TIME = envvar.get('SLEEP_TIME').default('1000').asInt();
export const BATCH_SIZE = envvar.get('BATCH_SIZE').default('100').asInt();
export const RETRY_WAIT_INTERVAL = envvar
  .get('RETRY_WAIT_INTERVAL')
  .default('30000')
  .asInt();
export const MAX_RETRIES = envvar.get('MAX_RETRIES').default('10').asInt();

export const HIGH_LOAD_DATABASE_ENDPOINT = envvar
  .get('HIGH_LOAD_DATABASE_ENDPOINT')
  .default('http://virtuoso:8890/sparql')
  .asUrlString();
