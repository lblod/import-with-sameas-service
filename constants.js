export const KNOWN_DOMAINS = [
  'data.lblod.info',
  'data.vlaanderen.be',
  'mu.semte.ch',
  'data.europa.eu',
  'purl.org',
  'www.ontologydesignpatterns.org',
  'www.w3.org',
  'xmlns.com',
  'www.semanticdesktop.org',
  'schema.org',
  'centrale-vindplaats.lblod.info'
];

export const PROTOCOLS_TO_RENAME = [
  'http:',
  'https:',
  'ftp:',
  'ftps:'
];

export const STATUS_BUSY = 'http://redpencil.data.gift/id/concept/JobStatus/busy';
export const STATUS_SCHEDULED = 'http://redpencil.data.gift/id/concept/JobStatus/scheduled';
export const STATUS_SUCCESS = 'http://redpencil.data.gift/id/concept/JobStatus/success';
export const STATUS_FAILED = 'http://redpencil.data.gift/id/concept/JobStatus/failed';

export const TARGET_GRAPH = process.env.TARGET_GRAPH || 'http://mu.semte.ch/graphs/public';
export const RENAME_DOMAIN = process.env.RENAME_DOMAIN || 'http://centrale-vindplaats.lblod.info/id/';


export const TASK_TYPE = 'http://redpencil.data.gift/vocabularies/tasks/Task';
export const ERROR_TYPE= 'http://open-services.net/ns/core#Error';

export const ERROR_URI_PREFIX = 'http://redpencil.data.gift/id/jobs/error/';

export const PREFIXES = `
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX prov: <http://www.w3.org/ns/prov#>
  PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX oslc: <http://open-services.net/ns/core#>
  PREFIX cogs: <http://vocab.deri.ie/cogs#>
  PREFIX adms: <http://www.w3.org/ns/adms#>
`;

export const TASK_HARVESTING_MIRRORING = 'http://lblod.data.gift/id/jobs/concept/TaskOperation/mirroring';
