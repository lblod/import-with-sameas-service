import envvar from 'env-var';
import * as fs from 'fs';
const CONFIG_JSON = JSON.parse(fs.readFileSync('/config/config.json'));
export const KNOWN_DOMAINS = CONFIG_JSON['known-domains'];
export const PROTOCOLS_TO_RENAME = CONFIG_JSON['protocols-to-rename'];

export const STATUS_BUSY =
  'http://redpencil.data.gift/id/concept/JobStatus/busy';
export const STATUS_SCHEDULED =
  'http://redpencil.data.gift/id/concept/JobStatus/scheduled';
export const STATUS_SUCCESS =
  'http://redpencil.data.gift/id/concept/JobStatus/success';
export const STATUS_FAILED =
  'http://redpencil.data.gift/id/concept/JobStatus/failed';

export const TARGET_GRAPH = envvar
  .get('TARGET_GRAPH')
  .default('http://mu.semte.ch/graphs/public')
  .asUrlString();
export const RENAME_DOMAIN = envvar
  .get('RENAME_DOMAIN')
  .default('http://centrale-vindplaats.lblod.info/id/')
  .asUrlString();

export const SLEEP_TIME = envvar.get('SLEEP_TIME').default('1000').asInt();

export const BATCH_SIZE = envvar.get('BATCH_SIZE').default('100').asInt();

export const TASK_TYPE = 'http://redpencil.data.gift/vocabularies/tasks/Task';
export const ERROR_TYPE = 'http://open-services.net/ns/core#Error';

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

export const TASK_HARVESTING_MIRRORING =
  'http://lblod.data.gift/id/jobs/concept/TaskOperation/mirroring';
export const TASK_PUBLISH_HARVESTED_TRIPLES =
  'http://lblod.data.gift/id/jobs/concept/TaskOperation/publishHarvestedTriples';
export const TASK_HARVESTING_ADD_UUIDS =
  'http://lblod.data.gift/id/jobs/concept/TaskOperation/add-uuids';
