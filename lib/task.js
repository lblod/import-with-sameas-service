import { uuid } from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import {
  SPARQL_PREFIXES,
  TASK_TYPE,
  ERROR_TYPE,
  STATUS_BUSY,
  STATUS_SCHEDULED,
  TASK_HARVESTING_MIRRORING,
  TASK_PUBLISH_HARVESTED_TRIPLES,
  TASK_PUBLISH_HARVESTED_TRIPLES_WITH_DELETES,
  TASK_EXECUTE_DIFF_DELETES,
  TASK_HARVESTING_ADD_UUIDS,
  TASK_HARVESTING_ADD_HARVESTING_TAG,
  TASK_HARVESTING_ADD_VENDOR_TAG,
  HIGH_LOAD_DATABASE_ENDPOINT,
} from '../constants';
import { termToString } from 'rdf-string-ttl';
import { SparqlJsonParser } from 'sparqljson-parse';
import { DataFactory } from 'n3';
import { NAMESPACES as ns, BASES as bs } from '../constants';
const { literal } = DataFactory;
const connectionOptions = {
  sparqlEndpoint: HIGH_LOAD_DATABASE_ENDPOINT,
  mayRetry: true,
};
/**
 * Check in the triplestore if the given subject is a Task.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} subject - Represents the subject you want to check.
 * @returns {Boolean} True if the given subject represents a Task, false if
 * not.
 */
export async function isTask(subject) {
  const response = await querySudo(`
    ASK {
      ${termToString(subject)} a ${termToString(TASK_TYPE)} .
    }`);
  const parser = new SparqlJsonParser();
  return parser.parseJsonBoolean(response);
}

/**
 * Load all the details of the task from the triplestore.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} subject - Represents the URI of the task.
 * @returns {Object} The task as an object with properties: graph, task, id,
 * job, created, modified, status, index, operation, error, parentTasks,
 * resultsContainers, inputContainers. All of these are RDF.js values, except
 * for parentTasks, inputContainers, and resultsContainers, which are arrays of
 * RDF.js values.
 */
export async function loadTask(subject) {
  const taskResponse = await querySudo(
    `
    ${SPARQL_PREFIXES}
    SELECT DISTINCT ?graph ?task ?id ?job ?jobId ?created ?modified ?status ?index ?operation ?error WHERE {
      GRAPH ?graph {
        BIND(${termToString(subject)} as ?task)
        ?task
          a ${termToString(TASK_TYPE)} ;
          dct:isPartOf ?job ;
          mu:uuid ?id ;
          dct:created ?created ;
          dct:modified ?modified ;
          adms:status ?status ;
          task:index ?index ;
          task:operation ?operation .
        ?job mu:uuid ?jobId.
        OPTIONAL { ?task task:error ?error . }
      }
    }
    LIMIT 1
  `,
    {},
    connectionOptions,
  );
  const parser = new SparqlJsonParser();
  const task = parser.parseJsonResults(taskResponse)[0];
  if (!task) return;

  const parentTasksResponse = await querySudo(
    `
   ${SPARQL_PREFIXES}
   SELECT DISTINCT ?parentTask WHERE {
     GRAPH ?g {
       ${termToString(subject)} cogs:dependsOn ?parentTask .
      }
    }
  `,
    {},
    connectionOptions,
  );
  const parentTasks = new SparqlJsonParser()
    .parseJsonResults(parentTasksResponse)
    .map((row) => row.parentTask);
  task.parentTasks = parentTasks;

  const resultsContainersResponse = await querySudo(
    `
   ${SPARQL_PREFIXES}
   SELECT DISTINCT ?resultsContainer WHERE {
     GRAPH ?g {
       ${termToString(subject)} task:resultsContainer ?resultsContainer .
      }
    }
  `,
    {},
    connectionOptions,
  );
  const resultsContainers = new SparqlJsonParser()
    .parseJsonResults(resultsContainersResponse)
    .map((row) => row.resultsContainer);
  task.resultsContainers = resultsContainers;

  const inputContainersResponse = await querySudo(
    `
   ${SPARQL_PREFIXES}
   SELECT DISTINCT  ?inputContainer WHERE {
     GRAPH ?g {
       ${termToString(subject)} task:inputContainer ?inputContainer .
      }
    }
  `,
    {},
    connectionOptions,
  );
  const inputContainers = new SparqlJsonParser()
    .parseJsonResults(inputContainersResponse)
    .map((row) => row.inputContainer);
  task.inputContainers = inputContainers;

  return task;
}

/**
 * Updates the task with a new status in the triplestore.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} task - Represents the URI of the task.
 * @param {NamedNode} status - The new status.
 * @returns {undefined} Nothing
 */
export async function updateTaskStatus(task, status) {
  const now = literal(new Date().toISOString(), ns.xsd`dateTime`);
  return updateSudo(`
    ${SPARQL_PREFIXES}
    DELETE {
      GRAPH ?g {
        ?subject adms:status ?status .
        ?subject dct:modified ?modified .
      }
    }
    INSERT {
      GRAPH ?g {
       ?subject adms:status ${termToString(status)} .
       ?subject dct:modified ${termToString(now)} .
      }
    }
    WHERE {
      GRAPH ?g {
        BIND(${termToString(task.task)} as ?subject)
        ?subject adms:status ?status .
        OPTIONAL { ?subject dct:modified ?modified . }
      }
    }`);
}

/**
 * Creates and appends a resultsContainer in the triplestore to the given task
 * with the given file related to it.
 *
 * @async
 * @function
 * @param {NamedNode} task - Task for which to add the resultsContainer.
 * @param {Object} container - Object containing a `node` property that is the
 * `NamedNode` representing the container and an `id` property, a `Literal`,
 * representing the UUID of the container.
 * @param {NamedNode} file - A file that needs to be linked to the
 * resultsContainer.
 * @returns {undefined} Nothing
 */
export async function appendTaskResultFile(task, container, file) {
  return updateSudo(
    `
    ${SPARQL_PREFIXES}
    INSERT DATA {
      GRAPH ${termToString(task.graph)} {
        ${termToString(container.node)}
          a nfo:DataContainer ;
          mu:uuid ${termToString(container.id)} ;
          task:hasFile ${termToString(file)} .
        ${termToString(task.task)}
          task:resultsContainer ${termToString(container.node)} .
      }
    }`,
    {},
    connectionOptions,
  );
}

/**
 * Returns the set of sources (`prov:wasDerivedFrom` values) for which a result
 * file has already been recorded in the task's resultsContainers. This is used
 * to make pipelines resumable: files that were fully processed and recorded by
 * a previous (interrupted) run can be skipped instead of redone.
 *
 * Fetched once per run (rather than queried per file) so a fresh run with many
 * input files does not pay a round-trip per file.
 *
 * @public
 * @async
 * @function
 * @param {Object} task - The task whose results to inspect (needs `graph` and
 * `task` properties).
 * @returns {Set<String>} The IRI values of the already-recorded sources.
 */
export async function getRecordedResultDerivedFroms(task) {
  // Page through the results: a task can have tens of thousands of files and
  // Virtuoso caps a single result set (~10k rows), so an unpaged query would
  // silently truncate and make already-finalized files look unprocessed.
  const pageSize = 5000;
  const parser = new SparqlJsonParser();
  const derivedFroms = new Set();
  for (let offset = 0; ; offset += pageSize) {
    const response = await querySudo(
      `
      ${SPARQL_PREFIXES}
      SELECT DISTINCT ?derivedFrom WHERE {
        GRAPH ${termToString(task.graph)} {
          ${termToString(task.task)} task:resultsContainer ?container .
          ?container task:hasFile ?logicalFile .
          ?logicalFile prov:wasDerivedFrom ?derivedFrom .
        }
      }
      ORDER BY ?derivedFrom
      LIMIT ${pageSize} OFFSET ${offset}`,
      {},
      connectionOptions,
    );
    const rows = parser.parseJsonResults(response);
    for (const row of rows) derivedFroms.add(row.derivedFrom.value);
    if (rows.length < pageSize) break;
  }
  return derivedFroms;
}

/**
 * Creates an Error and appends it to the given task.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} task - Task to which to append the error object.
 * @param {String} errorMsg - A message to explain the error.
 * @returns {undefined} Nothing
 */
export async function appendTaskError(task, errorMsg) {
  const id = literal(uuid());
  const uri = bs.error(id.value);

  return updateSudo(`
   ${SPARQL_PREFIXES}
   INSERT DATA {
    GRAPH ${termToString(task.graph)} {
      ${termToString(uri)}
        a ${termToString(ERROR_TYPE)} ;
        mu:uuid ${termToString(id)} ;
        oslc:message ${termToString(literal(errorMsg))} .
      ${termToString(task.task)} task:error ${termToString(uri)} .
    }
   }`);
}

/**
 * Fetch the tasks from the triplestore that are not finished yet. These tasks
 * are in `busy` or `scheduled` state.
 *
 * @public
 * @async
 * @function
 * @returns {Array(NamedNode)} - Array of tasks in the form of NamedNodes.
 */
export async function getUnfinishedTasks() {
  const response = await querySudo(`
    ${SPARQL_PREFIXES}
    SELECT DISTINCT ?task WHERE {
      VALUES ?operation {
        ${termToString(TASK_HARVESTING_MIRRORING)}
        ${termToString(TASK_PUBLISH_HARVESTED_TRIPLES)}
        ${termToString(TASK_PUBLISH_HARVESTED_TRIPLES_WITH_DELETES)}
        ${termToString(TASK_EXECUTE_DIFF_DELETES)}
        ${termToString(TASK_HARVESTING_ADD_UUIDS)}
        ${termToString(TASK_HARVESTING_ADD_HARVESTING_TAG)}
        ${termToString(TASK_HARVESTING_ADD_VENDOR_TAG)}
      }
      VALUES ?status {
        ${termToString(STATUS_BUSY)}
        ${termToString(STATUS_SCHEDULED)}
      }
      ?task
        a ${termToString(TASK_TYPE)} ;
        adms:status ?status ;
        task:operation ?operation .
    }
  `);
  const parser = new SparqlJsonParser();
  return parser.parseJsonResults(response).map((e) => e.task);
}

/**
 * Get the vendor associated with the Job that is related to the Task.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} task - The task related to a Job that should have a
 * vendor associated with it.
 * @returns {NamedNode} The URI of the vendor.
 */
export async function getVendor(task) {
  const response = await querySudo(`
    ${SPARQL_PREFIXES}
    SELECT ?vendor {
      ${termToString(task)} dct:isPartOf ?job .
      ?job
        rdf:type cogs:Job ;
        prov:wasAssociatedWith ?vendor .
    }
    LIMIT 1
  `);
  const parser = new SparqlJsonParser();
  return parser.parseJsonResults(response)[0]?.vendor;
}

export async function waitForDatabase() {
  const maxRetries = 30;
  const delayMs = 2000;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      await querySudo('ASK { ?s ?p ?o }');
      console.log('Database connection established');
      return;
    } catch {
      console.log(`Waiting for database... (attempt ${attempt}/${maxRetries})`);
      if (attempt === maxRetries) {
        throw new Error(
          `Failed to connect to database after ${maxRetries} attempts`,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
}
