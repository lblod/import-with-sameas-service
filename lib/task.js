import * as mu from 'mu';
import * as mas from '@lblod/mu-auth-sudo';
import * as cts from '../constants';
import * as rst from 'rdf-string-ttl';
import * as sjp from 'sparqljson-parse';
import * as N3 from 'n3';
import { NAMESPACES as ns } from '../constants';
import { BASES as bs } from '../constants';
import { HIGH_LOAD_DATABASE_ENDPOINT } from '../constants';
const { literal } = N3.DataFactory;
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
  const response = await mas.querySudo(`
    ASK {
      ${rst.termToString(subject)} a ${rst.termToString(cts.TASK_TYPE)} .
    }`);
  const parser = new sjp.SparqlJsonParser();
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
  const taskResponse = await mas.querySudo(
    `
    ${cts.SPARQL_PREFIXES}
    SELECT DISTINCT ?graph ?task ?id ?job ?created ?modified ?status ?index ?operation ?error WHERE {
      GRAPH ?graph {
        BIND(${rst.termToString(subject)} as ?task)
        ?task
          a ${rst.termToString(cts.TASK_TYPE)} ;
          dct:isPartOf ?job ;
          mu:uuid ?id ;
          dct:created ?created ;
          dct:modified ?modified ;
          adms:status ?status ;
          task:index ?index ;
          task:operation ?operation .
        OPTIONAL { ?task task:error ?error . }
      }
    }
    LIMIT 1
  `,
    {},
    connectionOptions
  );
  const parser = new sjp.SparqlJsonParser();
  const task = parser.parseJsonResults(taskResponse)[0];
  if (!task) return;

  const parentTasksResponse = await mas.querySudo(
    `
   ${cts.SPARQL_PREFIXES}
   SELECT DISTINCT ?parentTask WHERE {
     GRAPH ?g {
       ${rst.termToString(subject)} cogs:dependsOn ?parentTask .
      }
    }
  `,
    {},
    connectionOptions
  );
  const parentTasks = parser
    .parseJsonResults(parentTasksResponse)
    .map((row) => row.parentTask);
  task.parentTasks = parentTasks;

  const resultsContainersResponse = await mas.querySudo(
    `
   ${cts.SPARQL_PREFIXES}
   SELECT DISTINCT ?resultsContainer WHERE {
     GRAPH ?g {
       ${rst.termToString(subject)} task:resultsContainer ?resultsContainer .
      }
    }
  `,
    {},
    connectionOptions
  );
  const resultsContainers = parser
    .parseJsonResults(resultsContainersResponse)
    .map((row) => row.resultsContainer);
  task.resultsContainers = resultsContainers;

  const inputContainersResponse = await mas.querySudo(
    `
   ${cts.SPARQL_PREFIXES}
   SELECT DISTINCT  ?inputContainer WHERE {
     GRAPH ?g {
       ${rst.termToString(subject)} task:inputContainer ?inputContainer .
      }
    }
  `,
    {},
    connectionOptions
  );
  const inputContainers = parser
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
  return mas.updateSudo(`
    ${cts.SPARQL_PREFIXES}
    DELETE {
      GRAPH ?g {
        ?subject adms:status ?status .
        ?subject dct:modified ?modified .
      }
    }
    INSERT {
      GRAPH ?g {
       ?subject adms:status ${rst.termToString(status)} .
       ?subject dct:modified ${rst.termToString(now)} .
      }
    }
    WHERE {
      GRAPH ?g {
        BIND(${rst.termToString(task.task)} as ?subject)
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
  return mas.updateSudo(
    `
    ${cts.SPARQL_PREFIXES}
    INSERT DATA {
      GRAPH ${rst.termToString(task.graph)} {
        ${rst.termToString(container.node)}
          a nfo:DataContainer ;
          mu:uuid ${rst.termToString(container.id)} ;
          task:hasFile ${rst.termToString(file)} .
        ${rst.termToString(task.task)}
          task:resultsContainer ${rst.termToString(container.node)} .
      }
    }`,
    {},
    connectionOptions
  );
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
  const id = literal(mu.uuid());
  const uri = bs.error(id.value);

  return mas.updateSudo(`
   ${cts.SPARQL_PREFIXES}
   INSERT DATA {
    GRAPH ${rst.termToString(task.graph)} {
      ${rst.termToString(uri)}
        a ${rst.termToString(cts.ERROR_TYPE)} ;
        mu:uuid ${rst.termToString(id)} ;
        oslc:message ${rst.termToString(literal(errorMsg))} .
      ${rst.termToString(task.task)} task:error ${rst.termToString(uri)} .
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
  const response = await mas.querySudo(`
    ${cts.SPARQL_PREFIXES}
    SELECT DISTINCT ?task WHERE {
      VALUES ?operation {
        ${rst.termToString(cts.TASK_HARVESTING_MIRRORING)}
        ${rst.termToString(cts.TASK_PUBLISH_HARVESTED_TRIPLES)}
        ${rst.termToString(cts.TASK_PUBLISH_HARVESTED_TRIPLES_WITH_DELETES)}
        ${rst.termToString(cts.TASK_EXECUTE_DIFF_DELETES)}
        ${rst.termToString(cts.TASK_HARVESTING_ADD_UUIDS)}
        ${rst.termToString(cts.TASK_HARVESTING_ADD_TAG)}
      }
      VALUES ?status {
        ${rst.termToString(cts.STATUS_BUSY)}
        ${rst.termToString(cts.STATUS_SCHEDULED)}
      }
      ?task
        a ${rst.termToString(cts.TASK_TYPE)} ;
        adms:status ?status ;
        task:operation ?operation .
    }
  `);
  const parser = new sjp.SparqlJsonParser();
  return parser.parseJsonResults(response).map((e) => e.task);
}

export async function waitForDatabase() {
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      await mas.querySudo('ASK { ?s ?p ?o }');
      break;
    } catch (e) {
      console.log('wait for database...');
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
}
