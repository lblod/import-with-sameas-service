import {
  NAMESPACES as ns,
  STATUS_SCHEDULED,
  STATUS_FAILED,
  TASK_HARVESTING_MIRRORING,
  TASK_HARVESTING_ADD_UUIDS,
  TASK_HARVESTING_ADD_HARVESTING_TAG,
  TASK_HARVESTING_ADD_VENDOR_TAG,
  TASK_PUBLISH_HARVESTED_TRIPLES,
  TASK_PUBLISH_HARVESTED_TRIPLES_WITH_DELETES,
  TASK_EXECUTE_DIFF_DELETES,
  TASK_TIMEOUT_HOURS,
} from './constants';
import {
  getUnfinishedTasks,
  waitForDatabase,
  isTask,
  loadTask,
  updateTaskStatus,
  appendTaskError,
} from './lib/task';
import { DataFactory } from 'n3';
import bodyParser from 'body-parser';
import { app, errorHandler } from 'mu';
import { run as runMirrorPipeline } from './lib/pipeline-mirroring';
import { run as runPublishPipeline } from './lib/pipeline-publishing';
import { run as runAddUUIDs } from './lib/pipeline-add-uuids';
import { run as runExecuteDiffDeletesPipeline } from './lib/pipeline-execute-diff-deletes';
import { run as runAddHarvestingTag } from './lib/pipeline-add-harvesting-tag';
import { run as runAddVendorTag } from './lib/pipeline-add-vendor-tag';
import { Lock } from 'async-await-mutex-lock';
const { namedNode } = DataFactory;

/**
 * Custom error class for task timeouts
 */
class TaskTimeoutError extends Error {
  constructor(taskUri, timeoutHours) {
    super(`Task ${taskUri} timed out after ${timeoutHours} hours`);
    this.name = 'TaskTimeoutError';
    this.taskUri = taskUri;
    this.timeoutHours = timeoutHours;
  }
}

/**
 * Lock to make sure that some functions don't run at the same time. E.g. when
 * a delta is still processing, you don't want to allow the manual searching
 * and restarting as it will also pick up the delta that is already busy
 * processing.
 */
const LOCK = new Lock();

app.use(
  bodyParser.json({
    limit: '50mb',
    type: function (req) {
      return /^application\/json/.test(req.get('content-type'));
    },
  }),
);

/**
 * Find unfinished tasks (busy or scheduled) and start them (again) from
 * scratch.
 *
 * @async
 * @function
 * @returns { undefined } Nothing
 */
async function findAndStartUnfinishedTasks() {
  try {
    const unfinishedTasks = await getUnfinishedTasks();
    for (const term of unfinishedTasks) await processTask(term);
  } catch (e) {
    console.error('Something went wrong while scheduling unfinished taks', e);
  }
}

/**
 * Run on startup.
 */
setTimeout(async () => {
  console.log('check if there is a task');
  await waitForDatabase();
  await findAndStartUnfinishedTasks();
}, 1000);

app.get('/', function (_, res) {
  res.send('Hello harvesting-import-sameas-service');
});

app.post('/find-and-start-unfinished-tasks', async function (req, res) {
  res
    .json({ status: 'Finding and restarting unfinished tasks' })
    .status(200)
    .end();
  await LOCK.acquire();
  try {
    await findAndStartUnfinishedTasks();
  } finally {
    LOCK.release();
  }
});

app.post('/force-retry-task', async function (req, res) {
  const taskUri = req.body?.uri;
  if (!taskUri)
    res.status(400).send({
      status:
        'No task URI given in the request body. Please send a JSON body with a `status` key and a task URI as value.',
    });
  res.status(200).send({ status: `Force restarting task \`${taskUri}\`` });
  await LOCK.acquire();
  try {
    await processTask(namedNode(taskUri));
  } finally {
    LOCK.release();
  }
});

app.post('/delta', async function (req, res) {
  // The delta notifier does not care about the result. Just return as soon as
  // possible.
  res.status(200).send().end();
  try {
    await LOCK.acquire();
    // Filter for triples in the body that are inserts about a task with a
    // status 'scheduled'.
    const taskSubjects = req.body
      .map((changeset) => changeset.inserts)
      .flat()
      .filter((insert) => insert.predicate.value === ns.adms`status`.value)
      .filter((insert) => insert.object.value === STATUS_SCHEDULED.value)
      .map((insert) => namedNode(insert.subject.value));
    if (!taskSubjects.length) {
      console.log(
        'Delta did not contain potential tasks that are interesting, awaiting the next batch!',
      );
    }
    for (const subject of taskSubjects) await processTask(subject);
  } catch (e) {
    console.error(
      'Something unexpected went wrong while handling delta task!',
      e,
    );
  } finally {
    LOCK.release();
  }
});

/**
 * Timeout wrapper for task execution with rollback support.
 * Optionally enforces a timeout based on TASK_TIMEOUT_HOURS environment variable.
 * When timeout occurs, triggers an abort on the task and fails task
 *
 * @async
 * @function
 * @param {Function} taskFunction - The pipeline function to execute
 * @param {Object} task - The task object
 * @param {...any} args - Additional arguments for the pipeline function
 * @returns {undefined} Nothing
 */
async function runWithTimeout(taskFunction, task, ...args) {
  // If timeout is disabled (0 or not set), run without timeout
  if (TASK_TIMEOUT_HOURS === 0) {
    return await taskFunction(task, ...args);
  }

  const timeoutMs = TASK_TIMEOUT_HOURS * 60 * 60 * 1000; //  Convert hours to milliseconds

  const ac = new AbortController();

  let timeoutId;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => {
      ac.abort();
      reject(new TaskTimeoutError(task.task.value, TASK_TIMEOUT_HOURS));
    }, timeoutMs);
  });

  try {
    const result = await Promise.race([
      taskFunction(task,ac.signal, ...args),
      timeoutPromise
    ]);
    clearTimeout(timeoutId);
    return result;
  } catch (error) {
    clearTimeout(timeoutId);

    // If it's a timeout error, mark task as failed without rollback
    if (error instanceof TaskTimeoutError) {
      console.error(`Task ${error.taskUri} timed out after ${error.timeoutHours} hours. Marking as failed without rollback due to unknown state.`);

      // Set task status to failed and append error
      await appendTaskError(task, error.message);
      await updateTaskStatus(task, STATUS_FAILED);
    }

    throw error;
  }
}

/**
 * Check if the given term is a task, load details from the task and execute
 * the correct pipeline for this task.
 *
 * @async
 * @function
 * @param { NamedNode } term - Represents the task that needs to be started.
 * @returns { undefined } Nothing
 */
async function processTask(term) {
  try {
    if (await isTask(term)) {
      const task = await loadTask(term);
      switch (task.operation.value) {
        case TASK_HARVESTING_MIRRORING.value:
          await runWithTimeout(runMirrorPipeline, task);
          break;
        case TASK_HARVESTING_ADD_UUIDS.value:
          await runWithTimeout(runAddUUIDs, task);
          break;
        case TASK_HARVESTING_ADD_HARVESTING_TAG.value:
          await runWithTimeout(runAddHarvestingTag, task);
          break;
        case TASK_HARVESTING_ADD_VENDOR_TAG.value:
          await runWithTimeout(runAddVendorTag, task);
          break;
        case TASK_PUBLISH_HARVESTED_TRIPLES.value:
          await runWithTimeout(runPublishPipeline, task, false);
          break;
        case TASK_PUBLISH_HARVESTED_TRIPLES_WITH_DELETES.value:
          await runWithTimeout(runPublishPipeline, task, true);
          break;
        case TASK_EXECUTE_DIFF_DELETES.value:
          await runWithTimeout(runExecuteDiffDeletesPipeline, task);
          break;
      }
    }
  } catch (e) {
    console.error(
      `Something went wrong while processing task: ${term.value}`,
      e,
    );
  }
}

app.use(errorHandler);
