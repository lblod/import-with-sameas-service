import { app, errorHandler } from 'mu';
import { Delta } from './lib/delta';
import {
  STATUS_SCHEDULED,
  TASK_PUBLISH_HARVESTED_TRIPLES,
  TASK_HARVESTING_MIRRORING,
  TASK_HARVESTING_ADD_UUIDS,
} from './constants';
import { run as runMirrorPipeline } from './lib/pipeline-mirroring';
import { run as runImportPipeline } from './lib/pipeline-importing';
import { run as runAddUUIDs } from './lib/pipeline-add-uuids';
import { isTask, loadTask } from './lib/task';
import bodyParser from 'body-parser';

app.use(
  bodyParser.json({
    type: function (req) {
      return /^application\/json/.test(req.get('content-type'));
    },
  })
);

app.get('/', function (_, res) {
  res.send('Hello harvesting-url-mirror');
});

app.post('/delta', async function (req, res, next) {
  try {
    const entries = new Delta(req.body).getInsertsFor(
      'http://www.w3.org/ns/adms#status',
      STATUS_SCHEDULED.value
    );
    if (!entries.length) {
      console.log(
        'Delta dit not contain potential tasks that are interesting, awaiting the next batch!'
      );
      return res.status(204).send();
    }

    for (let entry of entries) {
      if (!(await isTask(entry))) continue;
      const task = await loadTask(entry);

      if (isMirroringTask(task)) {
        await runMirrorPipeline(task);
      } else if (isImportingTask(task)) {
        await runImportPipeline(task);
      } else if (isAddingMuUUIDTask(task)) {
        await runAddUUIDs(task);
      }
    }

    return res.status(200).send().end();
  } catch (e) {
    console.log('Something unexpected went wrong while handling delta task!');
    console.error(e);
    return next(e);
  }
});

function isImportingTask(task) {
  return task.operation == TASK_PUBLISH_HARVESTED_TRIPLES.value;
}

function isMirroringTask(task) {
  return task.operation == TASK_HARVESTING_MIRRORING.value;
}

function isAddingMuUUIDTask(task) {
  return task.operation === TASK_HARVESTING_ADD_UUIDS.value;
}

app.use(errorHandler);
