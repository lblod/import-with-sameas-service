import {app, errorHandler} from 'mu';
import { Delta } from "./lib/delta";
import { STATUS_SCHEDULED, } from './constants';
import  { run as runMirrorPipeline } from './lib/pipeline-mirroring';

import bodyParser from 'body-parser';

app.use(bodyParser.json({
  type: function (req) {
    return /^application\/json/.test(req.get('content-type'));
  }
}));

app.get('/', function (req, res) {
  res.send('Hello harvesting-url-mirror');
});

app.post('/delta', async function (req, res, next) {
  try {
    const entries = new Delta(req.body).getInsertsFor('http://www.w3.org/ns/adms#status', STATUS_SCHEDULED);
    if (!entries.length) {
      console.log('Delta dit not contain potential tasks that are interesting, awaiting the next batch!');
      return res.status(204).send();
    }

    for (let entry of entries) {
      await runMirrorPipeline(entry);
    }

    return res.status(200).send().end();
  } catch (e) {
    console.log(`Something unexpected went wrong while handling delta task!`);
    console.error(e);
    return next(e);
  }
});

app.use(errorHandler);
