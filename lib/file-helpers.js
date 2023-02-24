import * as mu from 'mu';
import * as mas from '@lblod/mu-auth-sudo';
import * as cts from '../constants';
import fs from 'fs/promises';

/**
 * Write the given TTL content to a file and relates it to the given remote file and submitted document
 *
 * @param string ttl Turtle to write to the file
 * @param string submittedDocument URI of the submittedDocument to relate the new TTL file to
 * @param string remoteFile URI of the remote file to relate the new TTL file to
 */
export async function writeTtlFile(graph, content, logicalFileName) {
  const phyId = mu.uuid();
  const phyFilename = `${phyId}.ttl`;
  const path = `/share/${phyFilename}`;
  const physicalFile = path.replace('/share/', 'share://');
  const loId = mu.uuid();
  const logicalFile = cts.BASES.files`${loId}`.value;
  const now = new Date();

  try {
    await fs.writeFile(path, content, 'utf-8');
  } catch (e) {
    console.log(`Failed to write TTL to file <${physicalFile}>.`);
    throw e;
  }

  try {
    const stats = await fs.stat(path);
    const fileSize = stats.size;

    await mas.updateSudo(`
      ${cts.SPARQL_PREFIXES}
      INSERT DATA {
        GRAPH ${mu.sparqlEscapeUri(graph)} {
          ${mu.sparqlEscapeUri(physicalFile)}
            a nfo:FileDataObject ;
            nie:dataSource ${mu.sparqlEscapeUri(logicalFile)} ;
            mu:uuid ${mu.sparqlEscapeString(phyId)} ;
            nfo:fileName ${mu.sparqlEscapeString(phyFilename)} ;
            dct:creator <http://lblod.data.gift/services/harvesting-import-service> ;
            dct:created ${mu.sparqlEscapeDateTime(now)} ;
            dct:modified ${mu.sparqlEscapeDateTime(now)} ;
            dct:format "text/turtle" ;
            nfo:fileSize ${mu.sparqlEscapeInt(fileSize)} ;
            dbpedia:fileExtension "ttl" .
          ${mu.sparqlEscapeUri(logicalFile)}
            a nfo:FileDataObject ;
            mu:uuid ${mu.sparqlEscapeString(loId)} ;
            nfo:fileName ${mu.sparqlEscapeString(logicalFileName)} ;
            dct:creator <http://lblod.data.gift/services/harvesting-import-service> ;
            dct:created ${mu.sparqlEscapeDateTime(now)} ;
            dct:modified ${mu.sparqlEscapeDateTime(now)} ;
            dct:format "text/turtle" ;
            nfo:fileSize ${mu.sparqlEscapeInt(fileSize)} ;
            dbpedia:fileExtension "ttl" .
        }
      }
`);
  } catch (e) {
    console.log(
      `Failed to write TTL resource <${logicalFile}> to triplestore.`
    );
    throw e;
  }

  return logicalFile;
}
