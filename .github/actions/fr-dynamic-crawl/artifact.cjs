const fs = require('node:fs');
const path = require('node:path');
const { DefaultArtifactClient } = require('@actions/artifact');
const client = new DefaultArtifactClient();
function walk(dir) {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap(e => {
    const file = path.join(dir, e.name);
    if (e.isSymbolicLink()) throw new Error('Symlink not allowed in task result');
    return e.isDirectory() ? walk(file) : [file];
  });
}
async function find(name) {
  try {
    return (await client.getArtifact(name)).artifact;
  } catch (e) {
    if (e.name === 'ArtifactNotFoundError' || e.constructor?.name === 'ArtifactNotFoundError') return null;
    throw e;
  }
}
async function main() {
  const [op, name, dir] = process.argv.slice(2);
  let result;
  if (op === 'upload') {
    const root = path.resolve(dir);
    try {
      const uploaded = await client.uploadArtifact(name, walk(root), root, { retentionDays: 3, compressionLevel: 1 });
      result = { id: uploaded.id };
    } catch (error) {
      // A lost response may have followed a successful immutable upload.
      const existing = await find(name);
      if (!existing) throw error;
      // Verify byte-for-byte before accepting a retried upload.
      const temp = fs.mkdtempSync(path.join(require('node:os').tmpdir(), 'fr-result-'));
      try {
        await client.downloadArtifact(existing.id, { path: temp });
        const source = walk(root).map(f => path.relative(root, f)).sort();
        const saved = walk(temp).map(f => path.relative(temp, f)).sort();
        if (JSON.stringify(source) !== JSON.stringify(saved) ||
            source.some(f => !fs.readFileSync(path.join(root,f)).equals(fs.readFileSync(path.join(temp,f)))))
          throw new Error('Existing result differs from the local immutable capture');
        result = { id: existing.id };
      } finally { fs.rmSync(temp, { recursive:true, force:true }); }
    }
  } else if (op === 'recover') {
    const existing = await find(name);
    if (!existing) result = null;
    else {
      await client.downloadArtifact(existing.id, { path: path.resolve(dir) });
      result = { id: existing.id };
    }
  } else throw new Error('Unsupported artifact operation');
  console.log('FR_ARTIFACT_RESULT=' + JSON.stringify(result));
}
main().catch(e => { console.error(e.message); process.exitCode = 1; });
