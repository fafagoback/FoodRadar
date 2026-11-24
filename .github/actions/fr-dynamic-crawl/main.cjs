const { spawnSync } = require('node:child_process');
const index = process.env.INPUT_INDEX;
if (!/^(?:[0-9]|1[0-4])$/.test(index || '')) throw new Error('Invalid worker index');
const result = spawnSync('python', ['src/dynamic_crawl.py', 'worker', '--index', index], {
  cwd: process.env.GITHUB_WORKSPACE, stdio: 'inherit', env: process.env
});
if (result.error) throw result.error;
process.exitCode = result.status === null ? 1 : result.status;
