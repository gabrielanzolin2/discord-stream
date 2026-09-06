const { spawn } = require('node:child_process');
const path = require('node:path');

async function startServer(env = {}) {
  const child = spawn(process.execPath, ['server.js'], {
    cwd: path.join(__dirname, '..'),
    env: { ...process.env, PORT: '0', TURN_URLS: '', TURN_SECRET: '', ...env },
    windowsHide: true,
    stdio: ['ignore', 'pipe', 'pipe']
  });
  const port = await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => { child.kill(); reject(new Error('Server startup timeout')); }, 10000);
    child.once('error', reject);
    child.once('exit', (code) => { clearTimeout(timeout); reject(new Error('Server exited: ' + code)); });
    child.stdout.on('data', (chunk) => {
      const match = String(chunk).match(/porta: (\d+)/);
      if (match) { clearTimeout(timeout); resolve(Number(match[1])); }
    });
    child.stderr.on('data', (chunk) => process.stderr.write(chunk));
  });
  return { url: 'http://127.0.0.1:' + port, close: () => child.kill() };
}

module.exports = { startServer };
