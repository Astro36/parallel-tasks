/* ParallelTasks
Copyright (C) 2017  Astro
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.
You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

const cluster = require('cluster');
const numCPUs = require('os').cpus().length;

const run = funcs => new Promise((resolve) => {
  if (cluster.isMaster) {
    let funcsIndex = 0;
    const funcCount = funcs.length;
    const { workers } = cluster;
    const messageHandler = (message) => {
      if (message.type === 'response') {
        const worker = workers[message.workerId];
        if (funcsIndex < funcCount) {
          worker.send({
            type: 'request',
            workerId: message.workerId,
            funcsIndex,
          });
        } else {
          worker.disconnect();
          worker.kill();
        }
        if (funcsIndex + 1 === funcCount + numCPUs) {
          resolve();
        }
        funcsIndex += 1;
      }
    };
    for (let i = 0; i < numCPUs; i += 1) {
      const worker = cluster.fork();
      worker.on('message', messageHandler);
      worker.send({
        type: 'request',
        workerId: (i + 1).toString(),
        funcsIndex,
      });
      funcsIndex += 1;
    }
  } else {
    process.on('message', (message) => {
      if (message.type === 'request') {
        const func = funcs[message.funcsIndex];
        const done = () => {
          process.send({
            type: 'response',
            workerId: message.workerId,
          });
        };
        if (typeof func === 'function') {
          const ret = func();
          if (ret instanceof Promise) {
            ret.then(done);
          } else {
            done();
          }
        } else {
          done();
        }
      }
    });
  }
});

exports.run = run;
