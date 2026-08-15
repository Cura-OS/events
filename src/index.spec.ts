import { expect, test } from 'bun:test';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);

test('published root exports schemas and Node runtime only from its subpath', async () => {
  const schemas = require('@curaos/events');
  const node = require('@curaos/events/node');
  expect('DurableInboundConsumer' in schemas).toBe(false);
  expect(typeof node.DurableInboundConsumer).toBe('function');
  const packageJson = await Bun.file(new URL('../package.json', import.meta.url)).json();
  expect(packageJson.exports['./node']).toEqual({
    types: './dist/node.d.ts',
    node: './dist/node.js',
  });
  const build = await Bun.build({
    entrypoints: [new URL('./index.ts', import.meta.url).pathname],
    target: 'browser',
  });
  expect(build.success).toBe(true);
  expect(await build.outputs[0]!.text()).not.toContain('node:timers/promises');

  const rejectedNodeRuntime = await Bun.build({
    entrypoints: ['virtual:browser-entry'],
    target: 'browser',
    throw: false,
    logLevel: 'silent',
    plugins: [{
      name: 'browser-entry',
      setup(builder) {
        builder.onResolve({ filter: /^virtual:browser-entry$/ }, () => ({
          path: 'browser-entry', namespace: 'browser-entry',
        }));
        builder.onLoad({ filter: /.*/, namespace: 'browser-entry' }, () => ({
          contents: "import '@curaos/events/node'",
          loader: 'js',
        }));
      },
    }],
  });
  expect(rejectedNodeRuntime.success).toBe(false);
});
