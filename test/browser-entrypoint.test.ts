import { expect, test } from 'bun:test';
import * as schemas from '../src';

test('browser root excludes the Node-only consumer runtime', async () => {
  expect('DurableInboundConsumer' in schemas).toBe(false);
  const build = await Bun.build({
    entrypoints: [new URL('../src/index.ts', import.meta.url).pathname],
    target: 'browser',
  });
  expect(build.success).toBe(true);
  expect(await build.outputs[0]!.text()).not.toContain('node:timers/promises');
});
