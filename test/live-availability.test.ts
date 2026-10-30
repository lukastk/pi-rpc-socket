import { test } from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import extension from '../index.ts';
import type { ExtensionAPI, ExtensionContext } from '@earendil-works/pi-coding-agent';

/**
 * A Pi without the interactive-ready capability -- the shape of every released
 * Pi that predates live-v1 -- must still serve protocol 2, and must say through
 * the capabilities reply that live-v1 is unavailable rather than leave a client
 * waiting for a socket that will never appear.
 */

type Harness = {
	request: (value: unknown) => Promise<any>;
	messages: unknown[];
	shutdown: () => Promise<void>;
};

async function start(pi: ExtensionAPI, messages: unknown[]): Promise<Harness> {
	const hooks = new Map<string, Function>();
	const wrapped = {
		...(pi as unknown as Record<string, unknown>),
		on: (event: string, fn: Function) => { hooks.set(event, fn); },
	} as unknown as ExtensionAPI;
	extension(wrapped);
	const id = randomUUID();
	const ctx = {
		sessionManager: { getSessionId: () => id }, hasUI: false, cwd: '/fixture',
		isIdle: () => true, hasPendingMessages: () => false,
		getContextUsage: () => undefined,
	} as unknown as ExtensionContext;
	await hooks.get('session_start')!({}, ctx);
	const candidates = [process.env.TMPDIR, '/tmp', '/var/tmp'].filter(Boolean)
		.map(dir => path.join(dir!, 'pi-rpc-sockets', id + '.sock'));
	let socket: string | undefined;
	for (let i = 0; i < 200 && !socket; i++) {
		socket = candidates.find(p => fs.existsSync(p));
		if (!socket) await new Promise(r => setTimeout(r, 10));
	}
	assert.ok(socket, 'legacy protocol 2 listener did not bind');
	const request = (value: unknown) => new Promise<any>((resolve, reject) => {
		const client = net.createConnection(socket!);
		client.setEncoding('utf8');
		client.setTimeout(2000, () => client.destroy(new Error('timeout')));
		client.on('error', reject);
		let buffer = '';
		client.on('data', data => {
			buffer += data;
			if (buffer.includes('\n')) { client.destroy(); resolve(JSON.parse(buffer.split('\n')[0])); }
		});
		client.on('connect', () => client.write(JSON.stringify(value) + '\n'));
	});
	return { request, messages, shutdown: async () => { await hooks.get('session_shutdown')!({ reason: 'quit' }); } };
}

function basePi(messages: unknown[]): Record<string, unknown> {
	return {
		getCommands: () => [], getThinkingLevel: () => 'off',
		getSessionName: () => undefined,
		sendUserMessage: (...args: unknown[]) => { messages.push(args); },
	};
}

test('a Pi with no interactive-ready capability reports liveTransport "unsupported" and keeps protocol 2', async t => {
	const messages: unknown[] = [];
	const pi = basePi(messages) as unknown as ExtensionAPI;
	assert.equal(typeof (pi as any).registerInteractiveReady, 'undefined',
		'fixture must model a host WITHOUT the capability');
	const h = await start(pi, messages);
	t.after(() => h.shutdown());

	const caps = await h.request({ id: 'caps', capabilities: true });
	assert.equal(caps.ok, true);
	assert.equal(caps.protocol, 2, 'protocol 2 is unchanged by the live-v1 lane');
	assert.equal(caps.liveTransport, 'unsupported');

	// Not merely "the reply has a field": the legacy lane must still work.
	const sent = await h.request({ id: 'send', message: 'hello' });
	assert.equal(sent.status, 'submitted');
	assert.deepEqual(messages, [['hello', { deliverAs: 'steer', expandPromptTemplates: false }]]);
	const tmux = await h.request({ getTmuxInfo: true });
	assert.equal(tmux.ok, true);
});

test('a Pi that exposes the capability registers the handler and reports "supported" before the socket exists', async t => {
	const messages: unknown[] = [];
	const registered: Function[] = [];
	const pi = {
		...basePi(messages),
		registerInteractiveReady: (handler: Function) => { registered.push(handler); },
	} as unknown as ExtensionAPI;
	const h = await start(pi, messages);
	t.after(() => h.shutdown());

	assert.equal(registered.length, 1, 'the live-v1 handler is registered exactly once');
	assert.equal(typeof registered[0], 'function');
	const caps = await h.request({ id: 'caps', capabilities: true });
	assert.equal(caps.protocol, 2);
	// "supported", not "active": nothing has started a live socket yet, and
	// claiming otherwise would make a client wait on a listener that is absent.
	assert.equal(caps.liveTransport, 'supported');
});

test('a host whose registerInteractiveReady is not callable is treated as unsupported, not fatal', async t => {
	const messages: unknown[] = [];
	const pi = { ...basePi(messages), registerInteractiveReady: 'not-a-function' } as unknown as ExtensionAPI;
	const h = await start(pi, messages);
	t.after(() => h.shutdown());
	const caps = await h.request({ id: 'caps', capabilities: true });
	assert.equal(caps.liveTransport, 'unsupported');
	assert.equal(caps.protocol, 2);
});
