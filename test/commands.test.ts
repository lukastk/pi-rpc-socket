import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { ExtensionAPI, ExtensionContext, CompactOptions } from '@earendil-works/pi-coding-agent';
import { Commands } from '../commands.ts';

function fixture() {
 let name: string | undefined, thinking = 'high', time = 0;
 let compact: CompactOptions | undefined;
 const messages: unknown[] = [];
 let streamClaims = 0;
 const ctx = { isIdle: () => true, hasPendingMessages: () => false,
  compact: (options: CompactOptions) => { compact = options; }, cwd: '/fixture',
  sessionManager: { getSessionId: () => 'session-fixture' }, getContextUsage: () => undefined,
 } as unknown as ExtensionContext;
 const pi = { getCommands: () => [{name:'review',source:'prompt'}, {name:'skill:test',source:'skill'}, {name:'custom',source:'extension'}],
  sendUserMessage: (...args: unknown[]) => { messages.push(args); },
  setSessionName: (n: string) => {name=n;}, getSessionName: () => name,
  setThinkingLevel: (n: string) => {thinking=n;}, getThinkingLevel: () => thinking,
 } as unknown as ExtensionAPI;
 const commands = new Commands(pi,()=>ctx,async pattern => {
  if(pattern==='bad') throw new Error('no such model'); return {model: pattern};
 },()=>{streamClaims++;},()=>time);
 return {commands,ctx,pi,messages,streamClaims:()=>streamClaims,compact:()=>compact, advance:()=>{time+=3600_001;}};
}
const tick = () => new Promise(resolve => setImmediate(resolve));

test('compaction is pending until callback; instructions preserved; failure is not success', async () => {
 const f=fixture();
 assert.equal(f.commands.start('one','/compact retain MAGIC').status,'running');
 await tick(); assert.equal(f.commands.get('one').status,'running');
 assert.equal(f.compact()?.customInstructions,'retain MAGIC');
 f.compact()!.onError!(new Error('Nothing to compact'));
 await tick(); assert.deepEqual(f.commands.get('one'),{status:'failed',error:'Nothing to compact'});
 f.commands.start('two','/compact'); f.compact()!.onComplete!({summary:'kept'} as never);
 await tick(); assert.deepEqual(f.commands.get('two'),{status:'completed',result:{summary:'kept'}});
});
test('busy compaction refuses; request IDs never replay and finished operations expire', async () => {
 const f=fixture(); f.ctx.isIdle=()=>false;
 f.commands.start('busy','/compact'); await tick();
 assert.match(f.commands.get('busy').error!,/idle/); assert.equal(f.compact(),undefined);
 assert.throws(()=>f.commands.start('busy','/name overwritten'),/duplicate/);
 f.advance(); assert.throws(()=>f.commands.get('busy'),/expired/);
});
test('native operations, validation and honest resource submission', async () => {
 const f=fixture();
 for(const [i,cmd] of ['/name demo','/thinking low','/model valid','/session','/review args','/skill:test','/custom'].entries()) {
  f.commands.start(String(i),cmd); await tick();
  assert.equal(f.commands.get(String(i)).status,i<4?'completed':'submitted');
 }
 assert.equal(f.pi.getSessionName(),'demo'); assert.equal(f.pi.getThinkingLevel(),'low');
 assert.deepEqual(f.messages[0],['/review args',{deliverAs:'steer',expandPromptTemplates:true}]);
 assert.equal(f.streamClaims(),2,'only template/skill dispatch arms a streamed turn, not a command that might never start one');
 for(const cmd of ['/unknown','/reload','/new','/quit','hello']) assert.throws(()=>f.commands.start('bad',cmd));
 f.commands.start('invalid','/thinking enormous'); await tick(); assert.match(f.commands.get('invalid').error!,/invalid thinking/);
 f.commands.start('model','/model bad'); await tick(); assert.match(f.commands.get('model').error!,/no such model/);
 assert.throws(()=>f.commands.start('','/session'),/request id/);
 assert.throws(()=>f.commands.get('missing'),/unknown/);
 assert.equal(f.commands.capabilities().protocol,2);
});
test('capacity refuses without evicting running operations', () => {
 const f=fixture();
 for(let i=0;i<256;i++) f.commands.start(String(i),'/compact');
 f.advance(); assert.throws(()=>f.commands.start('excess','/session'),/capacity/);
 assert.equal(f.commands.get('0').status,'running');
});
