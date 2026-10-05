import { test } from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import extension from '../index.ts';
import type { ExtensionAPI, ExtensionContext } from '@earendil-works/pi-coding-agent';

test('real JSONL server validates envelopes, correlates replies and preserves literal Unicode', async t => {
 const hooks = new Map<string, Function>();
 const messages: unknown[] = [];
 const pi={on:(event: string,fn: Function)=>{hooks.set(event,fn);},
  getCommands:()=>[],getThinkingLevel:()=> 'off',getSessionName:()=>undefined,
  sendUserMessage:(...args:unknown[])=>{messages.push(args);},
 } as unknown as ExtensionAPI;
 extension(pi);
 const id=randomUUID();
 const ctx={sessionManager:{getSessionId:()=>id},hasUI:false,cwd:'/fixture',isIdle:()=>true,hasPendingMessages:()=>false,getContextUsage:()=>undefined,
  compact:(options:any)=>{setImmediate(()=>options.onError(new Error('real callback failure')));},
 } as unknown as ExtensionContext;
 await hooks.get('session_start')!({},ctx);
 t.after(async()=>{await hooks.get('session_shutdown')!({reason:'quit'});});
 // Same documented discovery order, each listener identified by its own UUID.
 const candidates=[process.env.TMPDIR,'/tmp','/var/tmp'].filter(Boolean).map(dir=>path.join(dir!,'pi-rpc-sockets',id+'.sock'));
 let socket: string | undefined;
 for(let i=0;i<100 && !socket;i++){socket=candidates.find(p=>fs.existsSync(p));if(!socket) await new Promise(r=>setTimeout(r,10));}
 assert.ok(socket,'listener did not bind');
 async function request(value: unknown, splitUnicode=false): Promise<any> {
  return new Promise((resolve,reject)=>{
   const client=net.createConnection(socket!);
   client.setEncoding('utf8');client.setTimeout(2000,()=>client.destroy(new Error('timeout')));
   client.on('error',reject);let buffer='';
   client.on('data',data=>{buffer+=data;if(buffer.includes('\n')){client.destroy();resolve(JSON.parse(buffer.split('\n')[0]));}});
   client.on('connect',()=>{
    const bytes=Buffer.from(JSON.stringify(value)+'\n');
    if(splitUnicode){const n=bytes.indexOf(Buffer.from('π'))+1;client.write(bytes.subarray(0,n));setTimeout(()=>client.write(bytes.subarray(n)),10);}
    else client.write(bytes);
   });
  });
 }
 const cap=await request({id:'caps',capabilities:true});assert.equal(cap.id,'caps');assert.equal(cap.protocol,2);
 const msg=await request({id:'send',message:'/literal π'},true);assert.equal(msg.id,'send');assert.equal(msg.status,'submitted');assert.equal(msg.delivered,'/literal π');
 assert.deepEqual(messages,[['/literal π',{deliverAs:'steer',expandPromptTemplates:false}]]);
 for(const value of [null,[],{id:'bad',message:'x',command:'/session'}]) assert.ok((await request(value)).error);
 const fail=await request({id:'direct',compact:true,instructions:'keep it'});assert.equal(fail.id,'direct');assert.equal(fail.ok,false);assert.match(fail.error,/real callback failure/);
 const started=await request({id:'operation',command:'/session'});assert.equal(started.operation.status,'running');
 const polled=await request({id:'poll',getOperation:'operation'});assert.equal(polled.id,'poll');assert.equal(polled.operation.status,'completed');assert.equal(polled.operation.result.sessionId,id);
 const replay=await request({id:'operation',command:'/session'});assert.match(replay.error,/duplicate/);
});
