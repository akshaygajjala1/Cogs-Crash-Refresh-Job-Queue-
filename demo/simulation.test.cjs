const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8').match(/<script>([\s\S]*)<\/script>/)[1];
function boot(lease = 2) {
  const els = new Map(); let intervalFn; let active = false;
  const make = id => ({id, value:id === 'lease' ? String(lease) : id === 'batch' ? '2' : '', textContent:'', innerHTML:'', disabled:false, className:'', style:{}, children:[], prepend(x){x.parentNode=this;this.children.unshift(x)}, append(x){x.parentNode=this;this.children.push(x)}, setAttribute(){}, remove(){if(this.parentNode)this.parentNode.children.pop()}, get lastElementChild(){return this.children[this.children.length-1]}});
  ['start','pause','reset','kill0','lease','batch','leaseOut','batchOut','clock','queueLabel','jobs','events','mPending','mLeased','mDone','mRequeued','pill0','pill1','work0','work1','bar0','bar1'].forEach(id => els.set(id, make(id)));
  const context = {document:{getElementById:id=>els.get(id),createElement:()=>make('node')}, setInterval:fn=>{intervalFn=fn;active=true;return 1}, clearInterval:()=>{active=false}, console};
  vm.runInNewContext(source, context); return {els, tick:()=>{if(active)intervalFn()}};
}
function run(sim, ticks) { for(let i=0;i<ticks;i++) sim.tick(); }

test('healthy workers renew a short lease and finish without requeue', () => { const sim=boot(2); sim.els.get('start').onclick(); run(sim, 80); assert.equal(Number(sim.els.get('mRequeued').textContent), 0); assert.equal(Number(sim.els.get('mDone').textContent), 18); });
test('killed worker cannot acknowledge and its lease is reclaimed', () => { const sim=boot(2); sim.els.get('start').onclick(); sim.tick(); sim.els.get('kill0').onclick(); const before=Number(sim.els.get('mDone').textContent); run(sim, 5); assert.equal(Number(sim.els.get('mDone').textContent), before); run(sim, 10); assert(Number(sim.els.get('mRequeued').textContent)>0); run(sim, 80); assert.equal(Number(sim.els.get('mDone').textContent), 18); });
test('pause freezes virtual time and reset restores initial state', () => { const sim=boot(); sim.els.get('start').onclick(); run(sim, 3); sim.els.get('pause').onclick(); const clock=sim.els.get('clock').textContent; run(sim, 10); assert.equal(sim.els.get('clock').textContent, clock); sim.els.get('reset').onclick(); assert.equal(Number(sim.els.get('mPending').textContent),18); assert.equal(Number(sim.els.get('mDone').textContent),0); });
