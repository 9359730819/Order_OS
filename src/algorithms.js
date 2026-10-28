export function schedule(input, algorithm, quantum = 2) {
  const jobs = input.map((p, i) => ({ ...p, id: String(p.id || `P${i + 1}`), order: String(p.order || `ORD-${1001 + i}`), arrival: Math.max(0, +p.arrival || 0), burst: Math.max(1, +p.burst || 1), priority: +p.priority || 1, remaining: Math.max(1, +p.burst || 1), first: null, completion: null }));
  const gantt = []; let t = 0, done = 0, ready = [], cursor = 0;
  const sorted = [...jobs].sort((a,b)=>a.arrival-b.arrival || a.id.localeCompare(b.id));
  const push = (p,start,end) => { const prev=gantt.at(-1); if(prev?.id===p?.id && prev.end===start) prev.end=end; else gantt.push({id:p?.id||'Idle',order:p?.order||'CPU idle',start,end}); };
  while(done < jobs.length) {
    while(cursor<sorted.length && sorted[cursor].arrival<=t) ready.push(sorted[cursor++]);
    if(!ready.length) { const next=sorted[cursor]; if(!next) break; push(null,t,next.arrival); t=next.arrival; continue; }
    let p;
    if(algorithm==='RR') p=ready.shift();
    else { let idx=0; for(let i=1;i<ready.length;i++){let a=ready[i],b=ready[idx]; let better=algorithm==='SJF'?a.burst<b.burst:algorithm==='Priority'?a.priority<b.priority:a.arrival<b.arrival; if(better || ((algorithm==='SJF'&&a.burst===b.burst)||(algorithm==='Priority'&&a.priority===b.priority)||(algorithm==='FCFS'&&a.arrival===b.arrival) && a.id.localeCompare(b.id)<0)) idx=i; } p=ready.splice(idx,1)[0]; }
    if(p.first===null) p.first=t;
    const run=algorithm==='RR'?Math.min(+quantum||1,p.remaining):p.remaining, end=t+run; push(p,t,end); p.remaining-=run;
    while(cursor<sorted.length && sorted[cursor].arrival<=end) ready.push(sorted[cursor++]);
    t=end;
    if(p.remaining<=0){p.completion=t;done++;} else ready.push(p);
  }
  const rows=jobs.map(p=>({...p,turnaround:p.completion-p.arrival,waiting:p.completion-p.arrival-p.burst,response:p.first-p.arrival}));
  const busy=rows.reduce((s,p)=>s+p.burst,0), total=t;
  const avg=k=>rows.reduce((s,p)=>s+p[k],0)/(rows.length||1);
  return {rows,gantt,avgWaiting:avg('waiting'),avgTurnaround:avg('turnaround'),avgResponse:avg('response'),utilization:total?busy/total*100:0,throughput:total?rows.length/total:0,totalTime:total};
}

export function bankers(allocation, maximum, available) {
 const n=allocation.length, m=available.length, need=maximum.map((row,i)=>row.map((x,j)=>x-allocation[i][j]));
 let work=[...available], finish=Array(n).fill(false), sequence=[], steps=[];
 while(sequence.length<n){let found=false; for(let i=0;i<n;i++) if(!finish[i]&&need[i].every((v,j)=>v<=work[j])){const before=[...work];work=work.map((v,j)=>v+allocation[i][j]);finish[i]=true;sequence.push(`P${i}`);steps.push({process:`P${i}`,need:[...need[i]],before,after:[...work]});found=true;break;} if(!found)break;}
 return {need, safe:finish.every(Boolean),sequence,steps,work};
}

export function allocateMemory(partitions, processes, strategy) {
 const free=partitions.map((size,i)=>({id:i+1,size:+size,process:null})); const results=[];
 for(const proc of processes){const fit=free.filter(p=>p.process===null&&p.size>=proc.size); let chosen=null; if(fit.length){chosen=strategy==='Best Fit'?fit.reduce((a,b)=>a.size<=b.size?a:b):strategy==='Worst Fit'?fit.reduce((a,b)=>a.size>=b.size?a:b):fit[0];chosen.process=proc.id;} results.push({process:proc.id,required:proc.size,partition:chosen?.id??null,waste:chosen?chosen.size-proc.size:0});}
 const used=free.filter(p=>p.process);return {partitions:free,results,allocated:used.length,freeMemory:free.filter(p=>!p.process).reduce((s,p)=>s+p.size,0),internal:used.reduce((s,p)=>s+p.size-processes.find(x=>x.id===p.process).size,0),utilization:free.reduce((s,p)=>s+p.size,0)?used.reduce((s,p)=>s+processes.find(x=>x.id===p.process).size,0)/free.reduce((s,p)=>s+p.size,0)*100:0};
}

export function semaphoreEvents(capacity, orders, consumptionDelay=2, scenario='steady') {
 const buffer=[], producerQueue=[], events=[];
 const delay=Math.max(1,+consumptionDelay||1);
 let empty=capacity, full=0, mutex=1, generated=0, produced=0, consumed=0;
 let consumerWaiting=false, nextConsume=Infinity, t=0;
 const sorted=[...orders].map((item,i)=>({...item,producer:item.producer||'P'+(i%3+1)})).sort((a,b)=>a.time-b.time||a.id.localeCompare(b.id));
 const log=(time,message,type='info')=>events.push({time,message,type,buffer:[...buffer],empty,full,mutex,generated,produced,consumed,waitingProducers:producerQueue.length});
 if(scenario==='empty'){consumerWaiting=true;log(0,'Consumer calls wait(full): full=0, buffer empty. It blocks until an order arrives.','blocked');}
 const insert=(item,time,fromQueue=false)=>{
  const producer='Producer '+item.producer;
  if(!fromQueue){generated++;log(time,producer+' receives '+item.id+' and calls wait(empty).','arrive');}
  if(empty===0){producerQueue.push(item);log(time,'wait(empty) blocks '+producer+'; BUFFER FULL. Request waits for signal(empty).','blocked');return false;}
  empty--;
  log(time,producer+' wait(empty) succeeds; empty='+empty+'.','semaphore');
  mutex=0;
  log(time,producer+' acquires mutex=0; exclusive buffer update begins.','semaphore');
  buffer.push(item.id);full++;produced++;
  mutex=1;
  log(time,producer+' inserts '+item.id+'; signal(mutex), signal(full). empty='+empty+', full='+full+'.','produce');
  if(consumerWaiting){consumerWaiting=false;log(time,'signal(full) wakes the consumer waiting on the empty buffer.','semaphore');}
  if(nextConsume===Infinity)nextConsume=time+delay;
  return true;
 };
 const consume=time=>{
  if(full===0)return;
  full--;mutex=0;
  log(time,'Consumer wait(full) succeeds; wait(mutex) succeeds. mutex=0.','semaphore');
  const id=buffer.shift();consumed++;
  log(time,'Consumer removes '+id+' from the FIFO head.','consume');
  mutex=1;empty++;
  log(time,'Consumer signal(mutex), signal(empty). empty='+empty+', full='+full+'.','semaphore');
  if(producerQueue.length){
   const waiting=producerQueue.shift();
   log(time,'signal(empty) wakes Producer '+waiting.producer+'; it retries the buffer insertion.','semaphore');
   insert(waiting,time,true);
  }
  nextConsume=full>0?time+delay:Infinity;
 };
 let cursor=0;
 while(cursor<sorted.length){
  const arrivalTime=sorted[cursor].time;
  while(full>0&&nextConsume<=arrivalTime){
   t=nextConsume;consume(t);
   if(full===0&&cursor<sorted.length&&sorted[cursor].time>t&&!consumerWaiting){consumerWaiting=true;log(t,'Consumer calls wait(full): buffer empty while the next order has not arrived.','blocked');}
  }
  t=arrivalTime;
  let end=cursor+1;while(end<sorted.length&&sorted[end].time===arrivalTime)end++;
  const batch=sorted.slice(cursor,end);
  if(scenario==='race'&&batch.length>1)log(t,batch.map(item=>'Producer '+item.producer).join(', ')+' arrive together and contend for shared buffer slots. mutex serializes their critical sections.','race');
  if(full===0&&cursor>0&&!consumerWaiting) {consumerWaiting=true;log(t,'Consumer calls wait(full): buffer empty; waiting for signal(full).','blocked');}
  for(const item of batch)insert(item,t,false);
  cursor=end;
 }
 while(full>0||producerQueue.length){if(nextConsume===Infinity)nextConsume=t+delay;t=nextConsume;consume(t);}
 const safe=generated===orders.length&&produced===orders.length&&consumed===orders.length&&buffer.length===0&&producerQueue.length===0&&mutex===1;
 return {events,buffer:[...buffer],empty,full,mutex,generated:orders.length,produced,consumed,pending:producerQueue.length,lostOrDuplicated:orders.length-consumed,safe,raceProtected:scenario==='race'&&safe};
}
