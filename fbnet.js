/* Dino Battle — Firebase Realtime Database backend.
   Provides: window.FBNET = { ready:Promise<bool>, join(name), scores, plays, saves } */
(function(){
  const CFG={apiKey:"AIzaSyCvJtFo9QdF-FJP5Dyb0XYibIaC17dwm9k",authDomain:"dino-battle-51dd5.firebaseapp.com",projectId:"dino-battle-51dd5",storageBucket:"dino-battle-51dd5.firebasestorage.app",messagingSenderId:"474341003491",appId:"1:474341003491:web:e6ec111d014e634b82f5d6",
    databaseURL:"https://dino-battle-51dd5-default-rtdb.asia-southeast1.firebasedatabase.app"};
  const F={ok:false,db:null,err:''};
  function rid(n){const c='abcdefghijklmnopqrstuvwxyz0123456789';let s='';for(let i=0;i<(n||16);i++)s+=c[Math.floor(Math.random()*c.length)];return s;}
  const myPeer=rid(16);
  F.ready=new Promise(res=>{try{
    if(!window.firebase){F.err='sdk';res(false);return;}
    firebase.initializeApp(CFG);F.db=firebase.database();
    const c=F.db.ref('.info/connected');let done=false;
    c.on('value',s=>{F.connected=!!s.val();if(!done&&F.connected){done=true;F.ok=true;res(true);}});
    setTimeout(()=>{if(!done){done=true;F.ok=F.connected||false;res(F.ok);}},8000);
  }catch(e){F.err=String(e);res(false);}});
  function safeName(s){return String(s||'').trim().slice(0,10);}
  function nameKey(s){return safeName(s).replace(/[.#$\/\[\]]/g,'_')||'nanashi';}

  // ---- rooms: same shape the game already uses (emit/on/presence/peers/onPeers/leave) ----
  F.join=async function(name){
    if(!F.ok)throw {code:'not_granted'};
    name=String(name).toLowerCase().replace(/[^a-z0-9_.-]/g,'').slice(0,48);
    const base=F.db.ref('rooms/'+name);const peersRef=base.child('peers');const meRef=peersRef.child(myPeer);const evRef=base.child('ev');const stRef=base.child('st');
    let pres={};let presDirty=false,presTimer=null;let peerSnap=[];const peersSubs=[],evSubs={},stSubs=[];let alive=true;
    const joinedAt=Date.now();
    await meRef.set({p:{},t:firebase.database.ServerValue.TIMESTAMP});meRef.onDisconnect().remove();
    base.child('alive').set(firebase.database.ServerValue.TIMESTAMP);
    function flushPres(){presTimer=null;if(!presDirty||!alive)return;presDirty=false;meRef.update({p:pres,t:firebase.database.ServerValue.TIMESTAMP}).catch(()=>{});}
    function mk(id,v,prev){const pr=(v&&v.p)||{};const old=prev&&prev.get(id);const same=old&&JSON.stringify(old.presence)===JSON.stringify(pr);return same?old:{peer:id,by:null,isMe:id===myPeer,sameTab:id===myPeer,kind:'viewer',guest:false,presence:Object.freeze(pr),updatedAt:Date.now()};}
    const prevMap=new Map();
    peersRef.on('value',s=>{if(!alive)return;const v=s.val()||{};const next=[];const nm=new Map();const joined=[],updated=[],left=[];
      Object.keys(v).forEach(id=>{const p=mk(id,v[id],prevMap);next.push(p);nm.set(id,p);if(!prevMap.has(id))joined.push(p);else if(prevMap.get(id)!==p)updated.push(p);});
      prevMap.forEach((p,id)=>{if(!nm.has(id))left.push(p);});
      prevMap.clear();nm.forEach((p,id)=>prevMap.set(id,p));peerSnap=Object.freeze(next);
      const ch={peers:peerSnap,joined,left,updated};peersSubs.forEach(fn=>{try{fn(ch);}catch(e){}});});
    evRef.orderByChild('t').startAt(joinedAt-2000).on('child_added',s=>{if(!alive)return;const m=s.val();if(!m||!m.topic)return;const msg={peer:m.peer,by:null,isMe:m.peer===myPeer,sameTab:m.peer===myPeer,kind:'viewer',guest:false,topic:m.topic,data:m.data};
      (evSubs[m.topic]||[]).forEach(fn=>{try{fn(msg);}catch(e){}});});
    stRef.on('value',s=>{if(!alive)return;const m=s.val();if(!m||!m.data)return;const msg={peer:m.peer,by:null,isMe:m.peer===myPeer,sameTab:m.peer===myPeer,kind:'viewer',guest:false,topic:'st',data:m.data};(evSubs['st']||[]).forEach(fn=>{try{fn(msg);}catch(e){}});});
    const room={name,
      emit(topic,data){if(!alive)return Promise.resolve();if(topic==='st'){return stRef.set({peer:myPeer,data:data===undefined?null:data,t:firebase.database.ServerValue.TIMESTAMP}).catch(()=>{});}
        const r=evRef.push();return r.set({peer:myPeer,topic,data:data===undefined?null:data,t:firebase.database.ServerValue.TIMESTAMP}).then(()=>{setTimeout(()=>{r.remove().catch(()=>{});},6000);}).catch(()=>{});},
      on(topic,fn){(evSubs[topic]=evSubs[topic]||[]).push(fn);return()=>{const a=evSubs[topic]||[];const i=a.indexOf(fn);if(i>=0)a.splice(i,1);};},
      presence(patch){Object.keys(patch||{}).forEach(k=>{if(patch[k]===null)delete pres[k];else pres[k]=patch[k];});presDirty=true;if(!presTimer)presTimer=setTimeout(flushPres,70);return Promise.resolve();},
      peers(){return peerSnap;},
      onPeers(fn){peersSubs.push(fn);setTimeout(()=>{if(alive)fn({peers:peerSnap,joined:peerSnap,left:[],updated:[]});},0);return()=>{const i=peersSubs.indexOf(fn);if(i>=0)peersSubs.splice(i,1);};},
      connected(){return !!F.connected;},
      onConnection(fn){const r=F.db.ref('.info/connected');const h=s=>fn(!!s.val());r.on('value',h);return()=>r.off('value',h);},
      async leave(){if(!alive)return;alive=false;peersRef.off();evRef.off();stRef.off();try{await meRef.remove();const s=await peersRef.once('value');if(!s.exists())await base.remove();}catch(e){}}
    };
    return room;
  };

  // ---- scores, plays, saves ----
  F.submitScore=async function(name,score,wave,kind){if(!F.ok)return false;name=safeName(name);if(!name||!(score>0))return false;const k=nameKey(name);
    const ref=F.db.ref('scores/'+k);await ref.transaction(cur=>{if(cur&&cur.score>=score)return cur;return {name,score:Math.round(score),wave:Math.round(wave),kind:String(kind||''),t:Date.now()};});
    F.db.ref('players/'+k).transaction(cur=>{cur=cur||{name,plays:0,best:0,total:0};cur.name=name;cur.plays=(cur.plays||0)+1;cur.best=Math.max(cur.best||0,Math.round(score));cur.total=(cur.total||0)+Math.round(score);cur.last=Date.now();cur.lastKind=String(kind||'');return cur;}).catch(()=>{});
    F.db.ref('plays').push({name,score:Math.round(score),wave:Math.round(wave),kind:String(kind||''),t:Date.now()}).catch(()=>{});return true;};
  F.seedScores=async function(list){if(!F.ok)return;for(const r of (list||[])){try{const name=safeName(r.name);if(!name)continue;await F.db.ref('scores/'+nameKey(name)).transaction(cur=>{if(cur&&cur.score>=r.score)return cur;return {name,score:Math.round(r.score),wave:Math.round(r.wave||0),kind:String(r.kind||''),t:r.t||Date.now(),legacy:true};});}catch(e){}}};
  F.watchTop=function(n,fn){if(!F.ok)return()=>{};const q=F.db.ref('scores').orderByChild('score').limitToLast(n||10);const h=s=>{const rows=[];s.forEach(c=>{rows.push(c.val());});rows.sort((a,b)=>b.score-a.score);fn(rows);};q.on('value',h);return()=>q.off('value',h);};
  F.cloudSave=async function(name,pin,data){if(!F.ok)return false;name=safeName(name);pin=String(pin||'').replace(/\D/g,'').slice(0,4);if(!name||pin.length!==4)return false;await F.db.ref('saves/'+nameKey(name)+'_'+pin).set({data,t:Date.now()});return true;};
  F.cloudLoad=async function(name,pin){if(!F.ok)return null;name=safeName(name);pin=String(pin||'').replace(/\D/g,'').slice(0,4);if(!name||pin.length!==4)return null;const s=await F.db.ref('saves/'+nameKey(name)+'_'+pin).once('value');const v=s.val();return v&&v.data||null;};
  F.cloudClear=async function(name,pin){if(!F.ok)return;name=safeName(name);pin=String(pin||'').replace(/\D/g,'').slice(0,4);if(!name||pin.length!==4)return;await F.db.ref('saves/'+nameKey(name)+'_'+pin).remove();};
  window.FBNET=F;
})();
