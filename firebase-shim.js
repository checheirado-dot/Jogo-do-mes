// HaxCareer + Firebase: faz o ranking mundial e as salas online funcionarem fora do claude.ai.
// O jogo chama claude.use('db' | 'user' | 'room'); aqui eu respondo a essas chamadas com o Firebase.
//
// MULTIPLAYER (v2): o Firebase agora é só a "ponte" e a reserva.
//  - Os jogadores se conectam direto (WebRTC, canal de dados sem ordem e sem retransmissão),
//    que é o caminho mais rápido possível: sem passar pelo servidor dos EUA a cada quadro.
//  - A troca dos códigos de conexão (sinalização) é feita pelo Firebase, em rooms/<sala>/sig.
//  - Se o P2P não abrir (rede bloqueando), o jogo continua pelo Firebase, mas com fila
//    limitada: mensagens velhas são descartadas em vez de se acumularem (era isso que travava).
//  - Cada mensagem tem um número de sequência; chegou algo mais velho que o último recebido, é ignorado.
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.14.1/firebase-app.js";
import {
  getDatabase, ref, set, remove, onValue, onChildAdded, onChildChanged, onChildRemoved,
  onDisconnect, query, orderByChild, limitToLast
} from "https://www.gstatic.com/firebasejs/10.14.1/firebase-database.js";

const firebaseConfig = {
  apiKey: "AIzaSyDEFw0M28Imap9OhVEdO8CMpOyn_BVUjYg",
  authDomain: "haxcarrer.firebaseapp.com",
  databaseURL: "https://haxcarrer-default-rtdb.firebaseio.com",
  projectId: "haxcarrer",
  storageBucket: "haxcarrer.firebasestorage.app",
  messagingSenderId: "630297358259",
  appId: "1:630297358259:web:d03527e8d9f1bb8ee2e5c6"
};

const app = initializeApp(firebaseConfig);
const db = getDatabase(app);

const rnd = () => Math.random().toString(36).slice(2, 12);
// id fixo do aparelho (ranking) e id da sessão (salas online)
const uid = (() => {
  try {
    let u = localStorage.getItem("hax.uid");
    if (!u) { u = "u" + rnd(); localStorage.setItem("hax.uid", u); }
    return u;
  } catch (e) { return "u" + rnd(); }
})();
const sid = "s" + rnd();
const clean = s => String(s).replace(/[^\w-]/g, "");

/* ---------- Ranking: pasta "rank" ---------- */
const dbApi = {
  collection(name) {
    let field = null, lim = 50;
    const q = {
      orderBy(f) { field = f; return q; },
      limit(n) { lim = n; return q; },
      onSnapshot(ok, fail) {
        const r = field
          ? query(ref(db, name), orderByChild(field), limitToLast(lim))
          : query(ref(db, name), limitToLast(lim));
        return onValue(r, snap => {
          const docs = [];
          snap.forEach(c => { const v = c.val(); docs.push({ id: c.key, data: () => v }); });
          docs.reverse(); // maior pontuação primeiro
          ok({ docs });
        }, err => { if (fail) fail(err); });
      }
    };
    return q;
  },
  doc(path) {
    return { set: obj => set(ref(db, path), obj) };
  }
};

/* ---------- Salas online ---------- */
const TYPES = ["hi", "lb", "in", "st"];
const FAST = { in: 1, st: 1 };              // tipos de alta frequência: vão por P2P quando possível
const RTC_OK = typeof RTCPeerConnection !== "undefined";
const RTC_CFG = {
  iceServers: [
    { urls: ["stun:stun.l.google.com:19302", "stun:stun1.l.google.com:19302", "stun:stun.cloudflare.com:3478"] },
    // retransmissor público e gratuito (reserva para redes de celular que bloqueiam P2P direto).
    // Se quiser mais estabilidade, troque por um TURN seu.
    { urls: ["turn:openrelay.metered.ca:80", "turn:openrelay.metered.ca:443", "turn:openrelay.metered.ca:443?transport=tcp"],
      username: "openrelayproject", credential: "openrelayproject" }
  ],
  iceCandidatePoolSize: 2
};
let seq = 0; // sequência única e crescente durante toda a vida da página

const gather = pc => new Promise(res => {
  if (pc.iceGatheringState === "complete") return res();
  const f = () => { if (pc.iceGatheringState === "complete") { pc.removeEventListener("icegatheringstatechange", f); res(); } };
  pc.addEventListener("icegatheringstatechange", f);
  setTimeout(res, 2500);
});

const roomApi = {
  async join(name) {
    const base = "rooms/" + clean(name);
    const H = {}, un = [];
    const peers = new Map();            // pid -> { pid, pc, dc, open, id, role, t0 }
    const tries = new Map();            // pid -> tentativas de P2P
    const seen = new Map();             // "peer|tipo" -> última sequência entregue
    const sigDone = new Map();          // pid -> último id de oferta tratado
    const hiSeen = new Set(), lbSeen = new Set();
    let sentLb = false, left = false, rtt = 0, lastFb = 0;
    const own = t => ref(db, base + "/" + t + "/" + sid);
    for (const t of TYPES) onDisconnect(own(t)).remove();
    onDisconnect(ref(db, base + "/sig/" + sid)).remove();

    /* entrega uma mensagem ao jogo, ignorando as que chegaram atrasadas */
    function deliver(peer, t, d, n) {
      if (n !== undefined && n !== null) {
        const k = peer + "|" + t;
        if (n <= (seen.get(k) || 0)) return;
        seen.set(k, n);
      }
      if (H[t]) H[t]({ data: d, isMe: false, peer });
    }

    /* ---------- P2P (WebRTC) ---------- */
    function closePeer(pid) {
      const P = peers.get(pid);
      if (!P) return;
      P.open = false;
      try { P.dc && P.dc.close(); } catch (e) {}
      try { P.pc && P.pc.close(); } catch (e) {}
      peers.delete(pid);
    }
    function attach(P, dc) {
      P.dc = dc;
      dc.onopen = () => { P.open = true; tries.delete(P.pid); };
      dc.onclose = () => { P.open = false; };
      dc.onerror = () => { P.open = false; };
      dc.onmessage = e => {
        let m; try { m = JSON.parse(e.data); } catch (x) { return; }
        if (m.t === "_p") { try { dc.send(JSON.stringify({ t: "_q", d: m.d })); } catch (x) {} return; }
        if (m.t === "_q") { const v = performance.now() - m.d; rtt = rtt ? rtt * 0.7 + v * 0.3 : v; return; }
        deliver(P.pid, m.t, m.d, m.n);
      };
      if (dc.readyState === "open") P.open = true;
    }
    function watch(P) {
      P.pc.onconnectionstatechange = () => {
        const s = P.pc.connectionState;
        if (s === "failed" || s === "closed" || s === "disconnected") P.open = false;
      };
    }
    async function dial(pid) { // quem vê a sala do anfitrião liga para ele
      if (left || !RTC_OK) return;
      closePeer(pid);
      const id = rnd();
      let pc;
      try { pc = new RTCPeerConnection(RTC_CFG); } catch (e) { return; }
      const P = { pid, pc, dc: null, open: false, id, role: "o", t0: performance.now(), got: false };
      peers.set(pid, P);
      watch(P);
      attach(P, pc.createDataChannel("hx", { ordered: false, maxRetransmits: 0 }));
      try {
        await pc.setLocalDescription(await pc.createOffer());
        await gather(pc);
        if (left || peers.get(pid) !== P) return;
        await set(ref(db, base + "/sig/" + pid + "/" + sid), { k: "o", id, s: pc.localDescription.sdp, t: Date.now() });
      } catch (e) { if (peers.get(pid) === P) closePeer(pid); }
    }
    function ensure(pid) { // chamado a cada sinal do anfitrião (1x por segundo no lobby)
      if (!RTC_OK || left) return;
      const P = peers.get(pid);
      if (P && (P.open || performance.now() - P.t0 < 9000)) return;
      const n = tries.get(pid) || 0;
      if (n >= 4) return;
      tries.set(pid, n + 1);
      dial(pid);
    }
    const sigRef = ref(db, base + "/sig/" + sid);
    const onSig = async s => {
      const from = s.key, v = s.val();
      if (!v || left || !RTC_OK) return;
      remove(s.ref).catch(() => {});
      if (v.k === "o") {
        if (sigDone.get(from) === v.id) return;
        sigDone.set(from, v.id);
        closePeer(from);
        let pc;
        try { pc = new RTCPeerConnection(RTC_CFG); } catch (e) { return; }
        const P = { pid: from, pc, dc: null, open: false, id: v.id, role: "a", t0: performance.now() };
        peers.set(from, P);
        watch(P);
        pc.ondatachannel = e => attach(P, e.channel);
        try {
          await pc.setRemoteDescription({ type: "offer", sdp: v.s });
          await pc.setLocalDescription(await pc.createAnswer());
          await gather(pc);
          if (left || peers.get(from) !== P) return;
          await set(ref(db, base + "/sig/" + from + "/" + sid), { k: "a", id: v.id, s: pc.localDescription.sdp, t: Date.now() });
        } catch (e) { if (peers.get(from) === P) closePeer(from); }
      } else if (v.k === "a") {
        const P = peers.get(from);
        if (!P || P.role !== "o" || P.id !== v.id || P.got) return;
        P.got = true;
        try { await P.pc.setRemoteDescription({ type: "answer", sdp: v.s }); } catch (e) {}
      }
    };
    un.push(onChildAdded(sigRef, onSig), onChildChanged(sigRef, onSig));
    const pingT = setInterval(() => {
      const msg = JSON.stringify({ t: "_p", d: performance.now() });
      for (const P of peers.values()) if (P.open) { try { P.dc.send(msg); } catch (e) {} }
    }, 2000);

    /* todos os que importam para mim estão conectados por P2P? */
    const rel = () => (sentLb ? hiSeen : lbSeen);
    function allOpen() {
      const R = rel();
      if (!R.size) return false;
      for (const id of R) { const P = peers.get(id); if (!P || !P.open) return false; }
      return true;
    }
    function anyOpen() { for (const P of peers.values()) if (P.open) return true; return false; }

    /* ---------- Reserva pelo Firebase: no máximo 2 gravações pendentes; o resto é trocado pelo dado mais novo ---------- */
    const fb = {};
    let warned = false;
    function fbSend(t, d, n) {
      const s = fb[t] || (fb[t] = { busy: 0, next: null });
      if (s.busy >= 2) { s.next = [d, n]; return Promise.resolve(); }
      s.busy++;
      return set(own(t), { d, n })
        .catch(err => { if (!warned) { warned = true; console.warn("Firebase:", err && err.code); } })
        .then(() => {
          s.busy--;
          if (s.next && s.busy < 2) { const x = s.next; s.next = null; fbSend(t, x[0], x[1]); }
        });
    }

    const listen = t => {
      const r = ref(db, base + "/" + t);
      const fn = s => {
        if (s.key === sid) return;
        const v = s.val();
        if (!v) return;
        if (t === "hi") hiSeen.add(s.key);
        else if (t === "lb") { lbSeen.add(s.key); ensure(s.key); }
        deliver(s.key, t, v.d, v.n);
      };
      un.push(onChildAdded(r, fn), onChildChanged(r, fn));
      if (t === "hi") un.push(onChildRemoved(r, s => hiSeen.delete(s.key)));
      if (t === "lb") un.push(onChildRemoved(r, s => lbSeen.delete(s.key)));
    };

    return {
      on(t, f) { if (!H[t]) listen(t); H[t] = f; },
      emit(t, d) {
        if (t === "lb") sentLb = true;
        const n = ++seq;
        if (FAST[t]) {
          let msg = null;
          for (const P of peers.values()) {
            if (!P.open || !P.dc || P.dc.bufferedAmount > 64000) continue;
            if (msg === null) msg = JSON.stringify({ t, d, n });
            try { P.dc.send(msg); } catch (e) {}
          }
          if (allOpen()) { // tudo por P2P: o servidor só recebe um sinal de vida de vez em quando
            const now = performance.now();
            if (now - lastFb < 700) return Promise.resolve();
            lastFb = now;
          }
          return fbSend(t, d, n);
        }
        return set(own(t), { d, n });
      },
      leave() {
        left = true;
        clearInterval(pingT);
        for (const f of un) { try { f(); } catch (e) {} }
        for (const pid of [...peers.keys()]) closePeer(pid);
        for (const t of TYPES) remove(own(t)).catch(() => {});
        remove(ref(db, base + "/sig/" + sid)).catch(() => {});
        if (sentLb) remove(ref(db, base)).catch(() => {}); // anfitrião saiu: apaga a sala
      },
      get fast() { return anyOpen(); },
      get mode() { return allOpen() ? "p2p" : anyOpen() ? "misto" : "servidor"; },
      get rtt() { return anyOpen() ? rtt : 0; }
    };
  },
  presence(p) {
    const r = ref(db, "lobby/" + sid);
    if (p && p.hx) {
      onDisconnect(r).remove();
      return set(r, { hx: p.hx, t: Date.now() });
    }
    return remove(r);
  },
  onPeers(cb) {
    return onValue(ref(db, "lobby"), snap => {
      const peers = [];
      snap.forEach(c => { peers.push({ isMe: c.key === sid, presence: c.val() }); });
      cb({ peers });
    });
  }
};

window.claude = {
  use: async name =>
    name === "db" ? dbApi :
    name === "user" ? { id: async () => uid } :
    name === "room" ? roomApi : null
};

/* ---------- O botão "Copiar link da sala" aponta para este site, não para o claude.ai ---------- */
const real = location.origin + location.pathname;
const fix = s => String(s).replace(/https:\/\/claude\.ai\/artifact\/[\w-]+/g, real);
try {
  if (navigator.share) {
    const o = navigator.share.bind(navigator);
    navigator.share = d => o(Object.assign({}, d, { url: d && d.url ? fix(d.url) : undefined }));
  }
  if (navigator.clipboard && navigator.clipboard.writeText) {
    const o = navigator.clipboard.writeText.bind(navigator.clipboard);
    navigator.clipboard.writeText = t => o(fix(t));
  }
} catch (e) {}
