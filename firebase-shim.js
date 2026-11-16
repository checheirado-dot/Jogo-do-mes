// HaxCareer + Firebase: faz o ranking mundial e as salas online funcionarem fora do claude.ai.
// O jogo chama claude.use('db' | 'user' | 'room'); aqui eu respondo a essas chamadas com o Firebase.
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.14.1/firebase-app.js";
import {
  getDatabase, ref, set, remove, onValue, onChildAdded, onChildChanged,
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

/* ---------- Salas online: pasta "rooms" (mensagens) e "lobby" (salas abertas) ---------- */
const TYPES = ["hi", "lb", "in", "st"];
const roomApi = {
  async join(name) {
    const base = "rooms/" + clean(name);
    const H = {}, un = [];
    let n = 0, sentLb = false;
    const own = t => ref(db, base + "/" + t + "/" + sid);
    for (const t of TYPES) onDisconnect(own(t)).remove();
    const listen = t => {
      const r = ref(db, base + "/" + t);
      const fn = s => {
        if (s.key === sid) return;
        const v = s.val();
        if (v && H[t]) H[t]({ data: v.d, isMe: false, peer: s.key });
      };
      un.push(onChildAdded(r, fn), onChildChanged(r, fn));
    };
    return {
      on(t, f) { if (!H[t]) listen(t); H[t] = f; },
      emit(t, d) {
        if (t === "lb") sentLb = true;
        n++;
        return set(own(t), { d, n });
      },
      leave() {
        for (const f of un) { try { f(); } catch (e) {} }
        for (const t of TYPES) remove(own(t)).catch(() => {});
        if (sentLb) remove(ref(db, base)).catch(() => {}); // anfitrião saiu: apaga a sala
      }
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
