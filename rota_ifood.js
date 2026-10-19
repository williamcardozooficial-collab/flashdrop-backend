'use strict';
// ===== ROTA iFOOD COM VARIOS PONTOS (entrega propria) =====
// Plano: "PENDENTE - Rota iFood com varios pontos.md" (13/10/2026).
// Cada entrega da rota e um pedido iFood normal, ligado aos outros por rota_id (id da 1a entrega) e rota_ordem
// (1..N, a ordem que a loja colocou). Pedidos sem rota_id nao passam por nada deste arquivo: com a rota desligada no
// admin (settings.rota_ifood_ativa = false) ninguem consegue criar rota e o sistema funciona exatamente como antes.
//
// Status usados (alem dos normais): 'rota_espera' = entregas 2..N de uma rota disponivel (o app so mostra 'pendente',
// entao o motoboy ve UMA tele so: a 1a entrega, com os bairros e o total). Ao aceitar a 1a, o servidor passa todas
// para o mesmo motoboy.
module.exports = function instalarRotaIfood(ctx) {
  const { app, pool, axios, bot, PORT, ADMIN_ID } = ctx;
  const STATUS_ATIVOS = ['aceito', 'na_loja', 'coletado', 'no_cliente'];
  const ANTES_DA_COLETA = ['em_preparo', 'rota_espera', 'pendente', 'aceito', 'na_loja'];
  const r2 = (v) => Math.round((parseFloat(v) || 0) * 100) / 100;
  const soma = (lista, campo) => r2(lista.reduce((a, p) => a + (parseFloat(p[campo]) || 0), 0));
  const fmtR = (v) => 'R$ ' + r2(v).toFixed(2).replace('.', ',');
  const fmtKm = (v) => (Math.round((parseFloat(v) || 0) * 10) / 10).toFixed(1).replace('.', ',') + ' km';
  const ceil = (v) => Math.ceil(v - 1e-9);
  function erro(msg, status, extra) { const e = new Error(msg); e.status = status || 400; e.extra = extra || null; return e; }
  // Envolve as rotas: erro conhecido vira resposta com a mensagem; erro inesperado vira 500 (e fica no log)
  const h = (fn) => async (req, res) => {
    try { await fn(req, res); }
    catch (e) { if (!e.status) console.error('[ROTA] Erro:', e.stack || e.message); if (!res.headersSent) res.status(e.status || 500).json(Object.assign({ error: e.status ? e.message : ('Erro na rota iFood: ' + e.message) }, e.extra || {})); }
  };

  // ---------- banco ----------
  async function criarColunas() {
    const qs = [
      'ALTER TABLE settings ADD COLUMN IF NOT EXISTS rota_ifood_ativa BOOLEAN DEFAULT false',
      'ALTER TABLE settings ADD COLUMN IF NOT EXISTS rota_valor_ponto DECIMAL DEFAULT 3',
      'ALTER TABLE settings ADD COLUMN IF NOT EXISTS rota_max_pontos INTEGER DEFAULT 4',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_id INTEGER',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_ordem INTEGER',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_pronto BOOLEAN DEFAULT false',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_nao_atende_em TIMESTAMP',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_nao_atende_admin BOOLEAN DEFAULT false',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_decisao VARCHAR(20)',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_decisao_por VARCHAR(100)',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_decisao_em TIMESTAMP',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_retorno VARCHAR(20)',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_retorno_valor DECIMAL',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_retorno_chegou_em TIMESTAMP',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_retorno_finalizado_em TIMESTAMP',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_retorno_finalizado_por VARCHAR(100)',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_socorro BOOLEAN DEFAULT false',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_socorro_de INTEGER',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_socorro_pedido_em TIMESTAMP',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_socorro_info TEXT',
      'ALTER TABLE users ADD COLUMN IF NOT EXISTS socorros_count INTEGER DEFAULT 0',
      'CREATE INDEX IF NOT EXISTS idx_orders_rota_id ON orders(rota_id)',
      // Formula nova do preco (13/10): o valor por ponto passa a R$ 3,00 UMA vez (depois continua ajustavel no admin)
      'ALTER TABLE settings ADD COLUMN IF NOT EXISTS rota_preco_v2 BOOLEAN DEFAULT false',
      // Gatilho automatico do "cliente nao atende" (o app Android nao tem o botao): minutos em no_cliente sem finalizar
      'ALTER TABLE settings ADD COLUMN IF NOT EXISTS rota_nao_atende_auto_min INTEGER DEFAULT 15',
      // Padrao do William: 15 min (passa de 10 para 15 UMA vez; depois continua ajustavel no admin)
      'ALTER TABLE settings ADD COLUMN IF NOT EXISTS rota_nao_atende_v15 BOOLEAN DEFAULT false',
      'UPDATE settings SET rota_nao_atende_auto_min=15, rota_nao_atende_v15=true WHERE id=1 AND rota_nao_atende_v15 IS NOT TRUE',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_nao_atende_auto_min INTEGER',
      // Prazo da LOJA para decidir (contador do alerta), separado do gatilho: padrao 5 min. Cada pedido guarda o prazo que valeu
      'ALTER TABLE settings ADD COLUMN IF NOT EXISTS rota_prazo_loja_min INTEGER DEFAULT 5',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS rota_nao_atende_prazo_min INTEGER',
      'UPDATE settings SET rota_valor_ponto=3, rota_preco_v2=true WHERE id=1 AND rota_preco_v2 IS NOT TRUE'
    ];
    for (const q of qs) { try { await pool.query(q); } catch (e) { console.error('[ROTA] coluna:', e.message); } }
  }
  async function cfg() { const r = await pool.query('SELECT * FROM settings WHERE id=1'); return r.rows[0] || {}; }
  const rotaLigada = (c) => !!(c && c.rota_ifood_ativa === true);
  function valorPonto(c) { const v = parseFloat(c.rota_valor_ponto); return isNaN(v) || v < 0 ? 3 : v; }
  function prazoLoja(c) { const v = parseInt(c.rota_prazo_loja_min); return isNaN(v) || v < 1 ? 5 : Math.min(v, 60); }
  function minNaoAtendeAuto(c) { const v = parseInt(c.rota_nao_atende_auto_min); return isNaN(v) || v < 1 ? 15 : Math.min(v, 120); }
  function maxPontos(c) { const v = parseInt(c.rota_max_pontos); return isNaN(v) || v < 2 ? 4 : Math.min(v, 10); }
  async function pedidosDaRota(rotaId, db) { const r = await (db || pool).query('SELECT * FROM orders WHERE rota_id=$1 ORDER BY rota_ordem NULLS LAST, id', [rotaId]); return r.rows; }
  const vivos = (ps) => ps.filter((p) => p.status !== 'cancelado').sort((a, b) => (a.rota_ordem || 99) - (b.rota_ordem || 99));
  async function pedido(id) { const r = await pool.query('SELECT * FROM orders WHERE id=$1', [parseInt(id) || 0]); return r.rows[0] || null; }

  // ---------- preco (mesma formula da tele normal da loja) ----------
  function tarifaKm(c, km) {
    const lim = parseFloat(c.km_limite_faixa) || 0, acima = parseFloat(c.price_per_km_acima) || 0;
    return (lim > 0 && acima > 0 && km > lim) ? acima : (parseFloat(c.price_per_km) || 2.5);
  }
  // Preco da rota (decisao do William, 13/10): arrancada + km TOTAL (soma dos trechos, na ordem da loja) x tarifa do km
  // (faixa acima do limite como na tele normal, pelo km total) + valor por ponto para cada cliente a partir do 2o.
  // Sem minimo por trecho: o minimo da tele normal vale so para o total. Arredonda uma vez, no total, para cima ao real.
  function precoRota(c, kmTotal, n) {
    const min = parseFloat(c.min_fee) || 8, arr = parseFloat(c.arrancada) || 5;
    return ceil(Math.max(min, arr + kmTotal * tarifaKm(c, kmTotal) + valorPonto(c) * Math.max(0, n - 1)));
  }
  function comissaoDe(c, total) {
    const raw = parseFloat(c.commission) || 0;
    const v = c.commission_type === 'percentual' ? Math.max(total * raw / 100, parseFloat(c.commission_min) || 0) : raw;
    return r2(Math.min(v, total));
  }
  // Divide igual entre as entregas, em centavos (os centavos que sobram vao para as primeiras)
  function dividir(total, n) { const c = Math.round(total * 100), base = Math.floor(c / n), resto = c - base * n; const out = []; for (let i = 0; i < n; i++) out.push((base + (i < resto ? 1 : 0)) / 100); return out; }
  // Taxa noturna e de chuva: mesma regra do POST /orders, cobradas UMA vez por rota
  function taxasExtrasAgora(c) {
    let noturna = 0, chuva = 0;
    if (c.taxa_noturna_ativa && parseFloat(c.taxa_noturna) > 0) {
      const brNow = new Date(Date.now() - 3 * 60 * 60 * 1000); const nowH = brNow.toISOString().slice(11, 16);
      const dowHoje = brNow.getUTCDay(), dowOntem = (dowHoje + 6) % 7;
      let sched = null; try { sched = c.taxa_noturna_horarios ? JSON.parse(c.taxa_noturna_horarios) : null; } catch (e) { sched = null; }
      let not = false;
      if (sched) {
        const hj = sched[dowHoje], on = sched[dowOntem];
        if (hj && hj.inicio && hj.fim) { if (hj.inicio <= hj.fim) { if (nowH >= hj.inicio && nowH < hj.fim) not = true; } else if (nowH >= hj.inicio) not = true; }
        if (!not && on && on.inicio && on.fim && on.inicio > on.fim && nowH < on.fim) not = true;
      } else {
        const ini = c.taxa_noturna_hora_inicio || '22:00', fim = c.taxa_noturna_hora_fim || '06:00';
        not = ini > fim ? (nowH >= ini || nowH < fim) : (nowH >= ini && nowH < fim);
      }
      if (not) noturna = parseFloat(c.taxa_noturna);
    }
    if (c.taxa_chuva_ativa && parseFloat(c.taxa_chuva) > 0) chuva = parseFloat(c.taxa_chuva);
    return { noturna: r2(noturna), chuva: r2(chuva) };
  }

  // ---------- mapas (Google) ----------
  const chaveMapas = () => process.env.GOOGLE_MAPS_KEY;
  const temCoord = (p) => p && p.lat !== null && p.lat !== undefined && p.lat !== '' && p.lng !== null && p.lng !== undefined && p.lng !== '' && !isNaN(parseFloat(p.lat)) && !isNaN(parseFloat(p.lng));
  async function geocodificar(endereco) {
    if (!chaveMapas()) throw erro('Serviço de mapas indisponível no momento.', 503);
    const r = await axios.get('https://maps.googleapis.com/maps/api/geocode/json?address=' + encodeURIComponent(endereco) + '&key=' + chaveMapas(), { timeout: 10000 });
    const d = r.data || {};
    if (d.status !== 'OK' || !d.results || !d.results.length) throw erro('Endereço não encontrado: ' + endereco);
    const g = d.results[0], tipo = g.geometry && g.geometry.location_type;
    if ((tipo === 'GEOMETRIC_CENTER' || tipo === 'APPROXIMATE') && !(g.address_components || []).some((x) => (x.types || []).includes('street_number'))) throw erro('Endereço impreciso: informe a rua e o número (' + endereco + ').');
    return { lat: g.geometry.location.lat, lng: g.geometry.location.lng };
  }
  async function enderecoAproximado(lat, lng) {
    try { if (!chaveMapas()) return null; const r = await axios.get('https://maps.googleapis.com/maps/api/geocode/json?latlng=' + lat + ',' + lng + '&key=' + chaveMapas(), { timeout: 8000 }); const d = r.data || {}; return d.status === 'OK' && d.results && d.results[0] ? d.results[0].formatted_address : null; } catch (e) { return null; }
  }
  // km pela rota do Google de cada origem[i] ate destino[i] (uma chamada so, pega a diagonal da matriz)
  async function trechosKm(origens, destinos) {
    if (!chaveMapas()) throw erro('Serviço de mapas indisponível no momento.', 503);
    const txt = (p) => parseFloat(p.lat) + ',' + parseFloat(p.lng);
    const r = await axios.get('https://maps.googleapis.com/maps/api/distancematrix/json?origins=' + encodeURIComponent(origens.map(txt).join('|')) + '&destinations=' + encodeURIComponent(destinos.map(txt).join('|')) + '&mode=driving&key=' + chaveMapas(), { timeout: 10000 });
    const d = r.data || {};
    if (d.status !== 'OK' || !d.rows) throw erro('Não foi possível calcular a rota agora. Tente de novo.', 502);
    return destinos.map((_, i) => { const el = d.rows[i] && d.rows[i].elements && d.rows[i].elements[i]; if (!el || el.status !== 'OK') throw erro('Não foi possível calcular o trecho até a ' + (i + 1) + 'ª entrega.'); return Math.round(el.distance.value / 10) / 100; });
  }
  // ---------- ordem mais curta ----------
  // km pela rota do Google entre todos os pares (origem + clientes), com cache de 6 h por par de coordenadas
  const _cacheKm = new Map(); const CACHE_KM_MS = 6 * 60 * 60 * 1000;
  const chaveCoord = (p) => (+p.lat).toFixed(5) + ',' + (+p.lng).toFixed(5);
  async function matrizKm(locais) {
    const n = locais.length, M = locais.map(() => new Array(n).fill(null)); let falta = false; const agora = Date.now();
    for (let i = 0; i < n; i++) for (let j = 1; j < n; j++) { if (i === j) continue; const k = chaveCoord(locais[i]) + '|' + chaveCoord(locais[j]); const c2 = _cacheKm.get(k); if (c2 && agora - c2.em < CACHE_KM_MS) M[i][j] = c2.km; else falta = true; }
    if (falta) {
      if (!chaveMapas()) throw erro('Serviço de mapas indisponível no momento.', 503);
      const txt = (p) => parseFloat(p.lat) + ',' + parseFloat(p.lng);
      const r = await axios.get('https://maps.googleapis.com/maps/api/distancematrix/json?origins=' + encodeURIComponent(locais.map(txt).join('|')) + '&destinations=' + encodeURIComponent(locais.slice(1).map(txt).join('|')) + '&mode=driving&key=' + chaveMapas(), { timeout: 10000 });
      const d = r.data || {}; if (d.status !== 'OK' || !d.rows) throw erro('Não foi possível calcular a rota agora. Tente de novo.', 502);
      for (let i = 0; i < n; i++) for (let j = 1; j < n; j++) { if (i === j) continue; const el = d.rows[i] && d.rows[i].elements && d.rows[i].elements[j - 1]; if (!el || el.status !== 'OK') continue; const km = Math.round(el.distance.value / 10) / 100; M[i][j] = km; _cacheKm.set(chaveCoord(locais[i]) + '|' + chaveCoord(locais[j]), { km, em: agora }); }
      if (_cacheKm.size > 5000) _cacheKm.clear();
    }
    return M;
  }
  function permutacoes(arr) { if (arr.length <= 1) return [arr.slice()]; const out = []; arr.forEach((x, i) => { permutacoes(arr.slice(0, i).concat(arr.slice(i + 1))).forEach((p) => out.push([x].concat(p))); }); return out; }
  // Ordem dos clientes com menor km total saindo da loja (termina no ultimo cliente). Empate: fica a ordem da loja.
  // Devolve { ordem: indices na lista recebida, km_atual, km_otimo }
  async function ordemMaisCurta(origem, pontos) {
    const n = pontos.length; const ident = pontos.map((_, i) => i);
    if (n < 2 || n > 6) return { ordem: ident, km_atual: null, km_otimo: null };
    const M = await matrizKm([origem].concat(pontos));
    const kmDe = (ord) => { let t = 0, ant = 0; for (const i of ord) { const v = M[ant][i + 1]; if (v == null) return Infinity; t += v; ant = i + 1; } return r2(t); };
    const kmAtual = kmDe(ident); let melhor = ident, kmMelhor = kmAtual;
    permutacoes(ident).forEach((p) => { const k = kmDe(p); if (k < kmMelhor - 0.01) { melhor = p; kmMelhor = k; } });
    return { ordem: melhor, km_atual: isFinite(kmAtual) ? kmAtual : null, km_otimo: isFinite(kmMelhor) ? kmMelhor : null };
  }
  // Cotacao: origem -> p1 -> p2 ... (na ordem da loja). extrasFixos: no recalculo usa as taxas da criacao.
  async function cotar(c, origem, pontos, extrasFixos) {
    const kms = await trechosKm([origem].concat(pontos.slice(0, -1)), pontos);
    const kmTotal = r2(kms.reduce((a, b) => a + b, 0));
    const base = precoRota(c, kmTotal, kms.length);
    const ext = extrasFixos || taxasExtrasAgora(c);
    const total = r2(base + ext.noturna + ext.chuva); // noturna/chuva: uma vez por rota
    return { kms, base, extras: ext, total, comissao: comissaoDe(c, total), km_total: kmTotal };
  }
  // Valor de cada entrega: total e comissao divididos igual; ganho do motoboy = valor - comissao
  function porEntrega(cot) { const n = cot.kms.length, vt = dividir(cot.total, n), cm = dividir(cot.comissao, n); return vt.map((v, i) => ({ valor_total: v, comissao: cm[i], valor_motoboy: r2(v - cm[i]), distancia: cot.kms[i] })); }

  // ---------- quem esta pedindo ----------
  function quem(req) {
    const adm = ctx.adminDe(req).user; if (adm) return { papel: 'admin', id: adm.id, nome: 'Flash Drop (admin)' };
    const lj = ctx.tokenDe(req, 'loja'); if (lj) return { papel: 'loja', id: lj.id, username: lj.username, nome: 'loja' };
    const mb = ctx.tokenDe(req, 'motoboy'); if (mb) return { papel: 'motoboy', id: mb.id, nome: 'motoboy' };
    return null;
  }
  // Rotas so do admin: recusa com o motivo (sem_token, token_expirado, token_invalido, nao_admin) para a tela explicar
  function exigirAdmin(req, msg) {
    const a = ctx.adminDe(req); if (a.user) return { papel: 'admin', id: a.user.id, nome: 'Flash Drop (admin)' };
    throw erro(msg, a.erro === 'nao_admin' ? 403 : 401, { motivo: a.erro });
  }
  function exigirLojaDona(q, ps) {
    if (!q || (q.papel !== 'loja' && q.papel !== 'admin')) throw erro('Faça login de novo na loja.', 401);
    if (!ps || !ps.length) throw erro('Rota não encontrada.', 404);
    if (q.papel === 'loja' && ps[0].loja_user !== q.username) throw erro('Esta rota não é da sua loja.', 403);
  }

  // ---------- avisos ----------
  async function pushMotoboy(motoboyId, titulo, corpo) {
    try {
      const t = await pool.query('SELECT push_token FROM users WHERE id=$1', [motoboyId]); const token = t.rows[0] && t.rows[0].push_token; if (!token) return;
      await axios.post('https://exp.host/--/api/v2/push/send', [{ to: token, title: titulo, body: corpo, sound: 'default', priority: 'high', data: { tipo: 'rota_aviso' } }], { headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, timeout: 10000 });
    } catch (e) { console.error('[ROTA] push motoboy:', e.message); }
  }
  function avisarAdmin(msg) { try { if (bot) bot.sendMessage(ADMIN_ID, msg).catch(() => {}); } catch (e) {} }
  async function avisarRotaDisponivel(rotaId, motivo) {
    try {
      const ps = vivos(await pedidosDaRota(rotaId)); if (!ps.length) return; const p1 = ps[0];
      const loja = ctx.txtPuro(p1.loja_name) || p1.loja_user; const n = ps.length;
      const ganho = soma(ps, 'valor_motoboy'), km = soma(ps, 'distancia');
      const titulo = p1.rota_socorro ? '🆘 SOCORRO – ROTA iFood' : (motivo === 'novamente' ? 'ROTA iFood disponível novamente' : 'ROTA iFood');
      const linhas = ps.map((p, i) => '🏠 ' + (i + 1) + 'ª entrega: ' + (p.bairro_destino || '-')).join('\n');
      const msgTg = titulo + ' – ' + n + ' entregas – ganho total ' + fmtR(ganho) + '\n\nPedido #' + p1.id + '\nLoja: ' + loja + '\n📍 Coleta: ' + ctx.bairroColeta(p1) + '\n' + linhas + '\nDistância total: ' + fmtKm(km) + '\n\nAceite pelo app.';
      if (bot) {
        const g = process.env.TELEGRAM_GROUP_ID; if (g) bot.sendMessage(g, msgTg).catch(() => {});
        const mbs = await pool.query("SELECT telegram_id FROM users WHERE role='motoboy' AND online=true AND telegram_id IS NOT NULL AND (blocked IS NOT TRUE) AND (blocked_until IS NULL OR blocked_until <= (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT)");
        mbs.rows.forEach((mb) => bot.sendMessage(mb.telegram_id, msgTg).catch(() => {}));
      }
      ctx.sendPushToOnlineMotoboys('🛵 ' + titulo + '!', titulo + ' – ' + n + ' entregas – ganho total ' + fmtR(ganho) + ' · ' + loja, { orderId: p1.id });
      const url = process.env.BOT_URL, sec = process.env.BOT_SECRET;
      if (url && sec) {
        const nomeRota = loja + ' (rota com ' + n + ' entregas)';
        const msgG = '🛵 Pedido Disponível!\n📦 Pedido #' + p1.id + ' — ' + nomeRota + '\n📍 Coleta: ' + ctx.bairroColeta(p1) + '\n' + linhas + ctx.linhasKmGanho(km, ganho);
        axios.post(url + '/api/send-group-message', { message: msgG, mentionAll: true }, { headers: { 'x-bot-secret': sec } }).catch((e) => console.error('[ROTA] grupo:', e.message));
        // secundario: Km total da rota; rota com 2+ entregas vai sem a linha R$
        const ls = ctx.msgDisponivelSecundario(p1, nomeRota, { km, ganho, semGanho: n >= 2 }).split('\n'); const k = ls.findIndex((l) => l.indexOf('🏠 Entrega:') === 0);
        if (k >= 0) ls.splice(k, 1, ...linhas.split('\n'));
        axios.post(url + '/api/send-group-message', { message: ls.join('\n'), grupo: 'secundario' }, { headers: { 'x-bot-secret': sec } }).catch((e) => console.error('[ROTA] grupo2:', e.message));
      }
    } catch (e) { console.error('[ROTA] aviso disponivel:', e.message); }
  }
  // Grupo SECUNDARIO: "Pedido em Preparo" da rota (bairros 1a/2a...). A rota nao tem horario previsto: e lancada quando a loja marca Pronto
  async function avisarRotaPreparo(rotaId) {
    try {
      const url = process.env.BOT_URL, sec = process.env.BOT_SECRET; if (!url || !sec || !ctx.msgPreparoSecundario) return;
      const ps = vivos(await pedidosDaRota(rotaId)); if (!ps.length) return; const p1 = ps[0];
      const loja = ctx.txtPuro(p1.loja_name) || p1.loja_user;
      const linhas = ps.map((p, i) => '🏠 ' + (i + 1) + 'ª entrega: ' + (p.bairro_destino || '-')).join('\n');
      const msg = ctx.msgPreparoSecundario(p1, loja + ' (rota com ' + ps.length + ' entregas)', null, linhas);
      axios.post(url + '/api/send-group-message', { message: msg, grupo: 'secundario' }, { headers: { 'x-bot-secret': sec } }).catch((e) => console.error('[ROTA] grupo2 preparo:', e.message));
    } catch (e) { console.error('[ROTA] aviso preparo:', e.message); }
  }
  async function avisarCancelado(p) {
    try {
      const url = process.env.BOT_URL, sec = process.env.BOT_SECRET; if (!url || !sec) return;
      const msg = '❌ Pedido Cancelado!\nPedido #' + p.id + ' - ' + (ctx.txtPuro(p.loja_name) || p.loja_user) + '\nEste pedido foi cancelado. Motoboys nao precisam mais aguardar por ele.';
      axios.post(url + '/api/send-group-message', { message: msg }, { headers: { 'x-bot-secret': sec } }).catch(() => {});
    } catch (e) {}
  }

  // ---------- lancamento (todas "prontas" -> a 1a fica disponivel, as outras em 'rota_espera') ----------
  async function lancarRota(rotaId) {
    const ps = vivos(await pedidosDaRota(rotaId)); if (!ps.length) return false;
    if (!ps.every((p) => p.status === 'em_preparo' && p.rota_pronto)) return false;
    const up = await pool.query("UPDATE orders SET status='pendente', pending_until=$1, launch_at=0 WHERE id=$2 AND status='em_preparo' RETURNING id", [Date.now() + 15000, ps[0].id]);
    if (!up.rows.length) return false; // outra requisicao ja lancou
    await pool.query("UPDATE orders SET status='rota_espera', launch_at=0 WHERE rota_id=$1 AND id<>$2 AND status='em_preparo'", [rotaId, ps[0].id]);
    console.log('[ROTA] Rota #' + rotaId + ' lancada (' + ps.length + ' entregas).');
    avisarRotaDisponivel(rotaId, 'lancada');
    return true;
  }
  // Devolve a rota inteira para o sistema (desistencia ou motoboy nao chegou na loja): UMA penalidade so
  async function devolverRota(rotaId, motivo) {
    const ps = vivos(await pedidosDaRota(rotaId));
    const ativos = ps.filter((p) => p.status === 'aceito' || p.status === 'na_loja'); if (!ativos.length || !ps.length) return null;
    const mbId = ativos[0].motoboy_id, mbNome = ativos[0].motoboy_name;
    await pool.query("UPDATE orders SET status=CASE WHEN id=$2 THEN 'pendente' ELSE 'rota_espera' END, motoboy_id=NULL, motoboy_name=NULL, t_aceito=NULL, t_na_loja=NULL, chegada_loja_auto=false, pending_until=$3 WHERE rota_id=$1 AND status IN ('aceito','na_loja')", [rotaId, ps[0].id, Date.now() + 15000]);
    if (ctx.motivoDisponivel) await ctx.motivoDisponivel(ps[0].id, motivo === 'timeout' ? 'Voltou para disponível (motoboy não chegou na loja em 15 min · rota iFood)' : 'Voltou para disponível (motoboy desistiu da rota iFood)');
    let pen = null;
    if (mbId) { try { pen = await ctx.aplicarPenalidadeMotoboy(mbId, motivo === 'timeout' ? 'Você não chegou na loja em 15 minutos.' : 'Você desistiu de uma rota iFood.'); } catch (e) { console.error('[ROTA] penalidade:', e.message); } }
    const desc = (motivo === 'timeout' ? 'Motoboy nao chegou na loja em 15 minutos' : 'Motoboy desistiu da rota') + ' - a rota iFood voltou inteira para o sistema (' + (ctx.descPenalidade(pen) || 'penalidade') + ')';
    ativos.forEach((p) => ctx.registrarEvento(p.id, mbId, mbNome, motivo === 'timeout' ? 'timeout' : 'cancelado', desc));
    avisarRotaDisponivel(rotaId, 'novamente');
    return pedido(ps[0].id);
  }
  async function timeoutRota(order) {
    const ps = vivos(await pedidosDaRota(order.rota_id));
    const meus = ps.filter((p) => String(p.motoboy_id) === String(order.motoboy_id));
    if (!meus.some((p) => p.status === 'aceito')) return;
    // Ja chegou na loja por outra entrega da rota: so marca a chegada desta (sem penalidade)
    if (meus.some((p) => p.t_na_loja || ['na_loja', 'coletado', 'no_cliente', 'entregue'].includes(p.status))) {
      try { await axios.put('http://localhost:' + PORT + '/orders/' + order.id, { status: 'na_loja', t_na_loja: new Date().toISOString(), _chegada_agrupada: true }); } catch (e) { console.error('[ROTA] chegada agrupada:', e.message); }
      return;
    }
    console.log('[ROTA] Rota #' + order.rota_id + ': motoboy ' + order.motoboy_name + ' nao chegou na loja. Rota volta inteira para o sistema.');
    await devolverRota(order.rota_id, 'timeout');
  }

  // ---------- ganchos do PUT /orders/:id ----------
  // Antes de gravar. Devolve true quando ja respondeu (a rota tratou o pedido).
  async function antesDoPut(req, res, fields) {
    const o = await pedido(req.params.id); if (!o) return false;
    const aceiteNovo = fields.motoboy_id !== undefined && fields.motoboy_id !== null && fields.motoboy_id !== '' && String(o.motoboy_id) !== String(fields.motoboy_id);
    if (aceiteNovo) {
      if (!o.rota_id) {
        // Rota e exclusiva: com uma rota em andamento, o motoboy nao aceita mais nada
        const r = await pool.query('SELECT 1 FROM orders WHERE motoboy_id=$1 AND rota_id IS NOT NULL AND status = ANY($2) LIMIT 1', [fields.motoboy_id, STATUS_ATIVOS]);
        if (r.rows.length) { res.status(403).json({ error: 'Você está com uma rota iFood. Termine as entregas da rota antes de aceitar outro pedido.', rota_em_andamento: true }); return true; }
        return false;
      }
      if (o.status !== 'pendente' || o.motoboy_id) { res.status(403).json({ error: 'Esta rota não está mais disponível.', ja_aceito: true }); return true; }
      const r = await pool.query('SELECT 1 FROM orders WHERE motoboy_id=$1 AND status = ANY($2) LIMIT 1', [fields.motoboy_id, STATUS_ATIVOS]);
      if (r.rows.length) { res.status(403).json({ error: 'Termine suas entregas para pegar uma rota.', rota_exclusiva: true }); return true; }
      return false;
    }
    if (!o.rota_id) return false;
    // Desistencia de uma entrega = desistencia da rota inteira (todas voltam juntas, uma penalidade so)
    if (fields.status === 'pendente' && fields.motoboy_id === null && o.motoboy_id !== null) {
      const ps = vivos(await pedidosDaRota(o.rota_id));
      if (ps.some((p) => !(p.status === 'aceito' || p.status === 'na_loja'))) { res.status(403).json({ error: 'Não dá para desistir: a rota já foi coletada. Use "Preciso parar (socorro)" para chamar o admin.' }); return true; }
      const out = await devolverRota(o.rota_id, 'desistencia');
      res.json(out || o); return true;
    }
    if (fields.status === 'cancelado') { res.status(409).json({ error: 'Este pedido faz parte de uma rota iFood. Use "Cancelar este cliente" ou "Cancelar rota inteira" no cartão da rota.', rota: true }); return true; }
    if (fields.status === 'entregue') {
      // Ja finalizada (ex.: dois "Entreguei" seguidos): nao finaliza de novo (nao paga o motoboy duas vezes)
      if (o.status === 'entregue') { res.json(o); return true; }
      // Loja decidiu "Deixar com o motoboy": finaliza sem codigo do iFood (sem robo)
      if (o.rota_decisao === 'deixar') { req._rotaSemIfood = true; req._rotaSemAvisoCliente = true; return false; }
      // Retorno para a loja: finaliza com o codigo de entrega da Flash Drop (que so a loja ve), sem robo
      if (o.rota_retorno === 'pendente' || o.rota_retorno === 'chegou') {
        const cod = String(fields.observacao_entrega || '').replace(/\D/g, '');
        if (!o.delivery_code || cod !== String(o.delivery_code)) { res.status(400).json({ error: 'Código incorreto. No retorno, use o código de entrega da Flash Drop que a loja passa quando recebe o pedido de volta.' }); return true; }
        req._rotaSemIfood = true; req._rotaSemAvisoCliente = true;
        fields.rota_retorno = 'finalizado'; fields.rota_retorno_finalizado_em = new Date().toISOString(); fields.rota_retorno_finalizado_por = String(o.motoboy_name || ('motoboy ' + o.motoboy_id));
        return false;
      }
    }
    // Retorno: "Cheguei" agora e a chegada de volta na loja (sem avisos ao cliente, sem reabrir contadores)
    if (fields.status === 'no_cliente' && o.rota_retorno === 'pendente') {
      fields.rota_retorno = 'chegou'; fields.rota_retorno_chegou_em = new Date().toISOString(); delete fields.t_no_cliente; delete fields.chegada_cliente_auto;
      req._rotaRetornoChegada = true;
    }
    return false;
  }
  // Depois de gravar: aceite e coleta valem para a rota inteira
  async function depoisDoPut(req, order, fields) {
    if (!order || !order.rota_id) return;
    try {
      if (fields.status === 'aceito' && order.motoboy_id && String(order.motoboy_id) === String(fields.motoboy_id)) {
        const r = await pool.query("UPDATE orders SET status='aceito', motoboy_id=$1, motoboy_name=$2, t_aceito=$3 WHERE rota_id=$4 AND status='rota_espera' AND motoboy_id IS NULL RETURNING id", [order.motoboy_id, order.motoboy_name, order.t_aceito || new Date(), order.rota_id]);
        r.rows.forEach((x) => ctx.registrarEvento(x.id, order.motoboy_id, order.motoboy_name, 'aceito', 'Aceito por ' + (order.motoboy_name || '') + ' junto com a rota iFood #' + order.rota_id));
        console.log('[ROTA] Rota #' + order.rota_id + ' aceita por ' + order.motoboy_name + ' (' + (r.rows.length + 1) + ' entregas).');
      }
      // Um "Coletei" marca todas como coletadas (so entao liberam os dados dos clientes)
      if (fields.status === 'coletado' && !order.rota_retorno) {
        await pool.query("UPDATE orders SET status='coletado', t_coletado=COALESCE(t_coletado, NOW()), t_na_loja=COALESCE(t_na_loja, NOW()) WHERE rota_id=$1 AND id<>$2 AND motoboy_id=$3 AND status IN ('aceito','na_loja')", [order.rota_id, order.id, order.motoboy_id]);
      }
    } catch (e) { console.error('[ROTA] depois do PUT:', e.message); }
  }

  // ---------- GET /orders para o app Android atual (sem versao nova) ----------
  // Disponivel: a 1a entrega mostra os bairros na ordem, o total e a km total. Retorno: endereco e nome viram os da loja.
  function paraApp(rows) {
    if (!rows.some((o) => o.rota_id)) return rows;
    const porRota = {}; rows.forEach((o) => { if (o.rota_id) (porRota[o.rota_id] = porRota[o.rota_id] || []).push(o); });
    return rows.map((o) => {
      if (!o.rota_id) return o;
      if (o.status === 'pendente' && !o.motoboy_id) {
        const ps = vivos(porRota[o.rota_id] || []);
        if (ps.length > 1) {
          const bairros = ps.map((p) => p.bairro_destino || '-').join(' → ');
          return Object.assign({}, o, { endereco_entrega: 'ROTA iFood · ' + ps.length + ' entregas: ' + bairros, bairro_destino: bairros, complemento_entrega: null, distancia: soma(ps, 'distancia'), valor_motoboy: soma(ps, 'valor_motoboy'), valor_total: soma(ps, 'valor_total'), loja_name: (o.loja_name || '') + ' · ROTA ' + ps.length + ' entregas' });
        }
      }
      if (o.rota_retorno === 'pendente' || o.rota_retorno === 'chegou') {
        return Object.assign({}, o, { nome_cliente: 'RETORNO PARA A LOJA', telefone_cliente: null, endereco_entrega: o.endereco_coleta, complemento_entrega: o.complemento_coleta || null, bairro_destino: o.bairro_coleta || null, entrega_lat: o.coleta_lat, entrega_lng: o.coleta_lng });
      }
      return o;
    });
  }

  // ---------- criacao (loja) ----------
  async function lerPontos(c, body) {
    const pontos = Array.isArray(body.pontos) ? body.pontos : [];
    if (pontos.length < 2) throw erro('Uma rota precisa de pelo menos 2 entregas.');
    if (pontos.length > maxPontos(c)) throw erro('Máximo de ' + maxPontos(c) + ' entregas por rota.');
    const col = body.coleta || {};
    const origem = temCoord(col) ? { lat: parseFloat(col.lat), lng: parseFloat(col.lng) } : (col.endereco ? await geocodificar(col.endereco) : null);
    if (!origem) throw erro('Informe o endereço de coleta.');
    const out = [];
    for (let i = 0; i < pontos.length; i++) {
      const p = pontos[i] || {}; const end = String(p.endereco_entrega || '').trim();
      if (!end) throw erro('Informe o endereço da ' + (i + 1) + 'ª entrega.');
      const coord = temCoord(p) ? { lat: parseFloat(p.lat), lng: parseFloat(p.lng) } : await geocodificar(end);
      out.push(Object.assign({}, p, coord, { endereco_entrega: end }));
    }
    return { origem, pontos: out, coleta: col };
  }
  app.post('/rotas/ifood/cotar', h(async (req, res) => {
    const q = quem(req); if (!q || (q.papel !== 'loja' && q.papel !== 'admin')) throw erro('Faça login de novo na loja.', 401);
    const c = await cfg(); if (!rotaLigada(c)) throw erro('Rota iFood desligada.', 403);
    const lidos = await lerPontos(c, req.body || {});
    // ordem mais curta automatica: o preco ja sai na ordem otimizada; a tela reorganiza os clientes com "ordem"
    const om = await ordemMaisCurta(lidos.origem, lidos.pontos); const pontos = om.ordem.map((i) => lidos.pontos[i]);
    const cot = await cotar(c, lidos.origem, pontos);
    res.json({ ok: true, n: pontos.length, total: cot.total, km_total: cot.km_total, ordem: om.ordem, km_ordem_loja: om.km_atual, economia_km: om.km_atual != null && om.km_otimo != null ? r2(om.km_atual - om.km_otimo) : 0 });
  }));
  app.post('/rotas/ifood', h(async (req, res) => {
    const q = quem(req); if (!q || q.papel !== 'loja') throw erro('Faça login de novo na loja.', 401);
    const c = await cfg(); if (!rotaLigada(c)) throw erro('Rota iFood desligada.', 403);
    const fech = await ctx.bloqueioFuncionamento(req); if (fech) throw erro(fech.mensagem, 403, { fechado: true, proxima_abertura: fech.proxima_abertura });
    const lr = await pool.query("SELECT id, username, name, credit, phone FROM users WHERE id=$1 AND role='loja'", [q.id]); const loja = lr.rows[0]; if (!loja) throw erro('Loja não encontrada.', 404);
    const body = req.body || {};
    (body.pontos || []).forEach((p, i) => { if (String((p && p.ifood_localizador) || '').replace(/\D/g, '').length !== 8) throw erro('Verifique o localizador iFood (8 números) do ' + (i + 1) + 'º cliente.'); if (!String((p && p.nome_cliente) || '').trim()) throw erro('Informe o nome do ' + (i + 1) + 'º cliente.'); });
    const lidos = await lerPontos(c, body); const { origem, coleta } = lidos;
    // o servidor valida a ordem: cria sempre na ordem mais curta (rota_ordem = ordem otimizada), sem confiar so no navegador
    const om = await ordemMaisCurta(origem, lidos.pontos); const pontos = om.ordem.map((i) => lidos.pontos[i]);
    const cot = await cotar(c, origem, pontos); const vals = porEntrega(cot);
    const cli = await pool.connect();
    try {
      await cli.query('BEGIN');
      const cr = await cli.query('SELECT credit FROM users WHERE id=$1 FOR UPDATE', [loja.id]); const credito = parseFloat(cr.rows[0].credit) || 0;
      if (credito < cot.total) { await cli.query('ROLLBACK'); throw erro('Saldo insuficiente para a rota. Total ' + fmtR(cot.total) + '; seu saldo ' + fmtR(credito) + '.', 402, { faltante: r2(cot.total - credito), total: cot.total }); }
      let rotaId = null; const ids = [];
      for (let i = 0; i < pontos.length; i++) {
        const p = pontos[i], v = vals[i];
        const ins = await cli.query(
          `INSERT INTO orders (loja_user, loja_name, plataforma, endereco_coleta, bairro_coleta, complemento_coleta, obs_coleta, endereco_entrega, bairro_destino, complemento_entrega,
             nome_cliente, telefone_cliente, cod_pedido, cobrar_cliente, tipo_pagamento, valor_pedido, valor_total, valor_motoboy, comissao, distancia, status, pending_until, telefone_loja,
             launch_at, delivery_code, taxa_extra_chuva, taxa_extra_noturna, chuva_desconto_de, ifood_localizador, ifood_numero_pedido, ifood_nome_cliente,
             coleta_lat, coleta_lng, entrega_lat, entrega_lng, rota_id, rota_ordem, rota_pronto, t_em_preparo, created_at)
           VALUES ($1,$2,'telegram',$3,$4,$5,$6,$7,$8,$9,$10,$11,'','nao','ifood',0,$12,$13,$14,$15,'em_preparo',$16,$17,0,$18,$19,$20,'admin',$21,$22,$23,$24,$25,$26,$27,$28,$29,false,NOW(), NOW() - ($30 * INTERVAL '1 second'))
           RETURNING id`,
          [loja.username, ctx.sanitize(loja.name), ctx.sanitize(coleta.endereco || ''), ctx.sanitize(coleta.bairro || '') || null, ctx.sanitize(coleta.complemento || '') || null, ctx.sanitize(coleta.obs || '') || null,
            ctx.sanitize(p.endereco_entrega), ctx.sanitize(p.bairro_destino || '') || null, ctx.sanitize(p.complemento_entrega || '') || null,
            ctx.sanitize(String(p.nome_cliente).trim()), String(p.telefone_cliente || '').replace(/\D/g, '') || null,
            v.valor_total, v.valor_motoboy, v.comissao, v.distancia, Date.now() + 15000, loja.phone || null,
            String(Math.floor(1000 + Math.random() * 9000)), i === 0 ? cot.extras.chuva : 0, i === 0 ? cot.extras.noturna : 0,
            String(p.ifood_localizador).replace(/\D/g, ''), String(p.ifood_numero_pedido || '').replace(/[^\w-]/g, '') || null, ctx.sanitize(p.ifood_nome_cliente || '') || null,
            origem.lat, origem.lng, p.lat, p.lng, rotaId, i + 1, i]
        );
        const id = ins.rows[0].id; ids.push(id);
        if (i === 0) { rotaId = id; await cli.query('UPDATE orders SET rota_id=$1 WHERE id=$1', [id]); }
      }
      await cli.query('UPDATE users SET credit = credit - $1 WHERE id=$2', [cot.total, loja.id]);
      await cli.query('INSERT INTO loja_wallet_events (loja_id, tipo, valor, descricao, order_id) VALUES ($1,$2,$3,$4,$5)', [loja.id, 'debito_pedido', cot.total, 'Débito rota iFood #' + rotaId + ' (' + pontos.length + ' entregas)', rotaId]);
      await cli.query('COMMIT');
      console.log('[ROTA] Rota #' + rotaId + ' criada por ' + loja.username + ': ' + pontos.length + ' entregas, ' + fmtR(cot.total) + '.');
      avisarRotaPreparo(rotaId);
      res.json({ ok: true, rota_id: rotaId, ids, total: cot.total, n: pontos.length });
    } catch (e) { try { await cli.query('ROLLBACK'); } catch (e2) {} throw e; }
    finally { cli.release(); }
  }));
  // Loja marca "Pronto" em cada entrega; quando a ultima fica pronta, a rota e lancada
  app.post('/rotas/:rotaId/pronto', h(async (req, res) => {
    const ps = await pedidosDaRota(parseInt(req.params.rotaId) || 0); exigirLojaDona(quem(req), ps);
    const p = ps.find((x) => x.id === parseInt((req.body || {}).order_id)); if (!p) throw erro('Entrega não encontrada nesta rota.', 404);
    if (p.status !== 'em_preparo') throw erro('Esta entrega não está mais em preparo.', 409);
    await pool.query('UPDATE orders SET rota_pronto=true WHERE id=$1', [p.id]);
    const lancada = await lancarRota(p.rota_id);
    res.json({ ok: true, lancada });
  }));

  // ---------- cancelamentos (loja, antes da coleta) ----------
  async function devolverDinheiro(db, ps0, valor, descricao, rotaId) {
    if (!valor) return;
    if (ps0.rota_socorro) { await db.query('UPDATE platform_wallet SET balance = balance + $1, updated_at=NOW() WHERE id=1', [valor]); await db.query("INSERT INTO platform_events (tipo, valor, descricao, order_id) VALUES ('socorro_estorno', $1, $2, $3)", [valor, descricao, rotaId]); return; }
    await db.query('UPDATE users SET credit = credit + $1 WHERE username=$2', [valor, ps0.loja_user]);
    const lid = await db.query('SELECT id FROM users WHERE username=$1', [ps0.loja_user]);
    if (lid.rows.length) await db.query('INSERT INTO loja_wallet_events (loja_id, tipo, valor, descricao, order_id) VALUES ($1,$2,$3,$4,$5)', [lid.rows[0].id, valor > 0 ? 'estorno' : 'debito_pedido', Math.abs(valor), descricao, rotaId]);
  }
  async function cobrarTaxaCancelamento(db, ps0, taxa, pedidoId) {
    if (!taxa || ps0.rota_socorro) return 0;
    await db.query('UPDATE users SET credit = credit - $1 WHERE username=$2', [taxa, ps0.loja_user]);
    const lid = await db.query('SELECT id FROM users WHERE username=$1', [ps0.loja_user]);
    if (lid.rows.length) await db.query('INSERT INTO loja_wallet_events (loja_id, tipo, valor, descricao, order_id) VALUES ($1,$2,$3,$4,$5)', [lid.rows[0].id, 'taxa_cancelamento', taxa, 'Taxa de cancelamento - pedido #' + pedidoId + ' (cobrada pelo uso de dados e recursos do sistema neste cancelamento)', pedidoId]);
    return taxa;
  }
  app.post('/rotas/:rotaId/cancelar-cliente', h(async (req, res) => {
    const rotaId = parseInt(req.params.rotaId) || 0; const q = quem(req);
    exigirLojaDona(q, await pedidosDaRota(rotaId));
    const c = await cfg(); const cli = await pool.connect();
    try {
      await cli.query('BEGIN');
      const ps = (await cli.query('SELECT * FROM orders WHERE rota_id=$1 ORDER BY rota_ordem, id FOR UPDATE', [rotaId])).rows;
      const alvo = ps.find((x) => x.id === parseInt((req.body || {}).order_id));
      if (!alvo) throw erro('Entrega não encontrada nesta rota.', 404);
      if (alvo.status === 'cancelado') throw erro('Este cliente já foi cancelado.', 409);
      if (alvo.rota_socorro && q.papel !== 'admin') throw erro('Rota de socorro: só o admin pode cancelar.', 403);
      const vv = vivos(ps);
      if (vv.some((p) => !ANTES_DA_COLETA.includes(p.status))) throw erro('Depois que o motoboy coleta, não dá para cancelar pela tela.', 409);
      const resto = vv.filter((p) => p.id !== alvo.id);
      const totalAntes = soma(vv, 'valor_total');
      let totalDepois = 0, vals = [];
      if (resto.length) {
        const ext = { noturna: soma(vv, 'taxa_extra_noturna'), chuva: soma(vv, 'taxa_extra_chuva') };
        const cot = await cotar(c, { lat: alvo.coleta_lat, lng: alvo.coleta_lng }, resto.map((p) => ({ lat: p.entrega_lat, lng: p.entrega_lng })), ext);
        totalDepois = cot.total; vals = porEntrega(cot);
        for (let i = 0; i < resto.length; i++) {
          await cli.query('UPDATE orders SET rota_ordem=$2, distancia=$3, valor_total=$4, comissao=$5, valor_motoboy=$6, taxa_extra_noturna=$7, taxa_extra_chuva=$8 WHERE id=$1', [resto[i].id, i + 1, vals[i].distancia, vals[i].valor_total, vals[i].comissao, vals[i].valor_motoboy, i === 0 ? ext.noturna : 0, i === 0 ? ext.chuva : 0]);
        }
      }
      await cli.query("UPDATE orders SET status='cancelado', cancelado_por=$2, rota_ordem=NULL WHERE id=$1", [alvo.id, q.papel === 'admin' ? 'admin' : 'loja']);
      // Era a 1a de uma rota ja disponivel: a nova 1a fica disponivel no lugar dela
      if (alvo.status === 'pendente' && resto.length) await cli.query("UPDATE orders SET status='pendente', pending_until=$2 WHERE id=$1 AND status='rota_espera'", [resto[0].id, Date.now() + 15000]);
      const diff = r2(totalAntes - totalDepois);
      await devolverDinheiro(cli, vv[0], diff, (diff >= 0 ? 'Estorno' : 'Diferença') + ' rota iFood #' + rotaId + ': cliente do pedido #' + alvo.id + ' cancelado (rota recalculada de ' + fmtR(totalAntes) + ' para ' + fmtR(totalDepois) + ')', rotaId);
      const taxa = await cobrarTaxaCancelamento(cli, vv[0], r2(parseFloat(c.taxa_cancelamento) || 0), alvo.id);
      await cli.query('COMMIT');
      ctx.registrarEvento(alvo.id, alvo.motoboy_id, alvo.motoboy_name, 'cancelado', 'Cliente cancelado pela ' + (q.papel === 'admin' ? 'Flash Drop' : 'loja') + ' (rota iFood #' + rotaId + ' recalculada)');
      if (alvo.motoboy_id) pushMotoboy(alvo.motoboy_id, '❌ Entrega cancelada pela loja', 'Pedido #' + alvo.id + ' saiu da sua rota. Os valores das outras entregas foram recalculados.');
      if (!resto.length && ['pendente', 'aceito', 'na_loja'].includes(alvo.status)) avisarCancelado(alvo);
      if (resto.length && resto.every((p) => p.status === 'em_preparo')) await lancarRota(rotaId);
      res.json({ ok: true, total_antes: totalAntes, total_depois: totalDepois, diferenca: diff, taxa, restantes: resto.length });
    } catch (e) { try { await cli.query('ROLLBACK'); } catch (e2) {} throw e; }
    finally { cli.release(); }
  }));
  app.post('/rotas/:rotaId/cancelar', h(async (req, res) => {
    const rotaId = parseInt(req.params.rotaId) || 0; const q = quem(req);
    exigirLojaDona(q, await pedidosDaRota(rotaId));
    const c = await cfg(); const cli = await pool.connect();
    try {
      await cli.query('BEGIN');
      const ps = (await cli.query('SELECT * FROM orders WHERE rota_id=$1 ORDER BY rota_ordem, id FOR UPDATE', [rotaId])).rows; const vv = vivos(ps);
      if (!vv.length) throw erro('Esta rota já está cancelada.', 409);
      if (vv[0].rota_socorro && q.papel !== 'admin') throw erro('Rota de socorro: só o admin pode cancelar.', 403);
      if (vv.some((p) => !ANTES_DA_COLETA.includes(p.status))) throw erro('Depois que o motoboy coleta, não dá para cancelar pela tela.', 409);
      const total = soma(vv, 'valor_total');
      await cli.query("UPDATE orders SET status='cancelado', cancelado_por=$2 WHERE rota_id=$1 AND status<>'cancelado'", [rotaId, q.papel === 'admin' ? 'admin' : 'loja']);
      await devolverDinheiro(cli, vv[0], total, 'Estorno rota iFood #' + rotaId + ' (rota inteira cancelada)', rotaId);
      const taxa = await cobrarTaxaCancelamento(cli, vv[0], r2(parseFloat(c.taxa_cancelamento) || 0), rotaId);
      await cli.query('COMMIT');
      vv.forEach((p) => ctx.registrarEvento(p.id, p.motoboy_id, p.motoboy_name, 'cancelado', 'Rota iFood #' + rotaId + ' cancelada inteira pela ' + (q.papel === 'admin' ? 'Flash Drop' : 'loja')));
      const mb = vv.find((p) => p.motoboy_id); if (mb) pushMotoboy(mb.motoboy_id, '❌ Rota cancelada pela loja', 'A rota iFood #' + rotaId + ' foi cancelada. Você não precisa mais buscá-la.');
      if (vv.some((p) => ['pendente', 'aceito', 'na_loja'].includes(p.status))) avisarCancelado(vv[0]);
      res.json({ ok: true, estorno: total, taxa });
    } catch (e) { try { await cli.query('ROLLBACK'); } catch (e2) {} throw e; }
    finally { cli.release(); }
  }));

  // ---------- cliente nao atende ----------
  app.post('/rotas/entrega/:id/nao-atende', h(async (req, res) => {
    const o = await pedido(req.params.id); if (!o || !o.rota_id) throw erro('Entrega não encontrada.', 404);
    const q = quem(req);
    if (!q || (q.papel === 'motoboy' && String(q.id) !== String(o.motoboy_id)) || q.papel === 'loja') throw erro('Esta entrega não está com você.', 403);
    if (!['coletado', 'no_cliente'].includes(o.status) || o.rota_retorno) throw erro('Só dá para avisar "cliente não atende" depois de coletar e antes de finalizar.', 409);
    if (o.rota_nao_atende_em) return res.json({ ok: true, ja_avisado: true });
    const prazo = prazoLoja(await cfg());
    await pool.query('UPDATE orders SET rota_nao_atende_em=NOW(), rota_nao_atende_admin=false, rota_nao_atende_prazo_min=$2 WHERE id=$1 AND rota_nao_atende_em IS NULL', [o.id, prazo]);
    ctx.registrarEvento(o.id, o.motoboy_id, o.motoboy_name, 'nao_atende', 'Motoboy avisou: cliente não atende. A loja tem ' + prazo + ' min para decidir.');
    res.json({ ok: true, prazo_min: prazo });
  }));
  async function valorRetorno(c, o) {
    const ps = vivos(await pedidosDaRota(o.rota_id)); const ultimo = ps[ps.length - 1] || o;
    const km = (await trechosKm([{ lat: ultimo.entrega_lat, lng: ultimo.entrega_lng }], [{ lat: o.coleta_lat, lng: o.coleta_lng }]))[0];
    return { km, valor: r2(km * tarifaKm(c, km)) };
  }
  app.post('/rotas/entrega/:id/decisao', h(async (req, res) => {
    const o = await pedido(req.params.id); if (!o || !o.rota_id) throw erro('Entrega não encontrada.', 404);
    const q = quem(req); exigirLojaDona(q, [o]);
    if (!o.rota_nao_atende_em) throw erro('O motoboy não avisou que o cliente não atende.', 409);
    if (o.rota_decisao) throw erro('Esta entrega já foi decidida.', 409);
    if (!['coletado', 'no_cliente'].includes(o.status)) throw erro('Esta entrega não está mais em andamento.', 409);
    const quemDecidiu = q.papel === 'admin' ? 'Flash Drop (admin)' : 'loja';
    const dec = String((req.body || {}).decisao || '');
    if (dec === 'deixar') {
      const up = await pool.query("UPDATE orders SET rota_decisao='deixar', rota_decisao_por=$2, rota_decisao_em=NOW() WHERE id=$1 AND rota_decisao IS NULL RETURNING id", [o.id, quemDecidiu]);
      if (!up.rows.length) throw erro('Esta entrega já foi decidida.', 409);
      const r = await axios.put('http://localhost:' + PORT + '/orders/' + o.id, { status: 'entregue', t_entregue: new Date().toISOString(), observacao_entrega: 'Entregue - cliente não atendeu, ficou com o motoboy (decisão da ' + (q.papel === 'admin' ? 'Flash Drop' : 'loja') + ')' }, { validateStatus: () => true });
      if (r.status >= 300) { await pool.query('UPDATE orders SET rota_decisao=NULL, rota_decisao_por=NULL, rota_decisao_em=NULL WHERE id=$1', [o.id]); throw erro((r.data && r.data.error) || 'Não foi possível finalizar.', r.status); }
      ctx.registrarEvento(o.id, o.motoboy_id, o.motoboy_name, 'entregue', 'Entregue - cliente não atendeu, ficou com o motoboy (decisão da ' + quemDecidiu + ')');
      if (o.motoboy_id) pushMotoboy(o.motoboy_id, '✅ Pedido #' + o.id + ' finalizado', 'A loja decidiu: o pedido fica com você. Não precisa de código.');
      return res.json({ ok: true, decisao: 'deixar' });
    }
    if (dec === 'retornar') {
      const c = await cfg(); const ret = await valorRetorno(c, o);
      if (!(req.body || {}).confirmar) return res.json({ ok: true, preview: true, valor_retorno: ret.valor, mensagem: 'O motoboy irá cobrar ' + fmtR(ret.valor) + ' para retornar. Confirmar?' });
      const com = c.commission_type === 'percentual' ? r2(ret.valor * (parseFloat(c.commission) || 0) / 100) : 0;
      const cli = await pool.connect();
      try {
        await cli.query('BEGIN');
        const up = await cli.query("UPDATE orders SET rota_decisao='retornar', rota_decisao_por=$2, rota_decisao_em=NOW(), rota_retorno='pendente', rota_retorno_valor=$3, valor_total=valor_total+$3, comissao=comissao+$4, valor_motoboy=valor_motoboy+$5, status='coletado' WHERE id=$1 AND rota_decisao IS NULL AND status IN ('coletado','no_cliente') RETURNING id", [o.id, quemDecidiu, ret.valor, com, r2(ret.valor - com)]);
        if (!up.rows.length) throw erro('Esta entrega já foi decidida.', 409);
        await cli.query('UPDATE users SET credit = credit - $1 WHERE username=$2', [ret.valor, o.loja_user]);
        const lid = await cli.query('SELECT id FROM users WHERE username=$1', [o.loja_user]);
        if (lid.rows.length) await cli.query('INSERT INTO loja_wallet_events (loja_id, tipo, valor, descricao, order_id) VALUES ($1,$2,$3,$4,$5)', [lid.rows[0].id, 'debito_pedido', ret.valor, 'Retorno à loja - pedido #' + o.id + ' (cliente não atendeu)', o.id]);
        await cli.query('COMMIT');
      } catch (e) { try { await cli.query('ROLLBACK'); } catch (e2) {} throw e; }
      finally { cli.release(); }
      ctx.registrarEvento(o.id, o.motoboy_id, o.motoboy_name, 'retorno', 'Retornar para a loja (decisão da ' + quemDecidiu + '): ' + fmtR(ret.valor) + ', ' + fmtKm(ret.km));
      if (o.motoboy_id) pushMotoboy(o.motoboy_id, '↩️ Pedido #' + o.id + ': retornar para a loja', 'Termine as outras entregas e volte com este pedido. Ao chegar, peça o código de entrega para a loja. Você recebe ' + fmtR(r2(ret.valor - com)) + ' pelo retorno.');
      return res.json({ ok: true, decisao: 'retornar', valor_retorno: ret.valor });
    }
    throw erro('Escolha "deixar" ou "retornar".');
  }));
  // Gatilho automatico: entrega da rota parada X min (admin, padrao 10) em "no_cliente" sem finalizar -> abre a decisao da
  // loja exatamente como o botao "Cliente nao atende" do motoboy. Nao vale para o retorno a loja nem para entrega ja decidida.
  // Se o motoboy finalizar antes, nada acontece; se finalizar depois, a decisao some (todas as telas so mostram enquanto
  // a entrega esta em coletado/no_cliente e a decisao recusa entrega finalizada).
  async function gatilhoNaoAtende() {
    try {
      const c = await cfg(); const min = minNaoAtendeAuto(c), prazo = prazoLoja(c);
      // Janela de 30 min depois do prazo: entrega parada ha muito mais tempo (ex.: de antes deste gatilho existir) fica como esta
      const limite = new Date(Date.now() - min * 60000).toISOString(), desde = new Date(Date.now() - (min + 30) * 60000).toISOString();
      const r = await pool.query("UPDATE orders SET rota_nao_atende_em=NOW(), rota_nao_atende_admin=false, rota_nao_atende_auto_min=$2, rota_nao_atende_prazo_min=$4 WHERE rota_id IS NOT NULL AND status='no_cliente' AND t_no_cliente IS NOT NULL AND t_no_cliente < $1 AND t_no_cliente > $3 AND rota_nao_atende_em IS NULL AND rota_decisao IS NULL AND rota_retorno IS NULL RETURNING id, rota_id, motoboy_id, motoboy_name", [limite, min, desde, prazo]);
      r.rows.forEach((o) => {
        ctx.registrarEvento(o.id, o.motoboy_id, o.motoboy_name, 'nao_atende', 'No cliente há ' + min + ' min sem finalizar: loja chamada para decidir (automático). A loja tem ' + prazo + ' min.');
        if (o.motoboy_id) pushMotoboy(o.motoboy_id, '⏱️ Pedido #' + o.id + ': a loja foi chamada', 'Você está no cliente há ' + min + ' min e ainda não finalizou. A loja foi chamada para decidir se o pedido fica com você ou volta para a loja. Se conseguir entregar, finalize normalmente.');
        console.log('[ROTA] Pedido #' + o.id + ': ' + min + ' min no cliente sem finalizar -> decisão da loja (automático).');
      });
    } catch (e) { console.error('[ROTA] gatilho nao atende:', e.message); }
  }
  // Passou o prazo da loja (o que valeu para o pedido; padrao 5 min, antigos 15) sem decisao: vai para o admin
  async function verificarNaoAtende() {
    await gatilhoNaoAtende();
    try {
      const r = await pool.query("UPDATE orders SET rota_nao_atende_admin=true WHERE rota_nao_atende_em IS NOT NULL AND rota_decisao IS NULL AND rota_nao_atende_admin IS NOT TRUE AND rota_nao_atende_em < NOW() - (COALESCE(rota_nao_atende_prazo_min, 15) * INTERVAL '1 minute') AND status IN ('coletado','no_cliente') RETURNING id, rota_id, loja_name, loja_user, motoboy_name, COALESCE(rota_nao_atende_prazo_min, 15) AS prazo");
      r.rows.forEach((o) => { avisarAdmin('⏰ Cliente não atende - a loja não decidiu em ' + o.prazo + ' min\nPedido #' + o.id + ' (rota iFood #' + o.rota_id + ')\nLoja: ' + (ctx.txtPuro(o.loja_name) || o.loja_user) + '\nMotoboy: ' + (o.motoboy_name || '-') + '\nResolva no painel admin (aba Rota iFood).'); console.log('[ROTA] Pedido #' + o.id + ': cliente não atende sem decisão da loja em ' + o.prazo + ' min -> admin.'); });
    } catch (e) { console.error('[ROTA] verificar nao atende:', e.message); }
  }

  // ---------- socorro do motoboy ----------
  app.post('/rotas/:rotaId/pedir-socorro', h(async (req, res) => {
    const rotaId = parseInt(req.params.rotaId) || 0; const ps = vivos(await pedidosDaRota(rotaId)); if (!ps.length) throw erro('Rota não encontrada.', 404);
    const q = quem(req); const meus = ps.filter((p) => STATUS_ATIVOS.includes(p.status));
    if (!q || q.papel !== 'motoboy' || !meus.length || String(meus[0].motoboy_id) !== String(q.id)) throw erro('Esta rota não está com você.', 403);
    await pool.query('UPDATE orders SET rota_socorro_pedido_em=NOW() WHERE rota_id=$1 AND status = ANY($2)', [rotaId, STATUS_ATIVOS]);
    avisarAdmin('🆘 Motoboy pediu socorro na rota iFood #' + rotaId + '\nMotoboy: ' + (meus[0].motoboy_name || '-') + '\nLoja: ' + (ctx.txtPuro(meus[0].loja_name) || meus[0].loja_user) + '\nEntregas que faltam: ' + meus.length + '\nUse "Socorro do Motoboy" no painel admin (aba Rota iFood).');
    res.json({ ok: true });
  }));
  app.post('/admin/rotas/:rotaId/socorro', h(async (req, res) => {
    const q = exigirAdmin(req, 'Só o admin pode acionar o socorro.');
    const rotaId = parseInt(req.params.rotaId) || 0;
    const faltam = vivos(await pedidosDaRota(rotaId)).filter((p) => STATUS_ATIVOS.includes(p.status));
    if (!faltam.length) throw erro('Não há entregas pendentes nesta rota.', 409);
    const mbId = faltam[0].motoboy_id, mbNome = faltam[0].motoboy_name; if (!mbId) throw erro('A rota não está com nenhum motoboy.', 409);
    const loc = (await pool.query('SELECT lat, lng, updated_at FROM motoboy_localizacao WHERE motoboy_id=$1', [mbId])).rows[0];
    if (!loc) throw erro('Ainda não há localização do motoboy ' + (mbNome || mbId) + '.', 409);
    const minAtras = Math.max(0, Math.round((Date.now() - new Date(loc.updated_at).getTime()) / 60000));
    const c = await cfg();
    const cot = await cotar(c, { lat: loc.lat, lng: loc.lng }, faltam.map((p) => ({ lat: p.entrega_lat, lng: p.entrega_lng }))); const vals = porEntrega(cot);
    const aprox = await enderecoAproximado(loc.lat, loc.lng);
    if (!(req.body || {}).confirmar) return res.json({ ok: true, preview: true, total: cot.total, km_total: cot.km_total, entregas: faltam.length, motoboy: mbNome, posicao: { lat: loc.lat, lng: loc.lng }, atualizada_ha_min: minAtras, endereco_aproximado: aprox });
    const coordTxt = parseFloat(loc.lat).toFixed(5) + ',' + parseFloat(loc.lng).toFixed(5);
    const horaLoc = new Date(loc.updated_at).toLocaleTimeString('pt-BR', { timeZone: 'America/Sao_Paulo', hour: '2-digit', minute: '2-digit' });
    const cli = await pool.connect(); let novaRota = null; const novos = [];
    try {
      await cli.query('BEGIN');
      const trava = await cli.query("UPDATE orders SET status='cancelado', cancelado_por='socorro', rota_socorro_info=$3 WHERE rota_id=$1 AND motoboy_id=$2 AND status = ANY($4) RETURNING id", [rotaId, mbId, 'Socorro do motoboy: entrega passada para outro motoboy', STATUS_ATIVOS]);
      if (trava.rows.length !== faltam.length) throw erro('A rota mudou enquanto o socorro era aberto. Atualize e tente de novo.', 409);
      for (let i = 0; i < faltam.length; i++) {
        const p = faltam[i], v = vals[i];
        const ins = await cli.query(
          `INSERT INTO orders (loja_user, loja_name, plataforma, endereco_coleta, bairro_coleta, complemento_coleta, obs_coleta, endereco_entrega, bairro_destino, complemento_entrega,
             nome_cliente, telefone_cliente, cod_pedido, cobrar_cliente, tipo_pagamento, valor_pedido, valor_total, valor_motoboy, comissao, distancia, status, pending_until, telefone_loja,
             launch_at, delivery_code, taxa_extra_chuva, taxa_extra_noturna, chuva_desconto_de, ifood_localizador, ifood_numero_pedido, ifood_nome_cliente,
             coleta_lat, coleta_lng, entrega_lat, entrega_lng, rota_id, rota_ordem, rota_pronto, rota_socorro, rota_socorro_de, rota_socorro_info, created_at)
           VALUES ($1,'Socorro do Motoboy','socorro',$2,'Socorro',$3,$4,$5,$6,$7,$8,$9,'','nao','ifood',0,$10,$11,$12,$13,$14,$15,$16,0,$17,0,$18,'admin',$19,$20,$21,$22,$23,$24,$25,$26,$27,true,true,$28,$29, NOW() - ($30 * INTERVAL '1 second'))
           RETURNING id`,
          [p.loja_user, coordTxt, 'Motoboy parado: ' + (mbNome || '') + (aprox ? ' (perto de ' + aprox + ')' : ''), 'Localização de ' + horaLoc + ' (há ' + minAtras + ' min) - pegue os pedidos com o motoboy parado',
            p.endereco_entrega, p.bairro_destino, p.complemento_entrega, p.nome_cliente, p.telefone_cliente, v.valor_total, v.valor_motoboy, v.comissao, v.distancia,
            i === 0 ? 'pendente' : 'rota_espera', Date.now() + 15000, p.telefone_loja, String(Math.floor(1000 + Math.random() * 9000)), i === 0 ? cot.extras.noturna : 0,
            p.ifood_localizador, p.ifood_numero_pedido, p.ifood_nome_cliente, loc.lat, loc.lng, p.entrega_lat, p.entrega_lng, novaRota, i + 1, mbId, 'Socorro da rota #' + rotaId + ' (pedido original #' + p.id + ')', i]
        );
        const id = ins.rows[0].id; novos.push(id);
        if (i === 0) { novaRota = id; await cli.query('UPDATE orders SET rota_id=$1 WHERE id=$1', [id]); }
      }
      await cli.query('UPDATE platform_wallet SET balance = balance - $1, total_sacado = total_sacado + $1, updated_at=NOW() WHERE id=1', [cot.total]);
      await cli.query("INSERT INTO platform_events (tipo, valor, descricao, order_id) VALUES ('socorro_motoboy', $1, $2, $3)", [cot.total, 'Socorro do motoboy ' + (mbNome || mbId) + ': rota #' + novaRota + ' (' + faltam.length + ' entregas da rota #' + rotaId + ')', novaRota]);
      await cli.query('UPDATE users SET socorros_count = COALESCE(socorros_count,0) + 1 WHERE id=$1', [mbId]);
      await cli.query('COMMIT');
    } catch (e) { try { await cli.query('ROLLBACK'); } catch (e2) {} throw e; }
    finally { cli.release(); }
    faltam.forEach((p) => ctx.registrarEvento(p.id, mbId, mbNome, 'socorro', 'Socorro do motoboy: entrega passada para a rota #' + novaRota + ' (o motoboy recebe só pelas entregas que já fez)'));
    pushMotoboy(mbId, '🆘 Socorro a caminho', 'As ' + faltam.length + ' entregas que faltavam foram passadas para outro motoboy. Aguarde no local para entregar os pedidos a ele.');
    console.log('[ROTA] Socorro: rota #' + rotaId + ' -> nova rota #' + novaRota + ' (' + faltam.length + ' entregas, ' + fmtR(cot.total) + ' pago pelo caixa).');
    avisarRotaDisponivel(novaRota, 'socorro');
    // a rota original terminou (o que faltava foi para o socorro): promocoes contam 1 para quem entregou nela
    if (ctx.premiacaoRota) await ctx.premiacaoRota(rotaId);
    res.json({ ok: true, rota_nova: novaRota, ids: novos, total: cot.total });
  }));

  // ---------- admin ----------
  app.get('/admin/rotas', h(async (req, res) => {
    const q = exigirAdmin(req, 'Só o admin.');
    const c = await cfg();
    const ativas = (await pool.query("SELECT * FROM orders WHERE rota_id IN (SELECT DISTINCT rota_id FROM orders WHERE rota_id IS NOT NULL AND status IN ('em_preparo','rota_espera','pendente','aceito','na_loja','coletado','no_cliente')) ORDER BY rota_id DESC, rota_ordem NULLS LAST, id")).rows;
    const rotas = {}; ativas.forEach((o) => { (rotas[o.rota_id] = rotas[o.rota_id] || []).push(o); });
    const casos = (await pool.query("SELECT id, rota_id, loja_name, loja_user, motoboy_name, nome_cliente, rota_nao_atende_em, rota_nao_atende_admin, rota_nao_atende_auto_min, rota_nao_atende_prazo_min, status FROM orders WHERE rota_nao_atende_em IS NOT NULL AND rota_decisao IS NULL AND status IN ('coletado','no_cliente') ORDER BY rota_nao_atende_em")).rows;
    const socorros = (await pool.query("SELECT id, name, custom_id, socorros_count FROM users WHERE role='motoboy' AND COALESCE(socorros_count,0) > 0 ORDER BY socorros_count DESC, name")).rows;
    res.json({ config: { ativa: rotaLigada(c), valor_ponto: valorPonto(c), max_pontos: maxPontos(c), nao_atende_min: minNaoAtendeAuto(c), prazo_loja_min: prazoLoja(c) }, rotas: Object.keys(rotas).map((k) => ({ rota_id: parseInt(k), entregas: rotas[k] })).sort((a, b) => b.rota_id - a.rota_id), casos_nao_atende: casos, motoboys_socorros: socorros });
  }));
  app.put('/settings/rota-ifood', h(async (req, res) => {
    const q = exigirAdmin(req, 'Só o admin pode mudar a Rota iFood.');
    const b = req.body || {}; const c = await cfg();
    const ativa = b.ativa === undefined ? rotaLigada(c) : (b.ativa === true || b.ativa === 'true');
    const vp = b.valor_ponto === undefined ? valorPonto(c) : parseFloat(String(b.valor_ponto).replace(',', '.'));
    const mp = b.max_pontos === undefined ? maxPontos(c) : parseInt(b.max_pontos);
    if (isNaN(vp) || vp < 0 || vp > 100) throw erro('Valor por ponto inválido (0 a 100).');
    if (isNaN(mp) || mp < 2 || mp > 10) throw erro('Máximo de pontos inválido (2 a 10).');
    const na = b.nao_atende_min === undefined ? minNaoAtendeAuto(c) : parseInt(b.nao_atende_min);
    if (isNaN(na) || na < 1 || na > 120) throw erro('Tempo do "cliente não atende" automático inválido (1 a 120 min).');
    const pl = b.prazo_loja_min === undefined ? prazoLoja(c) : parseInt(b.prazo_loja_min);
    if (isNaN(pl) || pl < 1 || pl > 60) throw erro('Prazo da loja para decidir inválido (1 a 60 min).');
    const r = await pool.query('UPDATE settings SET rota_ifood_ativa=$1, rota_valor_ponto=$2, rota_max_pontos=$3, rota_nao_atende_auto_min=$4, rota_prazo_loja_min=$5 WHERE id=1 RETURNING rota_ifood_ativa, rota_valor_ponto, rota_max_pontos, rota_nao_atende_auto_min, rota_prazo_loja_min', [ativa, vp, mp, na, pl]);
    console.log('[ROTA] Configuração: ligada=' + ativa + ', valor por ponto=' + vp + ', máx pontos=' + mp + ', não atende automático=' + na + ' min');
    res.json({ ok: true, ativa: r.rows[0].rota_ifood_ativa, valor_ponto: parseFloat(r.rows[0].rota_valor_ponto), max_pontos: r.rows[0].rota_max_pontos, nao_atende_min: r.rows[0].rota_nao_atende_auto_min, prazo_loja_min: r.rows[0].rota_prazo_loja_min });
  }));

  const intervalo = setInterval(verificarNaoAtende, 60 * 1000); if (intervalo.unref) intervalo.unref();
  return { criarColunas, antesDoPut, depoisDoPut, paraApp, timeoutRota, lancarRota, devolverRota, verificarNaoAtende, _cotar: cotar, _cfg: cfg };
};
