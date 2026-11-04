'use strict';
// ===== CARDAPIO COM A FLASH DROP FECHADA =====
// Com a chave do admin ligada e a Flash Drop fechada, o cardapio da loja aceita pedidos, entregues pela propria loja.
// O servidor decide e marca so_cardapio=true (nao confia no navegador). Antes do aceite a loja recebe so nome e telefone;
// ao aceitar, o servidor debita a taxa do saldo numa operacao so e libera os detalhes. A taxa nao volta em nenhum caso.
// Esses pedidos nunca vao para os motoboys, nao usam o PUT /orders e ficam fora de promocoes, ranking, emprestimo e contadores.
// Com a chave desligada ou a Flash Drop aberta, nada deste arquivo age: o pedido segue o caminho normal.
module.exports = function instalarCardapioFechado(ctx) {
  const { app, pool, axios, sanitize, tokenDe, adminDe, estadoFuncionamento, configFuncionamento, requisicaoDoAdmin, saldoBloqueadoLoja, lojaBotUrl, lojaBotSecret } = ctx;
  const r2 = (v) => Math.round((parseFloat(v) || 0) * 100) / 100;
  const fmtR = (v) => 'R$ ' + r2(v).toFixed(2).replace('.', ',');
  const TAXA_PADRAO = 0.5;
  const TIPOS_PAGAMENTO = ['dinheiro', 'cartao_credito', 'cartao_debito', 'cartao_aproximacao', 'maquina', 'pix'];
  const MSG_NAO_LANCA = 'Este pedido é do cardápio fora do horário: a entrega é feita pela loja e ele não pode ir para os motoboys.';
  function erro(msg, status, extra) { const e = new Error(msg); e.status = status || 400; e.extra = extra || null; return e; }
  const h = (fn) => async (req, res) => {
    try { await fn(req, res); }
    catch (e) { if (!e.status) console.error('[CARDAPIO_FECHADO] Erro:', e.stack || e.message); if (!res.headersSent) res.status(e.status || 500).json(Object.assign({ error: e.status ? e.message : 'Erro no cardápio fora do horário: ' + e.message }, e.extra || {})); }
  };

  // ---------- banco ----------
  async function criarColunas() {
    const qs = [
      'ALTER TABLE settings ADD COLUMN IF NOT EXISTS cardapio_fechado_ativo BOOLEAN DEFAULT false',
      'ALTER TABLE settings ADD COLUMN IF NOT EXISTS cardapio_fechado_taxa NUMERIC(10,2) DEFAULT 0.50',
      "ALTER TABLE users ADD COLUMN IF NOT EXISTS loja_entrega_modo VARCHAR(12) DEFAULT 'plataforma'",
      'ALTER TABLE users ADD COLUMN IF NOT EXISTS loja_entrega_valor NUMERIC(10,2) DEFAULT 0',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS so_cardapio BOOLEAN DEFAULT false',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS so_cardapio_aceito_em TIMESTAMP',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS so_cardapio_taxa NUMERIC(10,2)',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS so_cardapio_frete NUMERIC(10,2)',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS so_cardapio_retirada BOOLEAN DEFAULT false',
      // Retirada e opcao da loja (padrao NAO). "Sem valor de entrega": a loja informa o valor depois de aceitar.
      'ALTER TABLE users ADD COLUMN IF NOT EXISTS loja_permite_retirada BOOLEAN DEFAULT false',
      'ALTER TABLE orders ADD COLUMN IF NOT EXISTS so_cardapio_frete_loja BOOLEAN DEFAULT false',
      // O "valor proprio fixo" saiu: quem tinha passa para "Sem valor de entrega"
      "UPDATE users SET loja_entrega_modo='sem_valor' WHERE loja_entrega_modo='proprio'"
    ];
    for (const q of qs) { try { await pool.query(q); } catch (e) { console.error('[CARDAPIO_FECHADO] coluna:', e.message); } }
  }
  async function configuracao() {
    const r = await pool.query('SELECT cardapio_fechado_ativo, cardapio_fechado_taxa FROM settings WHERE id=1');
    const c = r.rows[0] || {}; const t = parseFloat(c.cardapio_fechado_taxa);
    return { ativo: c.cardapio_fechado_ativo === true, taxa: isNaN(t) || t < 0 ? TAXA_PADRAO : r2(t) };
  }
  // modo = chave ligada E Flash Drop fechada
  async function modoAgora() {
    const c = await configuracao();
    const est = estadoFuncionamento(await configFuncionamento(), Date.now());
    const fechado = est.aberto_agora === false;
    return { ativo: c.ativo, taxa: c.taxa, fechado, modo: c.ativo && fechado };
  }
  const modoEntrega = (loja) => ((loja && loja.loja_entrega_modo) === 'plataforma' || !(loja && loja.loja_entrega_modo) ? 'plataforma' : 'sem_valor');
  const temMpProprio = (loja) => !!String((loja && loja.mp_access_token) || '').trim();

  // ---------- criacao do pedido (POST /orders) ----------
  // Devolve true quando ja respondeu. Qualquer falha ao decidir cai no caminho normal (fechado continua bloqueado).
  async function antesDoPost(req, res) {
    const d = req.body || {};
    if (d.plataforma !== 'cardapio') return false;
    let m;
    try { if (requisicaoDoAdmin(req)) return false; m = await modoAgora(); if (!m.modo) return false; }
    catch (e) { console.error('[CARDAPIO_FECHADO] Erro ao decidir:', e.message); return false; }
    try {
      const lr = await pool.query("SELECT username, name, phone, address, loja_online, mp_access_token, loja_entrega_modo, loja_permite_retirada FROM users WHERE username=$1 AND role='loja'", [d.loja_user]);
      const loja = lr.rows[0];
      if (!loja) { res.status(404).json({ error: 'Loja não encontrada.' }); return true; }
      if (!loja.loja_online) { res.status(403).json({ error: 'A loja está offline no momento.', loja_offline: true }); return true; }
      const tipo = String(d.tipo_pagamento || '');
      if (tipo === 'pix' && !temMpProprio(loja)) {
        // PIX gerado antes de fechar (conta do Flash Drop): segue o caminho de sempre. PIX novo sem Mercado Pago da loja nao entra.
        if (d.pix_payment_id) return false;
        res.status(403).json({ error: 'Esta forma de pagamento não está disponível agora. Escolha outra.' }); return true;
      }
      if (TIPOS_PAGAMENTO.indexOf(tipo) === -1) { res.status(400).json({ error: 'Esta forma de pagamento não está disponível agora. Escolha outra.' }); return true; }
      const nome = String(sanitize(d.nome_cliente) || '').trim(), tel = String(d.telefone_cliente || '').trim();
      if (!nome || tel.replace(/\D/g, '').length < 10) { res.status(400).json({ error: 'Informe o nome e o celular.' }); return true; }
      const retirada = d.entrega_tipo === 'retirada';
      if (retirada && loja.loja_permite_retirada !== true) { res.status(400).json({ error: 'Esta loja não faz retirada. Informe o endereço de entrega.' }); return true; }
      // Sem valor de entrega: o frete fica vazio e a loja informa depois de aceitar (ja vendo o endereco)
      const freteLoja = !retirada && modoEntrega(loja) === 'sem_valor';
      const frete = retirada ? 0 : (freteLoja ? null : Math.max(0, r2(d.valor_total)));
      const num = (v) => (v !== undefined && v !== null && v !== '' && !isNaN(parseFloat(v))) ? parseFloat(v) : null;
      const ins = await pool.query(
        `INSERT INTO orders (loja_user, loja_name, plataforma, endereco_coleta, bairro_coleta, endereco_entrega, bairro_destino, complemento_entrega, nome_cliente, telefone_cliente, cpf_cliente, cod_pedido,
           cobrar_cliente, tipo_pagamento, valor_pedido, valor_total, valor_motoboy, comissao, distancia, status, launch_at, telefone_loja, obs_coleta, obs_entrega_loja, coleta_lat, coleta_lng, entrega_lat, entrega_lng,
           so_cardapio, so_cardapio_taxa, so_cardapio_frete, so_cardapio_retirada, so_cardapio_frete_loja)
         VALUES ($1,$2,'cardapio',$3,$4,$5,$6,$7,$8,$9,$10,$11,'nao',$12,$13,0,0,0,$14,'novo',0,$15,$16,$17,$18,$19,$20,$21,true,$22,$23,$24,$25) RETURNING id`,
        [loja.username, loja.name, d.endereco_coleta || null, d.bairro_coleta || null, retirada ? 'Retirada na loja' : sanitize(d.endereco_entrega), retirada ? null : (d.bairro_destino || null), retirada ? null : sanitize(d.complemento_entrega),
          nome, tel, d.cpf_cliente || null, d.cod_pedido || null, tipo, r2(d.valor_pedido), retirada ? 0 : num(d.distancia), loja.phone || null, d.obs_coleta || null, d.obs_entrega_loja || null,
          num(d.coleta_lat), num(d.coleta_lng), retirada ? null : num(d.entrega_lat), retirada ? null : num(d.entrega_lng), m.taxa, frete, retirada, freteLoja]
      );
      console.log('[CARDAPIO_FECHADO] Pedido #' + ins.rows[0].id + ' recebido para a loja ' + loja.username);
      res.json({ ok: true, id: ins.rows[0].id, so_cardapio: true, status: 'novo' });
      return true;
    } catch (e) {
      console.error('[CARDAPIO_FECHADO] Erro ao criar:', e.message);
      res.status(500).json({ error: 'Não foi possível enviar o pedido. Tente novamente.' });
      return true;
    }
  }

  // Checkout PIX com a Flash Drop fechada: so com o Mercado Pago da propria loja (o dinheiro nao pode cair na conta do Flash Drop)
  async function checkoutLiberado(req) {
    try {
      const m = await modoAgora(); if (!m.modo) return false;
      const lr = await pool.query("SELECT loja_online, mp_access_token FROM users WHERE id=$1 AND role='loja'", [(req.body || {}).loja_id]);
      return !!(lr.rows[0] && lr.rows[0].loja_online && temMpProprio(lr.rows[0]));
    } catch (e) { console.error('[CARDAPIO_FECHADO] checkout:', e.message); return false; }
  }

  // ---------- visao dos dados ----------
  // Lista de pedidos: so_cardapio so aparece para a loja dona (com o login dela). Antes do aceite: so nome e telefone.
  function ocultar(o) {
    return { id: o.id, status: o.status, plataforma: o.plataforma, so_cardapio: true, so_cardapio_aceito_em: null, so_cardapio_taxa: o.so_cardapio_taxa, loja_user: o.loja_user, loja_name: o.loja_name, nome_cliente: o.nome_cliente, telefone_cliente: o.telefone_cliente, created_at: o.created_at };
  }
  function filtrarLista(rows, req) {
    if (!rows.some((o) => o.so_cardapio)) return rows;
    const lj = tokenDe(req, 'loja');
    const out = [];
    rows.forEach((o) => {
      if (!o.so_cardapio) { out.push(o); return; }
      if (!lj || lj.username !== o.loja_user) return;
      out.push(o.so_cardapio_aceito_em ? o : ocultar(o));
    });
    return out;
  }
  // Rotas de pedido que nao valem para so_cardapio (PUT /orders, lancar, apagar...). Devolve true quando ja respondeu.
  async function recusarSeSoCardapio(id, res) {
    try {
      const r = await pool.query('SELECT so_cardapio FROM orders WHERE id=$1', [parseInt(id) || 0]);
      if (r.rows[0] && r.rows[0].so_cardapio) { res.status(403).json({ error: MSG_NAO_LANCA, so_cardapio: true }); return true; }
    } catch (e) { console.error('[CARDAPIO_FECHADO] verificar:', e.message); }
    return false;
  }

  // ---------- avisos ao cliente (mesmo bot do cardapio) ----------
  function avisarCliente(o, msg) {
    try {
      if (!o.telefone_cliente || !axios) return;
      axios.post(lojaBotUrl + '/api/send-message', { lojaId: String(o.loja_user), phone: o.telefone_cliente, message: msg }, { headers: { 'x-bot-secret': lojaBotSecret } }).catch((e) => console.error('[CARDAPIO_FECHADO] aviso cliente:', e.message));
    } catch (e) { console.error('[CARDAPIO_FECHADO] aviso cliente:', e.message); }
  }
  function msgConfirmado(o) {
    // O endereco do cardapio ja traz o bairro e o complemento
    const base = String(o.endereco_entrega || '');
    const endereco = [base].concat([o.complemento_entrega, o.bairro_destino].filter((x) => x && base.indexOf(x) === -1)).filter(Boolean).join(', ');
    return '📋 Pedido confirmado! #' + o.id + '\n\n' +
      'Seu pedido foi recebido com sucesso e já está em preparação. 🧑‍🍳🍔\n\n' +
      (o.so_cardapio_retirada
        ? '🏪 Retire o pedido na loja.\n' + (o.endereco_coleta ? '📍 Endereço da loja: ' + o.endereco_coleta + '\n' : '')
        : '🛵 A entrega será feita pela loja.\n📍 Endereço de entrega: ' + endereco + '\n' + (o.so_cardapio_frete_loja ? '🧾 A loja vai informar o valor da entrega.\n' : '')) +
      '💰 Forma de pagamento: ' + o.tipo_pagamento + '\n\n' +
      '⏳ Agora é só aguardar! 🎉';
  }

  function msgFrete(o, frete) {
    const total = r2((parseFloat(o.valor_pedido) || 0) + frete);
    return '🛵 Pedido #' + o.id + '\n\nTaxa de entrega: ' + fmtR(frete) + '\nTotal do pedido: ' + fmtR(total) +
      (o.tipo_pagamento === 'pix' ? '\n\nOs itens já foram pagos online. Combine com a loja o pagamento da entrega.' : '');
  }

  // ---------- acoes da loja ----------
  function lojaDoToken(req) { const lj = tokenDe(req, 'loja'); if (!lj) throw erro('Faça login de novo na loja.', 401); return lj; }
  // Aceitar: debita a taxa e libera os detalhes, tudo numa operacao so
  app.post('/cardapio-fechado/:id/aceitar', h(async (req, res) => {
    const lj = lojaDoToken(req); const id = parseInt(req.params.id) || 0;
    const bloq = await Promise.resolve(saldoBloqueadoLoja(lj.username)).then((v) => parseFloat(v) || 0).catch(() => 0);
    const cli = await pool.connect(); let pedido, taxa;
    try {
      await cli.query('BEGIN');
      const or = await cli.query('SELECT * FROM orders WHERE id=$1 FOR UPDATE', [id]); const o = or.rows[0];
      if (!o || !o.so_cardapio) throw erro('Pedido não encontrado.', 404);
      if (o.loja_user !== lj.username) throw erro('Este pedido não é da sua loja.', 403);
      if (o.status !== 'novo') throw erro('Este pedido já foi tratado.', 409);
      taxa = r2(o.so_cardapio_taxa);
      const ur = await cli.query('SELECT id, credit FROM users WHERE username=$1 FOR UPDATE', [lj.username]); const loja = ur.rows[0];
      if (!loja) throw erro('Loja não encontrada.', 404);
      const disponivel = r2((parseFloat(loja.credit) || 0) - bloq);
      if (disponivel < taxa) throw erro('Recarregue seu saldo para ver e aceitar o pedido.', 402, { sem_saldo: true, taxa, faltante: r2(taxa - disponivel) });
      if (taxa > 0) {
        await cli.query('UPDATE users SET credit = credit - $1 WHERE id=$2', [taxa, loja.id]);
        const desc = 'Taxa cardápio fora do horário – Pedido #' + id + ' – ' + fmtR(taxa);
        await cli.query('INSERT INTO loja_wallet_events (loja_id, tipo, valor, descricao, order_id) VALUES ($1,$2,$3,$4,$5)', [loja.id, 'taxa_cardapio_fechado', taxa, desc, id]);
        await cli.query('UPDATE platform_wallet SET balance = balance + $1, total_ganho = total_ganho + $1, updated_at=NOW() WHERE id=1', [taxa]);
        await cli.query('INSERT INTO platform_events (tipo, valor, descricao, order_id) VALUES ($1,$2,$3,$4)', ['receita_cardapio_fechado', taxa, desc + ' (' + (o.loja_name || o.loja_user) + ')', id]);
      }
      const up = await cli.query("UPDATE orders SET status='em_preparo', plataforma='cardapio_aceito', so_cardapio_aceito_em=NOW(), t_em_preparo=NOW(), launch_at=0 WHERE id=$1 RETURNING *", [id]);
      pedido = up.rows[0];
      await cli.query('COMMIT');
    } catch (e) { try { await cli.query('ROLLBACK'); } catch (e2) {} throw e; }
    finally { cli.release(); }
    avisarCliente(pedido, msgConfirmado(pedido));
    console.log('[CARDAPIO_FECHADO] Pedido #' + id + ' aceito por ' + lj.username + ' (taxa ' + fmtR(taxa) + ')');
    res.json({ ok: true, taxa, pedido });
  }));
  // "Sem valor de entrega": depois de aceitar (ja vendo o endereco) a loja informa o valor deste pedido. Pode corrigir ate entregar.
  app.post('/cardapio-fechado/:id/frete', h(async (req, res) => {
    const lj = lojaDoToken(req); const id = parseInt(req.params.id) || 0;
    const valor = parseFloat(String((req.body || {}).valor === undefined || (req.body || {}).valor === null ? '' : req.body.valor).replace(',', '.'));
    if (isNaN(valor) || valor < 0 || valor > 500) throw erro('Informe um valor de entrega entre R$ 0,00 e R$ 500,00.');
    const or = await pool.query('SELECT loja_user, so_cardapio FROM orders WHERE id=$1', [id]); const o0 = or.rows[0];
    if (!o0 || !o0.so_cardapio) throw erro('Pedido não encontrado.', 404);
    if (o0.loja_user !== lj.username) throw erro('Este pedido não é da sua loja.', 403);
    const r = await pool.query("UPDATE orders SET so_cardapio_frete=$2 WHERE id=$1 AND so_cardapio=true AND so_cardapio_frete_loja=true AND status='em_preparo' AND so_cardapio_aceito_em IS NOT NULL RETURNING *", [id, r2(valor)]);
    if (!r.rows.length) throw erro('Não dá para informar a entrega deste pedido agora.', 409);
    avisarCliente(r.rows[0], msgFrete(r.rows[0], r2(valor)));
    res.json({ ok: true, frete: r2(valor), total: r2((parseFloat(r.rows[0].valor_pedido) || 0) + r2(valor)) });
  }));
  // Muda o status so se o pedido estiver no estado esperado (duas abas/cliques nao fazem nada duas vezes)
  async function mudarStatus(req, sql, condicao, msgCondicao) {
    const lj = lojaDoToken(req); const id = parseInt(req.params.id) || 0;
    const or = await pool.query('SELECT id, loja_user, so_cardapio FROM orders WHERE id=$1', [id]); const o = or.rows[0];
    if (!o || !o.so_cardapio) throw erro('Pedido não encontrado.', 404);
    if (o.loja_user !== lj.username) throw erro('Este pedido não é da sua loja.', 403);
    const r = await pool.query('UPDATE orders SET ' + sql + ' WHERE id=$1 AND so_cardapio=true AND ' + condicao + ' RETURNING *', [id]);
    if (!r.rows.length) throw erro(msgCondicao, 409);
    return r.rows[0];
  }
  // Recusar (nunca foi aceito): nao paga nada
  app.post('/cardapio-fechado/:id/recusar', h(async (req, res) => {
    const o = await mudarStatus(req, "status='cancelado', cancelado_por='loja'", "status='novo'", 'Este pedido já foi tratado.');
    avisarCliente(o, '❌ Pedido #' + o.id + '\n\nA loja ' + (o.loja_name || '') + ' não pôde aceitar o seu pedido. Fale com a loja se precisar.');
    res.json({ ok: true });
  }));
  // Cancelar depois de aceito: a taxa NAO volta (e nao ha taxa de cancelamento: nao houve corrida)
  app.post('/cardapio-fechado/:id/cancelar', h(async (req, res) => {
    const o = await mudarStatus(req, "status='cancelado', cancelado_por='loja'", "status='em_preparo' AND so_cardapio_aceito_em IS NOT NULL", 'Este pedido não pode mais ser cancelado.');
    avisarCliente(o, '❌ Pedido #' + o.id + '\n\nA loja ' + (o.loja_name || '') + ' cancelou o seu pedido. Fale com a loja se precisar.');
    res.json({ ok: true });
  }));
  app.post('/cardapio-fechado/:id/entregue', h(async (req, res) => {
    await mudarStatus(req, "status='entregue', t_entregue=NOW()", "status='em_preparo' AND so_cardapio_aceito_em IS NOT NULL", 'Este pedido não pode ser marcado como entregue.');
    res.json({ ok: true });
  }));

  // ---------- configuracao ----------
  // Publico: o que o cardapio precisa saber (nada de token nem de saldo)
  app.get('/cardapio-fechado/estado', h(async (req, res) => {
    const m = await modoAgora();
    const lr = await pool.query("SELECT loja_entrega_modo, loja_permite_retirada, mp_access_token FROM users WHERE id=$1 AND role='loja'", [parseInt(req.query.loja_id) || 0]);
    const l = lr.rows[0] || {};
    res.json({ modo: m.modo, entrega_modo: modoEntrega(l), permite_retirada: l.loja_permite_retirada === true, tem_mp_proprio: temMpProprio(l) });
  }));
  // Loja: "Valor da plataforma" ou "Sem valor de entrega"; "Permitir retirada na loja" (padrao NAO)
  app.post('/loja/cardapio-entrega', h(async (req, res) => {
    const lj = lojaDoToken(req); const b = req.body || {};
    const modo = b.modo === undefined ? undefined : (b.modo === 'plataforma' || b.modo === 'sem_valor' ? b.modo : null);
    if (modo === null) throw erro('Escolha como cobrar a entrega.');
    const ret = b.permite_retirada === undefined ? undefined : b.permite_retirada === true;
    const r = await pool.query('UPDATE users SET loja_entrega_modo=COALESCE($1, loja_entrega_modo), loja_permite_retirada=COALESCE($2, loja_permite_retirada) WHERE id=$3 RETURNING loja_entrega_modo, loja_permite_retirada', [modo === undefined ? null : modo, ret === undefined ? null : ret, lj.id]);
    res.json({ ok: true, entrega_modo: modoEntrega(r.rows[0]), permite_retirada: r.rows[0].loja_permite_retirada === true });
  }));
  function exigirAdmin(req) { const a = adminDe(req); if (!a.user) throw erro('Só o admin pode fazer isso.', a.erro === 'nao_admin' ? 403 : 401, { motivo: a.erro }); return a.user; }
  app.put('/settings/cardapio-fechado', h(async (req, res) => {
    exigirAdmin(req); const b = req.body || {}; const c = await configuracao();
    const ativo = typeof b.ativo === 'boolean' ? b.ativo : c.ativo;
    const taxa = b.taxa === undefined ? c.taxa : parseFloat(String(b.taxa).replace(',', '.'));
    if (isNaN(taxa) || taxa < 0 || taxa > 100) throw erro('Taxa inválida (R$ 0,00 a R$ 100,00).');
    const r = await pool.query('UPDATE settings SET cardapio_fechado_ativo=$1, cardapio_fechado_taxa=$2 WHERE id=1 RETURNING cardapio_fechado_ativo, cardapio_fechado_taxa', [ativo, r2(taxa)]);
    console.log('[CARDAPIO_FECHADO] Configuração: ativo=' + ativo + ', taxa=' + fmtR(taxa));
    res.json({ ok: true, ativo: r.rows[0].cardapio_fechado_ativo, taxa: r2(r.rows[0].cardapio_fechado_taxa) });
  }));
  // Admin: pedidos e arrecadacao (a taxa vem do extrato das lojas, que nao e apagado com o pedido)
  app.get('/admin/cardapio-fechado/resumo', h(async (req, res) => {
    exigirAdmin(req);
    const c = await configuracao();
    const soma = async (filtro) => { const r = await pool.query("SELECT COUNT(*)::int AS qtd, COALESCE(SUM(valor),0) AS total FROM loja_wallet_events WHERE tipo='taxa_cardapio_fechado'" + filtro); return { qtd: r.rows[0].qtd, total: r2(r.rows[0].total) }; };
    const geral = await soma('');
    const mes = await soma(" AND created_at >= date_trunc('month', NOW() AT TIME ZONE 'America/Sao_Paulo') AT TIME ZONE 'America/Sao_Paulo'");
    const hoje = await soma(" AND created_at >= date_trunc('day', NOW() AT TIME ZONE 'America/Sao_Paulo') AT TIME ZONE 'America/Sao_Paulo'");
    const ped = await pool.query("SELECT COUNT(*)::int AS recebidos, COUNT(*) FILTER (WHERE status='novo')::int AS aguardando, COUNT(*) FILTER (WHERE status='cancelado' AND so_cardapio_aceito_em IS NULL)::int AS recusados FROM orders WHERE so_cardapio=true");
    res.json({ ativo: c.ativo, taxa: c.taxa, geral, mes, hoje, pedidos: ped.rows[0] });
  }));

  return { criarColunas, antesDoPost, checkoutLiberado, filtrarLista, recusarSeSoCardapio, modoAgora };
};
