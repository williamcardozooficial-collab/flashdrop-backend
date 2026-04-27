const jwt = require('jsonwebtoken'); // rebuild
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const axios = require('axios');
const TelegramBot = require('node-telegram-bot-api');
require('dotenv').config();

const app = express();
app.use(cors({ origin: '*', methods: ['GET','POST','PUT','DELETE','OPTIONS'], allowedHeaders: ['Content-Type','Authorization'] }));
app.options('*', cors());
app.use(express.json());

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

// SANITIZACAO DE INPUT - protecao XSS e SQL Injection
function sanitize(val) {
  if (val === null || val === undefined) return val;
  if (typeof val !== 'string') return val;
  return val
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#x27;')
    .replace(/\/\//g, '')
    .replace(/--/g, '')
    .replace(/\/\*/g, '')
    .replace(/\*\//g, '')
    .replace(/;\s*(DROP|ALTER|CREATE|DELETE|UPDATE|INSERT|EXEC|EXECUTE|UNION|SELECT)/gi, '')
    .trim();
}
function san(obj, ...fields) {
  const out = { ...obj };
  fields.forEach(f => { if (out[f] !== undefined && out[f] !== null) out[f] = sanitize(String(out[f])); });
  return out;
}
// FIM SANITIZACAO

// Telegram Bot
const bot = process.env.TELEGRAM_BOT_TOKEN ? new TelegramBot(process.env.TELEGRAM_BOT_TOKEN, {polling: true}) : null;
const ADMIN_ID = 738230199;

// Bot Commands
if (bot) {
  bot.onText(/\/registrar (.+)/, async (msg, match) => {
    const telegram_id = msg.from.id;
    const username = match[1];
    try {
      await pool.query('UPDATE users SET telegram_id=$1 WHERE username=$2', [telegram_id, username]);
      bot.sendMessage(telegram_id, 'Registrado! Voce recebera notificacoes de novos pedidos.');
    } catch(e) {
      bot.sendMessage(telegram_id, 'Erro ao registrar. Verifique seu username.');
    }
  });
  bot.on('location', async (msg) => {
    const telegram_id = msg.from.id;
    const {latitude, longitude} = msg.location;
    try {
      await pool.query('UPDATE users SET lat=$1, lng=$2, last_location_update=NOW() WHERE telegram_id=$3', [latitude, longitude, telegram_id]);
    } catch(e) {}
  });
}

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      username VARCHAR(50) UNIQUE NOT NULL,
      password VARCHAR(100) NOT NULL,
      role VARCHAR(20) NOT NULL,
      name VARCHAR(100) NOT NULL,
      address TEXT,
      phone VARCHAR(30),
      vehicle VARCHAR(100),
      credit DECIMAL DEFAULT 0,
      balance DECIMAL DEFAULT 0,
      online BOOLEAN DEFAULT false,
      blocked BOOLEAN DEFAULT false,
      telegram_id BIGINT,
      lat DECIMAL,
      lng DECIMAL,
      last_location_update TIMESTAMP,
      created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS orders (
      id SERIAL PRIMARY KEY,
      loja_user VARCHAR(50),
      loja_name VARCHAR(100),
      plataforma VARCHAR(50),
      endereco_coleta TEXT,
      endereco_entrega TEXT,
      bairro_destino VARCHAR(100),
      nome_cliente VARCHAR(100),
      telefone_cliente VARCHAR(30),
      cod_pedido VARCHAR(50),
      cobrar_cliente VARCHAR(20) DEFAULT 'nao',
      tipo_pagamento VARCHAR(20) DEFAULT 'dinheiro',
      valor_pedido DECIMAL DEFAULT 0,
      valor_total DECIMAL DEFAULT 0,
      valor_motoboy DECIMAL DEFAULT 0,
      comissao DECIMAL DEFAULT 0,
      distancia DECIMAL DEFAULT 0,
      previsao VARCHAR(10),
      obs TEXT,
      status VARCHAR(30) DEFAULT 'pendente',
      motoboy_name VARCHAR(100),
      motoboy_id INTEGER,
      pending_until BIGINT,
      arrive_deadline BIGINT,
      t_aceito TIMESTAMP,
      t_na_loja TIMESTAMP,
      t_coletado TIMESTAMP,
      t_no_cliente TIMESTAMP,
      t_entregue TIMESTAMP,
      created_at TIMESTAMP DEFAULT NOW(),
      notified_admin BOOLEAN DEFAULT false
    );
    CREATE TABLE IF NOT EXISTS settings (
      id SERIAL PRIMARY KEY,
      min_fee DECIMAL DEFAULT 8.00,
      price_per_km DECIMAL DEFAULT 2.50,
      arrancada DECIMAL DEFAULT 5.00,
      commission DECIMAL DEFAULT 2.00,
      max_per_motoboy INT DEFAULT 2,
      launch_delay_minutes INT DEFAULT 60
    );
    INSERT INTO settings (id) VALUES (1) ON CONFLICT DO NOTHING;
    INSERT INTO users (username,password,role,name) VALUES ('admin','admin123','admin','Administrador') ON CONFLICT DO NOTHING;
  `);

  // Migrations
  try { await pool.query("ALTER TABLE orders ADD COLUMN IF NOT EXISTS tipo_pagamento VARCHAR(20) DEFAULT 'dinheiro'"); } catch(e) {}
  try { await pool.query("ALTER TABLE users ADD COLUMN IF NOT EXISTS blocked_until BIGINT DEFAULT 0"); } catch(e) {}
  try { await pool.query("ALTER TABLE users ADD COLUMN IF NOT EXISTS cpf VARCHAR(14)"); } catch(e) {}
  try { await pool.query("ALTER TABLE users ADD COLUMN IF NOT EXISTS approved BOOLEAN DEFAULT false"); } catch(e) {}
  try { await pool.query("UPDATE users SET approved=true WHERE (approved IS NULL OR approved=false) AND created_at < '2026-04-17 20:00:00'"); } catch(e) {}
  try { await pool.query("ALTER TABLE orders ADD COLUMN IF NOT EXISTS observacao_entrega TEXT"); } catch(e) {}
  try { await pool.query("ALTER TABLE orders ADD COLUMN IF NOT EXISTS telefone_loja VARCHAR(30)"); } catch(e) {}
  try { await pool.query("ALTER TABLE orders ALTER COLUMN motoboy_id TYPE INTEGER USING motoboy_id::INTEGER"); } catch(e) {}
  try { await pool.query("ALTER TABLE orders ADD COLUMN IF NOT EXISTS t_retornado TIMESTAMP"); } catch(e) {}
  try { await pool.query("ALTER TABLE orders ADD COLUMN IF NOT EXISTS complemento_coleta TEXT"); } catch(e) {}
  try { await pool.query("ALTER TABLE orders ADD COLUMN IF NOT EXISTS complemento_entrega TEXT"); } catch(e) {}
  try { await pool.query("ALTER TABLE orders ADD COLUMN IF NOT EXISTS obs_coleta TEXT"); } catch(e) {}
  try { await pool.query("ALTER TABLE orders ADD COLUMN IF NOT EXISTS obs_entrega_loja TEXT"); } catch(e) {}

  // ── MÓDULO DE INDICAÇÃO ─────────────────────────────────────────────
  try { await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_code VARCHAR(20) UNIQUE`); } catch(e) {}
  try { await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by INTEGER`); } catch(e) {}
  try { await pool.query(`CREATE TABLE IF NOT EXISTS referral_settings (
    id INTEGER PRIMARY KEY DEFAULT 1,
    ativo BOOLEAN DEFAULT true,
    comissao_por_pedido_loja DECIMAL DEFAULT 0.50,
    bonus_motoboy_meta DECIMAL DEFAULT 150.00,
    meta_pedidos_motoboy INTEGER DEFAULT 100,
    prazo_meta_dias INTEGER DEFAULT 30,
    validade_indicacao_loja_dias INTEGER DEFAULT 90,
    updated_at TIMESTAMP DEFAULT NOW()
  )`); } catch(e) {}
  try { await pool.query(`INSERT INTO referral_settings (id) VALUES (1) ON CONFLICT DO NOTHING`); } catch(e) {}
  try { await pool.query(`CREATE TABLE IF NOT EXISTS referrals (
    id SERIAL PRIMARY KEY,
    referrer_id INTEGER NOT NULL,
    referrer_name VARCHAR(100),
    referred_id INTEGER NOT NULL,
    referred_name VARCHAR(100),
    referred_role VARCHAR(20),
    status_ref VARCHAR(20) DEFAULT 'ativo',
    bonus_pago BOOLEAN DEFAULT false,
    bonus_valor DECIMAL DEFAULT 0,
    meta_pedidos INTEGER DEFAULT 0,
    total_pedidos_validos INTEGER DEFAULT 0,
    total_ganho DECIMAL DEFAULT 0,
    data_inicio TIMESTAMP DEFAULT NOW(),
    data_fim TIMESTAMP,
    data_conclusao TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
  )`); } catch(e) {}
  try { await pool.query(`CREATE TABLE IF NOT EXISTS referral_earnings (
    id SERIAL PRIMARY KEY,
    referrer_id INTEGER NOT NULL,
    referred_id INTEGER,
    order_id INTEGER,
    valor DECIMAL DEFAULT 0,
    tipo VARCHAR(40),
    created_at TIMESTAMP DEFAULT NOW()
  )`); } catch(e) {}
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS withdrawals (
      id SERIAL PRIMARY KEY,
      motoboy_id INTEGER NOT NULL,
      motoboy_name VARCHAR(100),
      valor DECIMAL NOT NULL,
      pix_key TEXT NOT NULL,
      status VARCHAR(20) DEFAULT 'pendente',
      obs TEXT,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )`);
  } catch(e) {}
  try { await pool.query("ALTER TABLE settings ADD COLUMN IF NOT EXISTS launch_delay_minutes INT DEFAULT 60"); } catch(e) {}
  try { await pool.query("ALTER TABLE orders ADD COLUMN IF NOT EXISTS launch_at BIGINT DEFAULT 0"); } catch(e) {}
  try { await pool.query("ALTER TABLE users ADD COLUMN IF NOT EXISTS custom_id VARCHAR(20)"); } catch(e) {}
  try { await pool.query("ALTER TABLE orders ADD COLUMN IF NOT EXISTS notified_admin BOOLEAN DEFAULT false"); } catch(e) {}

  // === CREDIT LIMIT MIGRATIONS ===
  // credit_mode: 0=sem limite, 1=bloquear ao atingir limite, 2=nao pode ultrapassar somando valor do pedido
  try { await pool.query("ALTER TABLE users ADD COLUMN IF NOT EXISTS credit_mode INTEGER DEFAULT 0"); } catch(e) {}
  // custom_credit_limit: NULL = usa padrao da plataforma; valor definido = usa este individualmente
  try { await pool.query("ALTER TABLE users ADD COLUMN IF NOT EXISTS custom_credit_limit DECIMAL DEFAULT NULL"); } catch(e) {}
  // Limite padrao de credito na tabela de configuracoes
  try { await pool.query("ALTER TABLE settings ADD COLUMN IF NOT EXISTS credit_limit DECIMAL DEFAULT 20.00"); } catch(e) {}

  // Auto-generate custom_id for existing users
  try {
    const existingUsers = await pool.query("SELECT id, role FROM users WHERE custom_id IS NULL OR custom_id = ''");
    for (const u of existingUsers.rows) {
      let prefix, digits;
      if (u.role === 'motoboy') { prefix = 'M'; digits = 4; }
      else if (u.role === 'loja') { prefix = 'L'; digits = 6; }
      else continue;
      const countRes = await pool.query("SELECT COUNT(*) FROM users WHERE role=$1 AND custom_id IS NOT NULL", [u.role]);
      const count = parseInt(countRes.rows[0].count) + 1;
      const newId = prefix + String(count).padStart(digits, '0');
      await pool.query("UPDATE users SET custom_id=$1 WHERE id=$2", [newId, u.id]);
    }
  } catch(e) { console.log('custom_id migration:', e.message); }

  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS notices (
      id SERIAL PRIMARY KEY,
      title VARCHAR(200) NOT NULL,
      body TEXT NOT NULL,
      target VARCHAR(20) DEFAULT 'all',
      created_by VARCHAR(50) DEFAULT 'admin',
      created_at TIMESTAMP DEFAULT NOW()
    )`);
  } catch(e) {}

  // ── CAIXA DA PLATAFORMA ──────────────────────────────────────
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS platform_wallet (
      id INTEGER PRIMARY KEY DEFAULT 1,
      balance DECIMAL DEFAULT 0,
      total_ganho DECIMAL DEFAULT 0,
      total_sacado DECIMAL DEFAULT 0,
      updated_at TIMESTAMP DEFAULT NOW()
    )`);
    await pool.query(`INSERT INTO platform_wallet (id) VALUES (1) ON CONFLICT DO NOTHING`);
  } catch(e) {}
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS platform_events (
      id SERIAL PRIMARY KEY,
      tipo VARCHAR(40) NOT NULL,
      valor DECIMAL NOT NULL,
      descricao TEXT,
      order_id INTEGER,
      created_at TIMESTAMP DEFAULT NOW()
    )`);
  } catch(e) {}

  // ── PROMOÇÕES DOS MOTOBOYS ────────────────────────────────────────
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS promotions (
      id SERIAL PRIMARY KEY,
      nome VARCHAR(200) NOT NULL,
      meta_entregas INTEGER NOT NULL DEFAULT 25,
      valor_bonus DECIMAL NOT NULL DEFAULT 30,
      tipo VARCHAR(20) NOT NULL DEFAULT 'dia',
      repetir BOOLEAN NOT NULL DEFAULT false,
      ativa BOOLEAN NOT NULL DEFAULT true,
      created_at TIMESTAMP DEFAULT NOW()
    )`);
  } catch(e) {}
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS promotion_progress (
      id SERIAL PRIMARY KEY,
      promotion_id INTEGER REFERENCES promotions(id) ON DELETE CASCADE,
      motoboy_id INTEGER NOT NULL,
      contagem INTEGER NOT NULL DEFAULT 0,
      data_ref DATE NOT NULL DEFAULT CURRENT_DATE,
      pago BOOLEAN NOT NULL DEFAULT false,
      pago_at TIMESTAMP,
      UNIQUE(promotion_id, motoboy_id, data_ref)
    )`);
  } catch(e) {}
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS produtos (
      id SERIAL PRIMARY KEY,
      loja_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      nome VARCHAR(200) NOT NULL,
      descricao TEXT,
      preco DECIMAL DEFAULT 0,
      categoria VARCHAR(100),
      foto_url TEXT,
      ativo BOOLEAN DEFAULT true,
      ordem INTEGER DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW()
    )`);
  } catch(e) {}
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS vitrine_config (
      id SERIAL PRIMARY KEY,
      loja_id INTEGER UNIQUE NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      ativa BOOLEAN DEFAULT false,
      descricao_loja TEXT,
      banner_url TEXT,
      categoria_loja VARCHAR(100),
      tempo_entrega_min INTEGER,
      tempo_entrega_max INTEGER,
      created_at TIMESTAMP DEFAULT NOW()
    )`);
  } catch(e) {}

   try { await pool.query(`CREATE TABLE IF NOT EXISTS pagamento_restaurante (
      id SERIAL PRIMARY KEY,
      order_id INTEGER NOT NULL,
      motoboy_id INTEGER NOT NULL,
      motoboy_name VARCHAR(100),
      loja_user VARCHAR(50) NOT NULL,
      valor DECIMAL NOT NULL,
      status VARCHAR(20) DEFAULT 'pendente',
      expires_at BIGINT NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    `); } catch(e) {}
  // ──────────────────────────────────────────────────────────────────

  console.log('DB initialized');
}

app.get('/health', (req, res) => res.json({ ok: true }));

const loginHandler = async (req, res) => {
  const { username, password } = req.body;
  const r = await pool.query('SELECT * FROM users WHERE username=$1 AND password=$2', [username, password]);
  if (r.rows.length === 0) return res.status(401).json({ error: 'Usuario ou senha invalidos.' });
  if (r.rows[0].blocked) return res.status(403).json({ error: 'Conta bloqueada.' });
  if (r.rows[0].approved === false) return res.status(403).json({ error: 'Cadastro aguardando aprovacao do administrador.' });
  const user = r.rows[0];
  const token = jwt.sign({ id: user.id, username: user.username, role: user.role }, process.env.JWT_SECRET || 'flashdrop_secret_2024', { expiresIn: '90d' });
  res.json({ ...user, token });
};
app.post('/users/login', loginHandler);
app.post('/api/login', loginHandler);
app.post('/api/admin/login', loginHandler);

app.get('/users', async (req, res) => {
  const r = await pool.query('SELECT * FROM users');
  res.json(r.rows);
});

app.get('/users/pending', async (req, res) => {
  try {
    const r = await pool.query("SELECT id,username,role,name,address,phone,vehicle,cpf,approved,created_at FROM users WHERE approved=false ORDER BY created_at ASC");
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/users/:id', async (req, res) => {
  try {
    const r = await pool.query('SELECT * FROM users WHERE id=$1', [req.params.id]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Nao encontrado' });
    res.json(r.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/users', async (req, res) => {
  try {
    const { username, password, role, name, address, phone, vehicle, cpf, telegram_id, custom_credit_limit, custom_id } = req.body;
    const _u = san({ username, name, address, vehicle }, 'username', 'name', 'address', 'vehicle');
    const { username: s_user, name: s_name, address: s_addr, vehicle: s_veh } = _u;
    if (phone) {
      const dupPhone = await pool.query('SELECT id FROM users WHERE phone=$1', [phone]);
      if (dupPhone.rows.length > 0) return res.status(400).json({ error: 'Telefone ja cadastrado.' });
    }
    if (cpf && role === 'motoboy') {
      const dupCpf = await pool.query('SELECT id FROM users WHERE cpf=$1', [cpf]);
      if (dupCpf.rows.length > 0) return res.status(400).json({ error: 'CPF ja cadastrado.' });
    }
    const approved = true;
    const r = await pool.query(
      'INSERT INTO users (username,password,role,name,address,phone,vehicle,cpf,approved,telegram_id,custom_credit_limit) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *',
      [s_user, password, role, s_name, s_addr, phone, s_veh, cpf || null, approved, telegram_id || null, custom_credit_limit || null]
    );
    let prefix2, digits2;
    if (role === 'motoboy') { prefix2 = 'M'; digits2 = 4; }
    else if (role === 'loja') { prefix2 = 'L'; digits2 = 6; }
    if (custom_id && custom_id.trim()) {
      await pool.query('UPDATE users SET custom_id=$1 WHERE id=$2', [custom_id.trim().toUpperCase(), r.rows[0].id]);
      r.rows[0].custom_id = custom_id.trim().toUpperCase();
    } else if (prefix2) {
      const cntRes = await pool.query("SELECT COUNT(*) FROM users WHERE role=$1 AND custom_id IS NOT NULL", [role]);
      const cnt = parseInt(cntRes.rows[0].count) + 1;
      const newCid = prefix2 + String(cnt).padStart(digits2, '0');
      await pool.query("UPDATE users SET custom_id=$1 WHERE id=$2", [newCid, r.rows[0].id]);
      r.rows[0].custom_id = newCid;
    }
    res.json(r.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/register', async (req, res) => {
  try {
    const { username, password, role, name, address, phone, vehicle, cpf, referral_code } = req.body;
    const _r = san({ username, name, address, vehicle }, 'username', 'name', 'address', 'vehicle');
    const { username: r_user, name: r_name, address: r_addr, vehicle: r_veh } = _r;
    if (role === 'motoboy') {
      if (!cpf) return res.status(400).json({ error: 'CPF obrigatorio para motoboy.' });
      if (!/^\d{3}\.\d{3}\.\d{3}-\d{2}$/.test(cpf)) return res.status(400).json({ error: 'CPF invalido. Use o formato 000.000.000-00' });
    }
    if (!phone) return res.status(400).json({ error: 'Telefone obrigatorio.' });
    if (!/^\(\d{2}\) \d \d{4}-\d{4}$/.test(phone)) return res.status(400).json({ error: 'Telefone invalido. Use o formato (00) 0 0000-0000' });
    const dupUser = await pool.query('SELECT id FROM users WHERE username=$1', [username]);
    if (dupUser.rows.length > 0) return res.status(400).json({ error: 'Nome de usuario ja existe.' });
    const dupPhone = await pool.query('SELECT id FROM users WHERE phone=$1', [phone]);
    if (dupPhone.rows.length > 0) return res.status(400).json({ error: 'Telefone ja cadastrado.' });
    if (cpf) {
      const dupCpf = await pool.query('SELECT id FROM users WHERE cpf=$1', [cpf]);
      if (dupCpf.rows.length > 0) return res.status(400).json({ error: 'CPF ja cadastrado.' });
    }
    const r = await pool.query(
      'INSERT INTO users (username,password,role,name,address,phone,vehicle,cpf,approved) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,false) RETURNING id,username,role,name,approved',
      [r_user, password, role || 'motoboy', r_name, r_addr, phone, r_veh, cpf || null]
    );
    const regRole = role || 'motoboy';
    let rpfx, rdigs;
    if (regRole === 'motoboy') { rpfx = 'M'; rdigs = 4; }
    else if (regRole === 'loja') { rpfx = 'L'; rdigs = 6; }
    if (rpfx) {
      const rcnt = await pool.query("SELECT COUNT(*) FROM users WHERE role=$1 AND custom_id IS NOT NULL", [regRole]);
      const rn = parseInt(rcnt.rows[0].count) + 1;
      const rcid = rpfx + String(rn).padStart(rdigs, '0');
      await pool.query("UPDATE users SET custom_id=$1 WHERE id=$2", [rcid, r.rows[0].id]);
      r.rows[0].custom_id = rcid;
    }
        // Processar referral_code se fornecido
    if (referral_code && referral_code.trim()) {
      try {
        const refUser = await pool.query('SELECT * FROM users WHERE referral_code=$1', [referral_code.trim().toUpperCase()]);
        if (refUser.rows.length > 0) {
          const referrer = refUser.rows[0];
          const newUserId = r.rows[0].id;
          const newUserRole = role || 'motoboy';
          const refSettings = await pool.query('SELECT * FROM referral_settings WHERE id=1');
          const sets = refSettings.rows[0];
          if (sets && sets.ativo) {
            let refType = 'motoboy';
            let metaPed = sets.meta_pedidos_motoboy || 100;
            let prazoD = sets.prazo_meta_dias || 30;
            let bonusVal = sets.bonus_motoboy_meta || 150;
            let dataFim = new Date();
            dataFim.setDate(dataFim.getDate() + prazoD);
            await pool.query(
              'INSERT INTO referrals (referrer_id, referrer_name, referred_id, referred_name, referred_role, status_ref, meta_pedidos, bonus_valor, data_inicio, data_fim) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW(),$9)',
              [referrer.id, referrer.name, newUserId, r.rows[0].name, newUserRole, 'ativo', metaPed, bonusVal, dataFim.toISOString()]
            );
            await pool.query('UPDATE users SET referred_by=$1 WHERE id=$2', [referrer.id, newUserId]);
          }
        }
      } catch(refErr) { console.log('Referral link error:', refErr.message); }
    }
    if (bot) bot.sendMessage(ADMIN_ID, '\uD83D\uDCCB Novo Cadastro Pendente!\n\nNome: ' + r.rows[0].name + '\nUsuario: ' + r.rows[0].username + '\nFuncao: ' + r.rows[0].role + '\n\nAcesse o painel admin para aprovar.').catch(function(){});
    res.json({ ok: true, user: r.rows[0] });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/users/:id/approve', async (req, res) => {
  try {
    const r = await pool.query('UPDATE users SET approved=true WHERE id=$1 RETURNING id,username,name,approved', [req.params.id]);
    res.json(r.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/users/:id', async (req, res) => {
  const fields = req.body;
  const sets = Object.keys(fields).map((k,i) => `${k}=$${i+2}`).join(',');
  const vals = Object.values(fields);
  const r = await pool.query(`UPDATE users SET ${sets} WHERE id=$1 RETURNING *`, [req.params.id, ...vals]);
  res.json(r.rows[0]);
});

app.delete('/users/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM users WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/orders', async (req, res) => {
  const r = await pool.query('SELECT * FROM orders ORDER BY created_at DESC');
  res.json(r.rows);
});

app.get('/api/pedidos', async (req, res) => {
  const r = await pool.query('SELECT o.*, u.name as loja_nome FROM orders o LEFT JOIN users u ON o.loja_user=u.username ORDER BY created_at DESC');
  res.json(r.rows.map(o => ({...o, cliente_nome: o.nome_cliente, status: o.status || 'pendente', valor: o.valor_total})));
});

app.get('/api/lojas', async (req, res) => {
  const r = await pool.query("SELECT id, name as nome, address as endereco, phone as telefone, online as ativo, credit as saldo FROM users WHERE role='loja'");
  res.json(r.rows);
});

app.get('/api/motoboys', async (req, res) => {
  const r = await pool.query("SELECT id, name as nome, phone as telefone, vehicle as veiculo, online as disponivel, balance as saldo FROM users WHERE role='motoboy'");
  res.json(r.rows);
});

// === VERIFICACAO DE LIMITE DE CREDITO DO MOTOBOY ===
// Retorna se o motoboy pode aceitar corridas em dinheiro e qual modo esta ativo
app.get('/users/:id/credit-check', async (req, res) => {
  try {
    const mbRes = await pool.query('SELECT id, balance, credit_mode, custom_credit_limit FROM users WHERE id=$1', [req.params.id]);
    if (mbRes.rows.length === 0) return res.status(404).json({ error: 'Motoboy nao encontrado.' });
    const mb = mbRes.rows[0];

    const settingsRes = await pool.query('SELECT credit_limit FROM settings WHERE id=1');
    const platformLimit = parseFloat(settingsRes.rows[0]?.credit_limit || 20.00);
    const effectiveLimit = mb.custom_credit_limit !== null ? parseFloat(mb.custom_credit_limit) : platformLimit;
    const balance = parseFloat(mb.balance || 0);
    const creditMode = parseInt(mb.credit_mode || 0);

    // Nova logica: custom_credit_limit define se motoboy pode pegar corridas em dinheiro
    // null ou 0 = bloqueado por padrao; > 0 = pode aceitar ate o limite
    const individualLimit = mb.custom_credit_limit !== null ? parseFloat(mb.custom_credit_limit) : 0;

    if (individualLimit <= 0) {
      return res.json({
        can_accept_cash: false,
        balance,
        individual_limit: individualLimit,
        blocked: true,
        message: 'Voce nao possui saldo suficiente para pegar este pedido. Procure manter saldo na plataforma para poder aceitar pedidos em dinheiro.'
      });
    }

    const blocked = balance <= -individualLimit;
    res.json({
      can_accept_cash: !blocked,
      balance,
      individual_limit: individualLimit,
      blocked,
      message: blocked ? 'Voce nao possui saldo suficiente para pegar este pedido. Procure manter saldo na plataforma para poder aceitar pedidos em dinheiro.' : null
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/orders', async (req, res) => {
  const d = req.body;
  d.nome_cliente = sanitize(d.nome_cliente);
  d.endereco_entrega = sanitize(d.endereco_entrega);
  d.obs = sanitize(d.obs);
  d.loja_name = sanitize(d.loja_name);
  d.complemento_entrega = sanitize(d.complemento_entrega);
  let telefone_loja = null;
  try {
    const lojaRes = await pool.query('SELECT phone FROM users WHERE username=$1', [d.loja_user]);
    if (lojaRes.rows.length > 0) telefone_loja = lojaRes.rows[0].phone;
  } catch(e) {}
  const r = await pool.query(
    `INSERT INTO orders (loja_user,loja_name,plataforma,endereco_coleta,endereco_entrega,bairro_destino,nome_cliente,telefone_cliente,cod_pedido,cobrar_cliente,tipo_pagamento,valor_pedido,valor_total,valor_motoboy,comissao,distancia,previsao,obs,status,pending_until,telefone_loja,launch_at,complemento_coleta,complemento_entrega,obs_coleta,obs_entrega_loja)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,'em_preparo',$19,$20,$21,$22,$23,$24,$25) RETURNING *`,
    [d.loja_user,d.loja_name,d.plataforma,d.endereco_coleta,d.endereco_entrega,d.bairro_destino,d.nome_cliente,d.telefone_cliente,d.cod_pedido,d.cobrar_cliente||'nao',d.tipo_pagamento||'dinheiro',d.valor_pedido||0,d.valor_total,d.valor_motoboy,d.comissao,d.distancia,d.previsao,d.obs,Date.now()+15000,telefone_loja,d.launch_at||0,d.complemento_coleta||null,d.complemento_entrega||null,d.obs_coleta||null,d.obs_entrega_loja||null]
  );
  if (bot) {
    const motoboys = await pool.query("SELECT telegram_id, name FROM users WHERE role='motoboy' AND online=true AND telegram_id IS NOT NULL");
    const pedido = r.rows[0];
    let lojaNome = pedido.loja_name;
    if (!lojaNome && pedido.loja_user) {
      const lojaRes = await pool.query("SELECT name FROM users WHERE username=$1", [pedido.loja_user]);
      if (lojaRes.rows.length > 0) lojaNome = lojaRes.rows[0].name;
    }
    lojaNome = lojaNome || pedido.loja_user;
    const pagLabel = ({dinheiro:'Dinheiro',maquina:'Maquina',pix:'PIX'}[pedido.tipo_pagamento] || pedido.tipo_pagamento || '-');
    const msgPedido = `Novo Pedido #${pedido.id} - Em Preparo\n\nLoja: ${lojaNome}\nPagamento: ${pagLabel}\nMotoboy ganha: R$ ${parseFloat(pedido.valor_motoboy).toFixed(2)}\nDistancia: ${pedido.distancia} km\n\nPedido em preparo. Sera lancado ao sistema em breve.\nFique de olho!`;
    const groupId = process.env.TELEGRAM_GROUP_ID;
    if (groupId) bot.sendMessage(groupId, msgPedido).catch(() => {});
    motoboys.rows.forEach(mb => bot.sendMessage(mb.telegram_id, msgPedido).catch(() => {}));
  }
  res.json(r.rows[0]);
});

app.put('/orders/:id', async (req, res) => {
  try {
    const fields = req.body;

    // === VERIFICACAO DE LIMITE DE CREDITO AO ACEITAR CORRIDA EM DINHEIRO ===
    if (fields.status === 'aceito' && fields.motoboy_id) {

      // Verifica bloqueio por cancelamento
      const mbRes = await pool.query('SELECT blocked_until, balance, credit_mode, custom_credit_limit FROM users WHERE id=$1', [fields.motoboy_id]);
      if (mbRes.rows.length > 0) {
        const mb = mbRes.rows[0];

        if (mb.blocked_until && mb.blocked_until > Date.now()) {
          const remaining = Math.ceil((mb.blocked_until - Date.now()) / 60000);
          return res.status(403).json({ error: 'Voce esta bloqueado por cancelamento. Aguarde ' + remaining + ' minuto(s).' });
        }

        // Verifica limite de credito apenas para corridas em dinheiro
        const orderRes = await pool.query('SELECT tipo_pagamento, valor_motoboy, comissao, valor_pedido FROM orders WHERE id=$1', [req.params.id]);
        if (orderRes.rows.length > 0) {
          const order = orderRes.rows[0];
          const isDinheiro = order.tipo_pagamento === 'dinheiro';

          if (isDinheiro) {
            // Logica: custom_credit_limit define o limite individual do motoboy
            // null ou 0 = bloqueado por padrao (admin precisa definir um valor > 0 para liberar)
            const individualLimit = mb.custom_credit_limit !== null ? parseFloat(mb.custom_credit_limit) : 0;
            const balance = parseFloat(mb.balance || 0);

            if (individualLimit <= 0) {
              // Limite nao definido ou zerado: motoboy bloqueado para corridas em dinheiro
              return res.status(403).json({
                error: 'Voce nao possui saldo suficiente para pegar este pedido. Procure manter saldo na plataforma para poder aceitar pedidos em dinheiro.',
                credit_blocked: true
              });
            }

            if (balance <= -individualLimit) {
              // Saldo negativo ultrapassou o limite individual
              return res.status(403).json({
                error: 'Voce nao possui saldo suficiente para pegar este pedido. Procure manter saldo na plataforma para poder aceitar pedidos em dinheiro.',
                credit_blocked: true
              });
            }
          }
        }
      }
    }

    const sets = Object.keys(fields).map((k,i) => `${k}=$${i+2}`).join(',');
    const vals = Object.values(fields);

    const prevOrderRes = await pool.query('SELECT motoboy_id FROM orders WHERE id=$1', [req.params.id]);
    const prevMotoboyId = prevOrderRes.rows.length > 0 ? prevOrderRes.rows[0].motoboy_id : null;

    const r = await pool.query(`UPDATE orders SET ${sets} WHERE id=$1 RETURNING *`, [req.params.id, ...vals]);
    const order = r.rows[0];

    // Cancelamento pelo motoboy: bloquear por 10 minutos
    if (fields.status === 'pendente' && fields.motoboy_id === null && prevMotoboyId) {
      const BLOCK_MS = 10 * 60 * 1000;
      const blockedUntil = Date.now() + BLOCK_MS;
      await pool.query('UPDATE users SET blocked_until=$1 WHERE id=$2', [blockedUntil, prevMotoboyId]);
    }

    if (fields.status === 'entregue' && order.motoboy_id) {
      const valorMotoboy = parseFloat(order.valor_motoboy) || 0;
      const valorPedido = parseFloat(order.valor_pedido) || 0;
      const comissao = parseFloat(order.comissao) || 0;
      const isDinheiro = order.tipo_pagamento === 'dinheiro';

      if (isDinheiro) {
        // Pedido dinheiro: motoboy cobrou do cliente
        // Debita valor_pedido (dinheiro da loja que ficou com motoboy) + comissao
        const debitoDinheiro = valorPedido + comissao;
        if (debitoDinheiro > 0) {
          await pool.query('UPDATE users SET balance = balance - $1 WHERE id=$2', [debitoDinheiro, order.motoboy_id]);
        }
        // Loja recebe valor_pedido de volta (motoboy vai repassar)
        if (valorPedido > 0) {
          await pool.query("UPDATE users SET credit = credit + $1 WHERE username=$2", [valorPedido, order.loja_user]);
        }
      } else {
        // PIX ou Maquina: motoboy recebe valor_motoboy
        if (valorMotoboy > 0) {
          await pool.query('UPDATE users SET balance = balance + $1 WHERE id=$2', [valorMotoboy, order.motoboy_id]);
        }
      }

      // ── CAIXA DA PLATAFORMA: creditar comissão ──────────────────────
      if (comissao > 0) {
        try {
          await pool.query(
            `UPDATE platform_wallet SET balance = balance + $1, total_ganho = total_ganho + $1, updated_at=NOW() WHERE id=1`,
            [comissao]
          );
          await pool.query(
            `INSERT INTO platform_events (tipo, valor, descricao, order_id) VALUES ('comissao', $1, $2, $3)`,
            [comissao, 'Comissão pedido #' + req.params.id + ' (' + (order.loja_name || order.loja_user) + ')', req.params.id]
          );
        } catch(ePw) { console.error('Platform wallet error:', ePw.message); }
      }

      // ── PROMOÇÕES: contabilizar entrega do motoboy ──────────────────
      try {
        if (ord.motoboy_id) {
          const todayPromos = await pool.query(
            `SELECT * FROM promotions WHERE ativa=true AND tipo='dia'`
          );
          for (const promo of todayPromos.rows) {
            const today = new Date().toISOString().slice(0,10);
            await pool.query(
              `INSERT INTO promotion_progress (promotion_id, motoboy_id, contagem, data_ref)
               VALUES ($1, $2, 1, $3)
               ON CONFLICT (promotion_id, motoboy_id, data_ref)
               DO UPDATE SET contagem = promotion_progress.contagem + 1`,
              [promo.id, ord.motoboy_id, today]
            );
            const prog = await pool.query(
              `SELECT * FROM promotion_progress WHERE promotion_id=$1 AND motoboy_id=$2 AND data_ref=$3`,
              [promo.id, ord.motoboy_id, today]
            );
            const p = prog.rows[0];
            if (p && p.contagem >= promo.meta_entregas && !p.pago) {
              const bonus = parseFloat(promo.valor_bonus);
              await pool.query(
                `UPDATE promotion_progress SET pago=true, pago_at=NOW() WHERE id=$1`,
                [p.id]
              );
              await pool.query(
                `UPDATE users SET balance = COALESCE(balance,0) + $1 WHERE id=$2`,
                [bonus, ord.motoboy_id]
              );
              await pool.query(
                `UPDATE platform_wallet SET balance = balance - $1, total_sacado = total_sacado + $1, updated_at=NOW() WHERE id=1`,
                [bonus]
              );
              await pool.query(
                `INSERT INTO platform_events (tipo, valor, descricao, order_id) VALUES ('promocao', $1, $2, $3)`,
                [bonus, 'Bonus promocao ' + promo.nome + ' - motoboy id ' + ord.motoboy_id, req.params.id]
              );
              console.log('[PROMO] Bonus R$' + bonus + ' pago ao motoboy id=' + ord.motoboy_id);
            }
          }
        }
      } catch(ePromo) { console.error('[PROMO] Erro promocao:', ePromo.message); }
      // ────────────────────────────────────────────────────────────────
      // ────────────────────────────────────────────────────────────────
    }

      // ── COMISSÃO DE INDICAÇÃO POR PEDIDO (automático) ────────────────
      try {
        const refCfg = await pool.query('SELECT * FROM referral_settings WHERE id=1');
        const refSettings = refCfg.rows[0];
        if (refSettings && refSettings.ativo && fields.status === 'entregue') {
          const ord = order; // já disponível do UPDATE acima
          const now = new Date();

          // 1) Comissão por indicação de LOJA (por pedido entregue)
          if (ord.loja_user) {
            const lojaUser = await pool.query('SELECT * FROM users WHERE username=$1', [ord.loja_user]);
            if (lojaUser.rows.length > 0) {
              const loja = lojaUser.rows[0];
              const lojaRef = await pool.query(
                `SELECT r.*, rs.comissao_por_pedido_loja FROM referrals r
                 JOIN referral_settings rs ON rs.id=1
                 WHERE r.referred_id=$1 AND r.referred_role='loja'
                   AND r.status_ref='ativo'
                   AND (r.data_fim IS NULL OR r.data_fim > NOW())`, [loja.id]);
              if (lojaRef.rows.length > 0) {
                const ref = lojaRef.rows[0];
                const comLoja = parseFloat(ref.comissao_por_pedido_loja || 0);
                if (comLoja > 0) {
                  await pool.query('UPDATE users SET balance = balance + $1 WHERE id=$2', [comLoja, ref.referrer_id]);
                  await pool.query(
                    `INSERT INTO referral_earnings (referrer_id, referred_id, order_id, valor, tipo)
                     VALUES ($1,$2,$3,$4,'comissao_pedido_loja')`,
                    [ref.referrer_id, loja.id, req.params.id, comLoja]);
                  await pool.query(
                    `UPDATE referrals SET total_pedidos_validos = total_pedidos_validos + 1,
                       total_ganho = total_ganho + $1 WHERE id=$2`, [comLoja, ref.id]);
                  console.log('[REFERRAL] Comissao loja paga: R$' + comLoja + ' ao indicador id=' + ref.referrer_id);
                }
              }
            }
          }

          // 2) Contabilizar pedido do MOTOBOY e pagar bônus se atingiu meta
          if (ord.motoboy_id) {
            const mbRef = await pool.query(
              `SELECT r.* FROM referrals r WHERE r.referred_id=$1
               AND r.referred_role='motoboy' AND r.status_ref='ativo' AND r.bonus_pago=false`, [ord.motoboy_id]);
            if (mbRef.rows.length > 0) {
              const ref = mbRef.rows[0];
              const prazoOk = !ref.data_fim || new Date(ref.data_fim) > now;
              if (prazoOk) {
                await pool.query(`UPDATE referrals SET total_pedidos_validos = total_pedidos_validos + 1 WHERE id=$1`, [ref.id]);
                const updated = await pool.query('SELECT * FROM referrals WHERE id=$1', [ref.id]);
                const upd = updated.rows[0];
                console.log('[REFERRAL] Motoboy pedido contabilizado: ' + upd.total_pedidos_validos + '/' + upd.meta_pedidos);
                // Se atingiu a meta — pagar bônus automaticamente
                if (upd && upd.total_pedidos_validos >= upd.meta_pedidos && upd.meta_pedidos > 0) {
                  const bonus = parseFloat(upd.bonus_valor || 0);
                  if (bonus > 0) {
                    await pool.query('UPDATE users SET balance = balance + $1 WHERE id=$2', [bonus, upd.referrer_id]);
                    await pool.query(
                      `INSERT INTO referral_earnings (referrer_id, referred_id, order_id, valor, tipo)
                       VALUES ($1,$2,$3,$4,'bonus_meta_motoboy')`,
                      [upd.referrer_id, ord.motoboy_id, req.params.id, bonus]);
                    await pool.query(
                      `UPDATE referrals SET bonus_pago=true, total_ganho=$1,
                         data_conclusao=NOW(), status_ref='concluido' WHERE id=$2`, [bonus, upd.id]);
                    console.log('[REFERRAL] Bonus meta motoboy pago: R$' + bonus + ' ao indicador id=' + upd.referrer_id);
                  }
                }
              } else {
                // Prazo expirado — marcar como expirado
                await pool.query(`UPDATE referrals SET status_ref='expirado' WHERE id=$1`, [ref.id]);
                console.log('[REFERRAL] Indicacao motoboy expirada id=' + ref.id);
              }
            }
          }
        }
      } catch(eRef) { console.error('[REFERRAL] Erro na comissao de indicacao:', eRef.message); }

    if (fields.status === 'retornado') {
      await pool.query("UPDATE orders SET t_retornado=NOW() WHERE id=$1", [req.params.id]);
    }

    res.json(order);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* Ã¢ÂÂÃ¢ÂÂ SAQUES (WITHDRAWALS) Ã¢ÂÂÃ¢ÂÂ */
app.get('/withdrawals', async (req, res) => {
  try {
    const r = await pool.query("SELECT * FROM withdrawals WHERE created_at >= NOW() - INTERVAL '7 days' ORDER BY created_at DESC");
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/withdrawals/motoboy/:id', async (req, res) => {
  try {
    const r = await pool.query("SELECT * FROM withdrawals WHERE motoboy_id=$1 AND created_at >= NOW() - INTERVAL '7 days' ORDER BY created_at DESC", [req.params.id]);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/withdrawals', async (req, res) => {
  try {
    const { motoboy_id, motoboy_name, valor, pix_key } = req.body;
    if (!motoboy_id || !valor || !pix_key) return res.status(400).json({ error: 'Dados incompletos.' });
    if (parseFloat(valor) <= 0) return res.status(400).json({ error: 'Valor invalido.' });
    const mb = await pool.query('SELECT balance FROM users WHERE id=$1', [motoboy_id]);
    if (mb.rows.length === 0) return res.status(404).json({ error: 'Motoboy nao encontrado.' });
    if (parseFloat(mb.rows[0].balance) < parseFloat(valor)) return res.status(400).json({ error: 'Saldo insuficiente para o saque solicitado.' });
    const r = await pool.query(
      'INSERT INTO withdrawals (motoboy_id, motoboy_name, valor, pix_key) VALUES ($1,$2,$3,$4) RETURNING *',
      [motoboy_id, motoboy_name, valor, pix_key]
    );
    if (bot) bot.sendMessage(ADMIN_ID, '\uD83D\uDCB8 Novo Pedido de Saque!\n\nMotoboy: ' + r.rows[0].motoboy_name + '\nValor: R$ ' + parseFloat(r.rows[0].valor).toFixed(2) + '\nChave PIX: ' + r.rows[0].pix_key + '\n\nAcesse o painel admin para processar.').catch(function(){});
    res.json(r.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/withdrawals/:id', async (req, res) => {
  try {
    const { status, obs } = req.body;
    const wr = await pool.query('SELECT * FROM withdrawals WHERE id=$1', [req.params.id]);
    if (wr.rows.length === 0) return res.status(404).json({ error: 'Saque nao encontrado.' });
    const w = wr.rows[0];
    if (w.status !== 'pendente') return res.status(400).json({ error: 'Este saque ja foi processado.' });
    await pool.query('UPDATE withdrawals SET status=$1, obs=$2, updated_at=NOW() WHERE id=$3', [status, obs || null, req.params.id]);
    if (status === 'aprovado') {
      await pool.query('UPDATE users SET balance = balance - $1 WHERE id=$2', [w.valor, w.motoboy_id]);
    }
    const updated = await pool.query('SELECT * FROM withdrawals WHERE id=$1', [req.params.id]);
    res.json(updated.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* Ã¢ÂÂÃ¢ÂÂ DELETE PEDIDO: corrigido para nao dar estorno em pedidos a dinheiro Ã¢ÂÂÃ¢ÂÂ */
app.delete('/orders/:id', async (req, res) => {
  try {
    const orderRes = await pool.query('SELECT * FROM orders WHERE id=$1', [req.params.id]);
    if (orderRes.rows.length > 0) {
      const order = orderRes.rows[0];
      // CORRECAO: pedidos em dinheiro NAO geram estorno para a loja
      // pois o saldo da loja nunca foi descontado no pedido a dinheiro
      // Apenas pedidos PIX/maquina (nao-dinheiro) geram estorno se nao foram entregues
      const isDinheiro = order.tipo_pagamento === 'dinheiro';
      if (!isDinheiro && order.status !== 'entregue' && order.loja_user) {
        const valorTotal = parseFloat(order.valor_total) || 0;
        if (valorTotal > 0) {
          await pool.query("UPDATE users SET credit = credit + $1 WHERE username=$2", [valorTotal, order.loja_user]);
        }
      }
    }
    await pool.query('DELETE FROM orders WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/users/:id/unblock-penalty', async (req, res) => {
  try {
    await pool.query('UPDATE users SET blocked_until=0 WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/settings', async (req, res) => {
  const r = await pool.query('SELECT * FROM settings WHERE id=1');
  res.json(r.rows[0]);
});

/* Ã¢ÂÂÃ¢ÂÂ AJUSTES DA PLATAFORMA (inclui credit_limit) Ã¢ÂÂÃ¢ÂÂ */
app.put('/settings', async (req, res) => {
  const { min_fee, price_per_km, arrancada, commission, max_per_motoboy, launch_delay_minutes, credit_limit } = req.body;
  const delayVal = (launch_delay_minutes != null) ? parseInt(launch_delay_minutes) : 60;
  const creditLimitVal = (credit_limit != null) ? parseFloat(credit_limit) : 20.00;
  const r = await pool.query(
    'UPDATE settings SET min_fee=$1, price_per_km=$2, arrancada=$3, commission=$4, max_per_motoboy=$5, launch_delay_minutes=$6, credit_limit=$7 WHERE id=1 RETURNING *',
    [min_fee, price_per_km, arrancada, commission, max_per_motoboy, delayVal, creditLimitVal]
  );
  res.json(r.rows[0]);
});

// ── CAIXA DA PLATAFORMA ────────────────────────────────────────────

app.get('/platform/wallet', async (req, res) => {
  try {
    const r = await pool.query('SELECT * FROM platform_wallet WHERE id=1');
    res.json(r.rows[0] || { balance: 0, total_ganho: 0, total_sacado: 0 });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/platform/events', async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT * FROM platform_events WHERE created_at >= NOW() - INTERVAL '48 hours' ORDER BY created_at DESC LIMIT 200`
    );
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/platform/withdraw', async (req, res) => {
  try {
    const { valor, motivo } = req.body;
    if (!valor || parseFloat(valor) <= 0) return res.status(400).json({ error: 'Valor invalido.' });
    if (!motivo || !motivo.trim()) return res.status(400).json({ error: 'Informe o motivo do saque.' });
    const walletRes = await pool.query('SELECT * FROM platform_wallet WHERE id=1');
    const wallet = walletRes.rows[0];
    if (!wallet || parseFloat(wallet.balance) < parseFloat(valor)) {
      return res.status(400).json({ error: 'Saldo insuficiente na caixa.' });
    }
    const v = parseFloat(valor);
    await pool.query(
      `UPDATE platform_wallet SET balance = balance - $1, total_sacado = total_sacado + $1, updated_at=NOW() WHERE id=1`,
      [v]
    );
    await pool.query(
      `INSERT INTO platform_events (tipo, valor, descricao) VALUES ('saque', $1, $2)`,
      [v, 'Saque: ' + motivo.trim()]
    );
    const updated = await pool.query('SELECT * FROM platform_wallet WHERE id=1');
    res.json({ ok: true, wallet: updated.rows[0] });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ────────────────────────────────────────────────────────────────────


/* PROMOCOES */
app.get('/promotions', async (req, res) => {
  try {
    const r = await pool.query('SELECT * FROM promotions ORDER BY id DESC');
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/promotions', async (req, res) => {
  try {
    const { nome, meta_entregas, valor_bonus, tipo, repetir, ativa } = req.body;
    const r = await pool.query(
      'INSERT INTO promotions (nome, meta_entregas, valor_bonus, tipo, repetir, ativa) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *',
      [nome, meta_entregas || 25, valor_bonus || 30, tipo || 'dia', repetir || false, ativa !== false]
    );
    res.json(r.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/promotions/:id', async (req, res) => {
  try {
    const { nome, meta_entregas, valor_bonus, tipo, repetir, ativa } = req.body;
    const r = await pool.query(
      'UPDATE promotions SET nome=$1, meta_entregas=$2, valor_bonus=$3, tipo=$4, repetir=$5, ativa=$6 WHERE id=$7 RETURNING *',
      [nome, meta_entregas, valor_bonus, tipo, repetir, ativa, req.params.id]
    );
    res.json(r.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/promotions/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM promotions WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/promotions/progress', async (req, res) => {
  try {
    const today = new Date().toISOString().slice(0,10);
    const r = await pool.query(
      `SELECT pp.*, p.nome, p.meta_entregas, p.valor_bonus, u.name as motoboy_name
       FROM promotion_progress pp
       JOIN promotions p ON p.id = pp.promotion_id
       JOIN users u ON u.id = pp.motoboy_id
       WHERE pp.data_ref = $1
       ORDER BY pp.contagem DESC`,
      [today]
    );
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/promotions/motoboy/:id', async (req, res) => {
  try {
    const today = new Date().toISOString().slice(0,10);
    const activePromos = await pool.query('SELECT * FROM promotions WHERE ativa=true');
    const progRes = await pool.query(
      `SELECT * FROM promotion_progress WHERE motoboy_id=$1 AND data_ref=$2`,
      [req.params.id, today]
    );
    const result = activePromos.rows.map(promo => {
      const prog = progRes.rows.find(rr => rr.promotion_id === promo.id);
      return {
        promotion_id: promo.id,
        nome: promo.nome,
        meta_entregas: promo.meta_entregas,
        valor_bonus: promo.valor_bonus,
        tipo: promo.tipo,
        contagem: prog ? prog.contagem : 0,
        pago: prog ? prog.pago : false
      };
    });
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});


app.post('/distance', async (req, res) => {
  try {
    const { origin, destination } = req.body;
    if (!origin || !destination) return res.status(400).json({ error: 'Origin e destination obrigatorios' });
    const apiKey = process.env.GOOGLE_MAPS_KEY;
    if (!apiKey) return res.status(500).json({ error: 'API key nao configurada' });

    // ── VALIDAR ENDEREÇO DE DESTINO VIA GEOCODING ──────────────────────
    // Garante que o endereço tem rua e número, não apenas bairro/cidade
    const geoUrl = `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(destination)}&key=${apiKey}`;
    const geoResp = await axios.get(geoUrl);
    const geoData = geoResp.data;
    if (geoData.status === 'OK' && geoData.results.length > 0) {
      const result = geoData.results[0];
      const locType = result.geometry.location_type;
      // GEOMETRIC_CENTER = apenas bairro/cidade/região (impreciso)
      // APPROXIMATE = área aproximada
      // ROOFTOP e RANGE_INTERPOLATED = endereço com número (aceitar)
      if (locType === 'GEOMETRIC_CENTER' || locType === 'APPROXIMATE') {
        const hasStreetNumber = result.address_components.some(c => c.types.includes('street_number'));
        if (!hasStreetNumber) {
          return res.status(400).json({
            error: 'Endereco impreciso. Informe a rua e o numero (ex: Rua das Flores, 123). Apenas bairro ou cidade nao e aceito.'
          });
        }
      }
    }
    // ───────────────────────────────────────────────────────────────────

    const url = `https://maps.googleapis.com/maps/api/distancematrix/json?origins=${encodeURIComponent(origin)}&destinations=${encodeURIComponent(destination)}&mode=driving&key=${apiKey}`;
    const resp = await axios.get(url);
    const data = resp.data;
    if (data.status !== 'OK' || !data.rows[0] || data.rows[0].elements[0].status !== 'OK') {
      return res.status(400).json({ error: 'Endereco nao encontrado ou rota invalida' });
    }
    const elem = data.rows[0].elements[0];
    const distance_km = parseFloat((elem.distance.value / 1000).toFixed(2));
    const duration_min = Math.ceil(elem.duration.value / 60);
    res.json({ distance_km, duration_min });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

/* Ã¢ÂÂÃ¢ÂÂ AVISOS (NOTICES) Ã¢ÂÂÃ¢ÂÂ */
app.get('/notices', async (req, res) => {
  try {
    const target = req.query.target;
    let r;
    if (target) {
      r = await pool.query("SELECT * FROM notices WHERE target='all' OR target=$1 ORDER BY created_at DESC", [target]);
    } else {
      r = await pool.query("SELECT * FROM notices ORDER BY created_at DESC");
    }
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/notices', async (req, res) => {
  try {
    const { title, body, target, created_by } = req.body;
    if (!title || !body) return res.status(400).json({ error: 'Titulo e texto obrigatorios.' });
    const r = await pool.query(
      "INSERT INTO notices (title, body, target, created_by) VALUES ($1,$2,$3,$4) RETURNING *",
      [title, body, target || 'all', created_by || 'admin']
    );
    res.json(r.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/notices/:id', async (req, res) => {
  try {
    await pool.query("DELETE FROM notices WHERE id=$1", [req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/users/by-custom-id/:cid', async (req, res) => {
  try {
    const r = await pool.query("SELECT * FROM users WHERE UPPER(custom_id)=UPPER($1)", [req.params.cid]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Usuario nao encontrado.' });
    res.json(r.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/webhook', (req, res) => res.json({ ok: true }));

const PORT = process.env.PORT || 3000;

const ARRIVE_TIMEOUT_MS = 15 * 60 * 1000;

async function checkLateArrivals() {
  try {
    const cutoff = new Date(Date.now() - ARRIVE_TIMEOUT_MS).toISOString();
    const r = await pool.query(
      "SELECT * FROM orders WHERE status='aceito' AND t_aceito IS NOT NULL AND t_na_loja IS NULL AND t_aceito < $1",
      [cutoff]
    );
    for (const order of r.rows) {
      console.log(`[JOB] Pedido #${order.id}: motoboy ${order.motoboy_name} nao chegou na loja. Resetando para pendente.`);
      await pool.query(
        "UPDATE orders SET status='pendente', motoboy_id=NULL, motoboy_name=NULL, t_aceito=NULL, pending_until=$1 WHERE id=$2",
        [Date.now() + 15000, order.id]
      );
      if (bot) {
        const groupId = process.env.TELEGRAM_GROUP_ID;
        let lojaRepostNome = order.loja_name;
        if (!lojaRepostNome && order.loja_user) {
          const lrRes = await pool.query("SELECT name FROM users WHERE username=$1", [order.loja_user]);
          if (lrRes.rows.length > 0) lojaRepostNome = lrRes.rows[0].name;
        }
        lojaRepostNome = lojaRepostNome || order.loja_user;
        const msgRepost = `Pedido #${order.id} disponivel novamente!\n\nLoja: ${lojaRepostNome}\nMotoboy ganha: R$ ${parseFloat(order.valor_motoboy).toFixed(2)}\nDistancia: ${order.distancia} km\n\nMotoboy anterior nao chegou no prazo.`;
        if (groupId) bot.sendMessage(groupId, msgRepost).catch(() => {});
        const motoboys = await pool.query("SELECT telegram_id FROM users WHERE role='motoboy' AND online=true AND telegram_id IS NOT NULL");
        motoboys.rows.forEach(mb => bot.sendMessage(mb.telegram_id, msgRepost).catch(() => {}));
      }
    }
  } catch(e) { console.error('[JOB] Erro ao verificar chegadas:', e.message); }
}

app.post('/orders/:id/launch', async (req, res) => {
  try {
    const r = await pool.query("UPDATE orders SET status='pendente', launch_at=0, pending_until=$1 WHERE id=$2 AND status='em_preparo' RETURNING *", [Date.now()+15000, req.params.id]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Pedido nao encontrado ou ja lancado.' });
    const pedido = r.rows[0];
    if (bot) {
      let lojaNome = pedido.loja_name;
      if (!lojaNome && pedido.loja_user) {
        const lr = await pool.query("SELECT name FROM users WHERE username=$1", [pedido.loja_user]);
        if (lr.rows.length > 0) lojaNome = lr.rows[0].name;
      }
      lojaNome = lojaNome || pedido.loja_user;
      const pagLabel = ({dinheiro:'Dinheiro',maquina:'Maquina',pix:'PIX'}[pedido.tipo_pagamento] || pedido.tipo_pagamento || '-');
      const msgLancado = `Pedido #${pedido.id} DISPONIVEL AGORA!\n\nLoja: ${lojaNome}\nPagamento: ${pagLabel}\nMotoboy ganha: R$ ${parseFloat(pedido.valor_motoboy).toFixed(2)}\nDistancia: ${pedido.distancia} km\n\nPedido pronto! Aceite agora.`;
      const groupId = process.env.TELEGRAM_GROUP_ID;
      if (groupId) bot.sendMessage(groupId, msgLancado).catch(() => {});
      const motoboys = await pool.query("SELECT telegram_id FROM users WHERE role='motoboy' AND online=true AND telegram_id IS NOT NULL");
      motoboys.rows.forEach(mb => bot.sendMessage(mb.telegram_id, msgLancado).catch(() => {}));
    }
    res.json(r.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ══════════════════════════════════════════════════════════════════
// MÓDULO DE INDICAÇÃO
// ══════════════════════════════════════════════════════════════════

// GET /referral-settings
app.get('/referral-settings', async (req, res) => {
  try {
    const r = await pool.query('SELECT * FROM referral_settings WHERE id=1');
    res.json(r.rows[0] || {});
  } catch(e) { res.status(500).json({error: e.message}); }
});

// PUT /referral-settings — admin atualiza regras
app.put('/referral-settings', async (req, res) => {
  try {
    const { ativo, comissao_por_pedido_loja, bonus_motoboy_meta, meta_pedidos_motoboy, prazo_meta_dias, validade_indicacao_loja_dias } = req.body;
    await pool.query(`UPDATE referral_settings SET
      ativo=$1, comissao_por_pedido_loja=$2, bonus_motoboy_meta=$3,
      meta_pedidos_motoboy=$4, prazo_meta_dias=$5, validade_indicacao_loja_dias=$6,
      updated_at=NOW() WHERE id=1`,
      [ativo, comissao_por_pedido_loja, bonus_motoboy_meta, meta_pedidos_motoboy, prazo_meta_dias, validade_indicacao_loja_dias]);
    res.json({ok: true});
  } catch(e) { res.status(500).json({error: e.message}); }
});

// GET /referrals — admin: todos os registros
app.get('/referrals', async (req, res) => {
  try {
    const r = await pool.query('SELECT * FROM referrals ORDER BY created_at DESC');
    res.json(r.rows);
  } catch(e) { res.status(500).json({error: e.message}); }
});

// GET /referrals/summary — ranking de indicadores
app.get('/referrals/summary', async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT r.referrer_id, r.referrer_name,
        COUNT(r.id) AS total_indicados,
        COALESCE(SUM(r.total_ganho),0) AS total_ganhos,
        SUM(CASE WHEN r.bonus_pago THEN 1 ELSE 0 END) AS bonus_pagos,
        SUM(CASE WHEN r.status_ref='ativo' THEN 1 ELSE 0 END) AS indicacoes_ativas
      FROM referrals r
      GROUP BY r.referrer_id, r.referrer_name
      ORDER BY total_ganhos DESC`);
    res.json(r.rows);
  } catch(e) { res.status(500).json({error: e.message}); }
});

// GET /referrals/user/:userId — indicados e ganhos de um usuário
app.get('/referrals/user/:userId', async (req, res) => {
  try {
    const id = req.params.userId;
    const referred = await pool.query('SELECT * FROM referrals WHERE referrer_id=$1 ORDER BY created_at DESC', [id]);
    const earnings = await pool.query('SELECT * FROM referral_earnings WHERE referrer_id=$1 ORDER BY created_at DESC LIMIT 100', [id]);
    const total = await pool.query('SELECT COALESCE(SUM(valor),0) AS total FROM referral_earnings WHERE referrer_id=$1', [id]);
    res.json({ referred: referred.rows, earnings: earnings.rows, total_ganhos: parseFloat(total.rows[0].total) });
  } catch(e) { res.status(500).json({error: e.message}); }
});

// GET /referral-code/:userId — gera/retorna código único
app.get('/referral-code/:userId', async (req, res) => {
  try {
    const id = req.params.userId;
    const user = await pool.query('SELECT referral_code, name FROM users WHERE id=$1', [id]);
    if (user.rows.length === 0) return res.status(404).json({error: 'Usuário não encontrado'});
    let code = user.rows[0].referral_code;
    if (!code) {
      const base = (user.rows[0].name || 'USER').replace(/[^A-Za-z0-9]/g,'').substring(0,4).toUpperCase();
      code = base + String(id).padStart(4,'0');
      await pool.query('UPDATE users SET referral_code=$1 WHERE id=$2', [code, id]);
    }
    res.json({code});
  } catch(e) { res.status(500).json({error: e.message}); }
});

// POST /referrals/apply — vincula indicação ao cadastrar
app.post('/referrals/apply', async (req, res) => {
  try {
    const { referred_id, referral_code } = req.body;
    if (!referred_id || !referral_code) return res.status(400).json({error: 'Dados incompletos'});
    const refUser = await pool.query('SELECT * FROM users WHERE referral_code=$1', [referral_code.trim().toUpperCase()]);
    if (refUser.rows.length === 0) return res.status(404).json({error: 'Código inválido'});
    const referrer = refUser.rows[0];
    const newUser = await pool.query('SELECT * FROM users WHERE id=$1', [referred_id]);
    if (newUser.rows.length === 0) return res.status(404).json({error: 'Usuário não encontrado'});
    const referred = newUser.rows[0];
    const existing = await pool.query('SELECT id FROM referrals WHERE referred_id=$1', [referred_id]);
    if (existing.rows.length > 0) return res.status(409).json({error: 'Usuário já foi indicado'});
    const cfg = await pool.query('SELECT * FROM referral_settings WHERE id=1');
    const settings = cfg.rows[0] || {};
    if (!settings.ativo) return res.json({ok: true, msg: 'Programa inativo'});
    let dataFim = null;
    let metaPedidos = 0;
    let bonusValor = 0;
    if (referred.role === 'loja') {
      const dias = parseInt(settings.validade_indicacao_loja_dias || 90);
      dataFim = new Date(Date.now() + dias * 86400000);
    } else if (referred.role === 'motoboy') {
      const dias = parseInt(settings.prazo_meta_dias || 30);
      dataFim = new Date(Date.now() + dias * 86400000);
      metaPedidos = parseInt(settings.meta_pedidos_motoboy || 100);
      bonusValor = parseFloat(settings.bonus_motoboy_meta || 150);
    }
    await pool.query(`INSERT INTO referrals
      (referrer_id, referrer_name, referred_id, referred_name, referred_role, data_fim, meta_pedidos, bonus_valor)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
      [referrer.id, referrer.name, referred.id, referred.name, referred.role, dataFim, metaPedidos, bonusValor]);
    res.json({ok: true, data_fim: dataFim, meta: metaPedidos, bonus: bonusValor});
  } catch(e) { res.status(500).json({error: e.message}); }
});

// PUT /referrals/:id — admin edita uma indicação
app.put('/referrals/:id', async (req, res) => {
  try {
    const { referrer_id, data_fim, meta_pedidos, bonus_valor, status_ref, bonus_pago, total_pedidos_validos } = req.body;
    const flds = [];
    const vals = [];
    let i = 1;
    if (referrer_id !== undefined) { flds.push('referrer_id=$' + i++); vals.push(referrer_id); }
    if (data_fim !== undefined) { flds.push('data_fim=$' + i++); vals.push(data_fim || null); }
    if (meta_pedidos !== undefined) { flds.push('meta_pedidos=$' + i++); vals.push(meta_pedidos); }
    if (bonus_valor !== undefined) { flds.push('bonus_valor=$' + i++); vals.push(bonus_valor); }
    if (status_ref !== undefined) { flds.push('status_ref=$' + i++); vals.push(status_ref); }
    if (bonus_pago !== undefined) { flds.push('bonus_pago=$' + i++); vals.push(bonus_pago); }
    if (total_pedidos_validos !== undefined) { flds.push('total_pedidos_validos=$' + i++); vals.push(total_pedidos_validos); }
    if (flds.length === 0) return res.status(400).json({error: 'Nenhum campo'});
    vals.push(req.params.id);
    await pool.query('UPDATE referrals SET ' + flds.join(',') + ' WHERE id=$' + i, vals);
    res.json({ok: true});
  } catch(e) { res.status(500).json({error: e.message}); }
});

// DELETE /referrals/:id — admin remove indicação
app.delete('/referrals/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM referrals WHERE id=$1', [req.params.id]);
    res.json({ok: true});
  } catch(e) { res.status(500).json({error: e.message}); }
});

// POST /referrals/cleanup — limpa registros finalizados há mais de 20 dias
app.post('/referrals/cleanup', async (req, res) => {
  try {
    const result = await pool.query(`DELETE FROM referrals
      WHERE status_ref IN ('concluido','expirado','cancelado')
        AND COALESCE(data_conclusao, data_fim) < NOW() - INTERVAL '20 days'
        AND bonus_pago = true
      RETURNING id`);
    const earnResult = await pool.query(`DELETE FROM referral_earnings
      WHERE created_at < NOW() - INTERVAL '110 days'
        AND referrer_id NOT IN (SELECT referrer_id FROM referrals WHERE status_ref='ativo')
      RETURNING id`);
    res.json({refs_deletados: result.rows.length, earnings_deletados: earnResult.rows.length});
  } catch(e) { res.status(500).json({error: e.message}); }
});

// ── VITRINE / CARDAPIO ──────────────────────────────────────────────────

app.get('/vitrine/lojas', async (req, res) => {
  try {
    const r = await pool.query(`SELECT u.id, u.username, u.name, u.address, u.phone, vc.descricao_loja, vc.banner_url, vc.categoria_loja, vc.tempo_entrega_min, vc.tempo_entrega_max FROM users u INNER JOIN vitrine_config vc ON vc.loja_id = u.id WHERE u.role='loja' AND u.approved=true AND vc.ativa=true ORDER BY u.name`);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/vitrine/lojas/:id', async (req, res) => {
  try {
    const r = await pool.query(`SELECT u.id, u.username, u.name, u.address, u.phone, vc.descricao_loja, vc.banner_url, vc.categoria_loja, vc.tempo_entrega_min, vc.tempo_entrega_max FROM users u LEFT JOIN vitrine_config vc ON vc.loja_id = u.id WHERE u.id=$1 AND u.role='loja'`, [req.params.id]);
    if (!r.rows.length) return res.status(404).json({ error: 'Loja nao encontrada' });
    res.json(r.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/vitrine/lojas/:id/produtos', async (req, res) => {
  try {
    const r = await pool.query('SELECT id, nome, descricao, preco, categoria, foto_url, ordem FROM produtos WHERE loja_id=$1 AND ativo=true ORDER BY categoria, ordem, nome', [req.params.id]);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/produtos', async (req, res) => {
  try {
    const { loja_id } = req.query;
    if (!loja_id) return res.status(400).json({ error: 'loja_id obrigatorio' });
    const r = await pool.query('SELECT * FROM produtos WHERE loja_id=$1 ORDER BY categoria, ordem, nome', [loja_id]);
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/produtos', async (req, res) => {
  try {
    const { loja_id, nome, descricao, preco, categoria, foto_url, ordem } = req.body;
    const r = await pool.query('INSERT INTO produtos (loja_id, nome, descricao, preco, categoria, foto_url, ordem) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *', [loja_id, nome, descricao||null, preco||0, categoria||null, foto_url||null, ordem||0]);
    res.json(r.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/produtos/:id', async (req, res) => {
  try {
    const { nome, descricao, preco, categoria, foto_url, ativo, ordem } = req.body;
    const r = await pool.query('UPDATE produtos SET nome=$1, descricao=$2, preco=$3, categoria=$4, foto_url=$5, ativo=$6, ordem=$7 WHERE id=$8 RETURNING *', [nome, descricao||null, preco||0, categoria||null, foto_url||null, ativo!==false, ordem||0, req.params.id]);
    res.json(r.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/produtos/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM produtos WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/vitrine/config/:loja_id', async (req, res) => {
  try {
    const r = await pool.query('SELECT * FROM vitrine_config WHERE loja_id=$1', [req.params.loja_id]);
    res.json(r.rows[0] || null);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/vitrine/config/:loja_id', async (req, res) => {
    try {
          const { banner_url, descricao, horario_abertura, horario_fechamento, tempo_entrega_min, tempo_entrega_max } = req.body;
          const lojaId = req.params.loja_id;
          await pool.query(
                  `INSERT INTO vitrine_config (loja_id, banner_url, descricao, horario_abertura, horario_fechamento, tempo_entrega_min, tempo_entrega_max)
                         VALUES ($1,$2,$3,$4,$5,$6,$7)
                                ON CONFLICT (loja_id) DO UPDATE SET
                                         banner_url=EXCLUDED.banner_url, descricao=EXCLUDED.descricao,
                                                  horario_abertura=EXCLUDED.horario_abertura, horario_fechamento=EXCLUDED.horario_fechamento,
                                                           tempo_entrega_min=EXCLUDED.tempo_entrega_min, tempo_entrega_max=EXCLUDED.tempo_entrega_max`,
                  [lojaId, banner_url, descricao, horario_abertura, horario_fechamento, tempo_entrega_min, tempo_entrega_max]
                );
          res.json({ ok: true });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── PAGAR AO RESTAURANTE ────────────────────────────────────────────────────
app.get('/orders/:id/pagar-restaurante/info', async (req, res) => {
  try {
    const orderId = parseInt(req.params.id);
    const ord = await pool.query('SELECT * FROM orders WHERE id=$1', [orderId]);
    if (ord.rows.length === 0) return res.status(404).json({ error: 'Pedido nao encontrado' });
    const order = ord.rows[0];
    const lojaU = await pool.query('SELECT credit FROM users WHERE username=$1', [order.loja_user]);
    const lojaCredit = lojaU.rows.length > 0 ? (parseFloat(lojaU.rows[0].credit)||0) : 0;
    const valorPedido = parseFloat(order.valor_pedido)||0;
    const maxValor = valorPedido + lojaCredit;
    const pending = await pool.query("SELECT id FROM pagamento_restaurante WHERE order_id=$1 AND status='pendente' AND expires_at>$2", [orderId, Date.now()]);
    res.json({ max_valor: maxValor, has_pending: pending.rows.length > 0, loja_credit: lojaCredit, valor_pedido: valorPedido });
  } catch(e) { res.status(500).json({ error: e.message }); }
});


app.post('/orders/:id/pagar-restaurante', async (req, res) => {
  try {
    const orderId = parseInt(req.params.id);
    const { motoboy_id, valor } = req.body;
    if (!motoboy_id || !valor || valor <= 0) return res.status(400).json({ error: 'Dados invalidos' });
    const amount = parseFloat(valor);
    const ord = await pool.query('SELECT * FROM orders WHERE id=$1', [orderId]);
    if (ord.rows.length === 0) return res.status(404).json({ error: 'Pedido nao encontrado' });
    const order = ord.rows[0];
    if (order.tipo_pagamento !== 'dinheiro') return res.status(400).json({ error: 'Apenas corridas em dinheiro' });
    if (String(order.motoboy_id) !== String(motoboy_id)) return res.status(403).json({ error: 'Nao autorizado' });
    const lojaU = await pool.query('SELECT credit FROM users WHERE username=$1', [order.loja_user]);
    if (lojaU.rows.length === 0) return res.status(404).json({ error: 'Loja nao encontrada' });
    const lojaCredit = parseFloat(lojaU.rows[0].credit) || 0;
    const valorPedido = parseFloat(order.valor_pedido) || 0;
    const maxValor = valorPedido + lojaCredit;
    if (amount > maxValor) return res.status(400).json({ error: 'Valor excede o maximo permitido', max: maxValor });
    const existing = await pool.query("SELECT id FROM pagamento_restaurante WHERE order_id=$1 AND status='pendente'", [orderId]);
    if (existing.rows.length > 0) return res.status(400).json({ error: 'Ja existe solicitacao pendente' });
    const expiresAt = Date.now() + 10 * 60 * 1000;
    const motU = await pool.query('SELECT name FROM users WHERE id=$1', [motoboy_id]);
    const motName = motU.rows.length > 0 ? motU.rows[0].name : 'Motoboy';
    const ins = await pool.query(
      "INSERT INTO pagamento_restaurante (order_id,motoboy_id,motoboy_name,loja_user,valor,status,expires_at) VALUES ($1,$2,$3,$4,$5,'pendente',$6) RETURNING id",
      [orderId, motoboy_id, motName, order.loja_user, amount, expiresAt]
    );
    const lojaInfo = await pool.query('SELECT telegram_id FROM users WHERE username=$1', [order.loja_user]);
    if (lojaInfo.rows.length > 0 && lojaInfo.rows[0].telegram_id && bot) {
      const msg = String.fromCodePoint(128184) + ' PAGAMENTO AO RESTAURANTE\n\nMotoboy ' + motName + ' quer pagar R$ ' + amount.toFixed(2) + ' ao restaurante.\nPedido #' + orderId + '\n\nAcesse o painel para aceitar ou recusar (10 minutos).';
      bot.sendMessage(lojaInfo.rows[0].telegram_id, msg).catch(()=>{});
    }
    res.json({ ok: true, id: ins.rows[0].id, expires_at: expiresAt, max_valor: maxValor });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/pagamento-restaurante/loja/:loja_user', async (req, res) => {
  try {
    const r = await pool.query(
      "SELECT * FROM pagamento_restaurante WHERE loja_user=$1 AND status='pendente' AND expires_at > $2 ORDER BY created_at DESC",
      [req.params.loja_user, Date.now()]
    );
    res.json(r.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/pagamento-restaurante/order/:order_id', async (req, res) => {
  try {
    const r = await pool.query(
      "SELECT * FROM pagamento_restaurante WHERE order_id=$1 ORDER BY created_at DESC LIMIT 1",
      [req.params.order_id]
    );
    res.json(r.rows.length > 0 ? r.rows[0] : { status: 'nenhuma' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/pagamento-restaurante/:id/responder', async (req, res) => {
  try {
    const { id } = req.params;
    const { decisao, loja_user } = req.body;
    if (!['aceito','recusado'].includes(decisao)) return res.status(400).json({ error: 'Decisao invalida' });
    const pr = await pool.query("SELECT * FROM pagamento_restaurante WHERE id=$1", [id]);
    if (pr.rows.length === 0) return res.status(404).json({ error: 'Nao encontrado' });
    const pag = pr.rows[0];
    if (pag.loja_user !== loja_user) return res.status(403).json({ error: 'Nao autorizado' });
    if (pag.status !== 'pendente') return res.status(400).json({ error: 'Nao esta pendente' });
    if (Date.now() > pag.expires_at) return res.status(400).json({ error: 'Expirado' });
    await pool.query("UPDATE pagamento_restaurante SET status=$1 WHERE id=$2", [decisao, id]);
    if (decisao === 'aceito') {
      const valor = parseFloat(pag.valor);
      await pool.query('UPDATE users SET balance = COALESCE(balance,0) + $1 WHERE id=$2', [valor, pag.motoboy_id]);
      await pool.query('UPDATE users SET credit = COALESCE(credit,0) - $1 WHERE username=$2', [valor, pag.loja_user]);
      const motInfo = await pool.query('SELECT telegram_id FROM users WHERE id=$1', [pag.motoboy_id]);
      if (motInfo.rows.length > 0 && motInfo.rows[0].telegram_id && bot) {
        const msg = String.fromCodePoint(9989) + ' Pagamento ACEITO! R$ ' + valor.toFixed(2) + ' creditado no seu saldo.';
        bot.sendMessage(motInfo.rows[0].telegram_id, msg).catch(()=>{});
      }
    }
    res.json({ ok: true, decisao });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/pagamento-restaurante/loja/:username', async (req, res) => {
    try {
          const now = Date.now();
          const r = await pool.query(
                  "SELECT * FROM pagamento_restaurante WHERE loja_user=$1 AND status='pendente' AND expires_at > $2 ORDER BY created_at DESC LIMIT 1",
                  [req.params.username, now]
                );
          res.json(r.rows.length > 0 ? r.rows[0] : null);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

async function checkAndLaunchOrders() {
  try {
    const now = Date.now();
    const r = await pool.query("SELECT * FROM orders WHERE status='em_preparo' AND launch_at > 0 AND launch_at <= $1", [now]);
    for (const pedido of r.rows) {
      await pool.query("UPDATE orders SET status='pendente', pending_until=$1 WHERE id=$2", [now + 15000, pedido.id]);
      if (bot) {
        let lojaNome = pedido.loja_name;
        if (!lojaNome && pedido.loja_user) {
          const lr = await pool.query("SELECT name FROM users WHERE username=$1", [pedido.loja_user]);
          if (lr.rows.length > 0) lojaNome = lr.rows[0].name;
        }
        lojaNome = lojaNome || pedido.loja_user;
        const pagLabel = ({dinheiro:'Dinheiro',maquina:'Maquina',pix:'PIX'}[pedido.tipo_pagamento] || pedido.tipo_pagamento || '-');
        const msgAuto = `Pedido #${pedido.id} DISPONIVEL - Lancamento Automatico!\n\nLoja: ${lojaNome}\nPagamento: ${pagLabel}\nMotoboy ganha: R$ ${parseFloat(pedido.valor_motoboy).toFixed(2)}\nDistancia: ${pedido.distancia} km\n\nTimer expirou - pedido agora no sistema!`;
        const groupId = process.env.TELEGRAM_GROUP_ID;
        if (groupId) bot.sendMessage(groupId, msgAuto).catch(() => {});
        const motoboys = await pool.query("SELECT telegram_id FROM users WHERE role='motoboy' AND online=true AND telegram_id IS NOT NULL");
        motoboys.rows.forEach(mb => bot.sendMessage(mb.telegram_id, msgAuto).catch(() => {}));
      }
      console.log(`[JOB] Pedido #${pedido.id} lancado automaticamente.`);
    }
  } catch(e) { console.error('[JOB] Erro ao lancar pedidos:', e.message); }
}

async function checkPendingOrdersAlert() {
  try {
    const rp = await pool.query("SELECT id, loja_name, cod_pedido FROM orders WHERE status='pendente' AND created_at < NOW() - INTERVAL '10 minutes' AND notified_admin IS NOT TRUE");
    for (const ord of rp.rows) {
      if (bot) await bot.sendMessage(ADMIN_ID, '\u23F0 Pedido Pendente +10min!\n\nPedido #' + ord.id + '\nLoja: ' + (ord.loja_name || '-') + '\nCodigo: ' + (ord.cod_pedido || '-') + '\n\nAcesse o painel admin.').catch(function(){});
      await pool.query('UPDATE orders SET notified_admin=true WHERE id=$1', [ord.id]);
    }
  } catch(e) { console.error('[JOB] checkPendingOrders:', e.message); }
}


async function expirePagamentosRestaurante() {
  try {
    await pool.query("UPDATE pagamento_restaurante SET status='expirado' WHERE status='pendente' AND expires_at <= $1", [Date.now()]);
  } catch(e) {}
}
initDB().then(() => {
  app.listen(PORT, () => console.log(`FlashDrop backend porta ${PORT}`));
  setInterval(checkLateArrivals, 60 * 1000);
  setInterval(checkPendingOrdersAlert, 5 * 60 * 1000);
  console.log('[JOB] Verificador de pedidos pendentes iniciado (5min)');
  setInterval(checkAndLaunchOrders, 30 * 1000);
  setInterval(expirePagamentosRestaurante, 30 * 1000);
  console.log('[JOB] Verificador de chegada iniciado (60s)');
  console.log('[JOB] Lancador automatico de pedidos iniciado (30s)');
});
