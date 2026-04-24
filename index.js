const jwt = require('jsonwebtoken');
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

// Telegram Bot
const bot = process.env.TELEGRAM_BOT_TOKEN ? new TelegramBot(process.env.TELEGRAM_BOT_TOKEN, {polling: true}) : null;

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
      created_at TIMESTAMP DEFAULT NOW()
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
    const { username, password, role, name, address, phone, vehicle, cpf } = req.body;
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
      'INSERT INTO users (username,password,role,name,address,phone,vehicle,cpf,approved) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *',
      [username, password, role, name, address, phone, vehicle, cpf || null, approved]
    );
    let prefix2, digits2;
    if (role === 'motoboy') { prefix2 = 'M'; digits2 = 4; }
    else if (role === 'loja') { prefix2 = 'L'; digits2 = 6; }
    if (prefix2) {
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
    const { username, password, role, name, address, phone, vehicle, cpf } = req.body;
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
      [username, password, role || 'motoboy', name, address, phone, vehicle, cpf || null]
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
        message: 'Voce nao possui limite de credito definido. Solicite ao administrador para configurar seu limite.'
      });
    }

    const blocked = balance <= -individualLimit;
    res.json({
      can_accept_cash: !blocked,
      balance,
      individual_limit: individualLimit,
      blocked,
      message: blocked ? 'Voce atingiu o limite de credito. Regularize seu saldo com o suporte para voltar a aceitar corridas em dinheiro.' : null
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/orders', async (req, res) => {
  const d = req.body;
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
                error: 'Voce nao possui limite de credito definido para corridas em dinheiro. Solicite ao administrador para configurar seu limite.',
                credit_blocked: true
              });
            }

            if (balance <= -individualLimit) {
              // Saldo negativo ultrapassou o limite individual
              return res.status(403).json({
                error: 'Voce atingiu o limite de credito. Regularize seu saldo com o suporte para voltar a aceitar corridas em dinheiro.',
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
    }

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

app.post('/distance', async (req, res) => {
  try {
    const { origin, destination } = req.body;
    if (!origin || !destination) return res.status(400).json({ error: 'Origin e destination obrigatorios' });
    const apiKey = process.env.GOOGLE_MAPS_KEY;
    if (!apiKey) return res.status(500).json({ error: 'API key nao configurada' });
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

initDB().then(() => {
  app.listen(PORT, () => console.log(`FlashDrop backend porta ${PORT}`));
  setInterval(checkLateArrivals, 60 * 1000);
  setInterval(checkAndLaunchOrders, 30 * 1000);
  console.log('[JOB] Verificador de chegada iniciado (60s)');
  console.log('[JOB] Lancador automatico de pedidos iniciado (30s)');
});
