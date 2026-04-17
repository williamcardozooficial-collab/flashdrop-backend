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
const bot = process.env.TELEGRAM_BOT_TOKEN 
  ? new TelegramBot(process.env.TELEGRAM_BOT_TOKEN, {polling: true})
  : null;

// Bot Commands
if (bot) {
  bot.onText(/\/registrar (.+)/, async (msg, match) => {
    const telegram_id = msg.from.id;
    const username = match[1];
    try {
      await pool.query('UPDATE users SET telegram_id=$1 WHERE username=$2', [telegram_id, username]);
      bot.sendMessage(telegram_id, '✅ Registrado! Você receberá notificações de novos pedidos.');
    } catch(e) {
      bot.sendMessage(telegram_id, '❌ Erro ao registrar. Verifique seu username.');
    }
  });

  bot.on('location', async (msg) => {
    const telegram_id = msg.from.id;
    const {latitude, longitude} = msg.location;
    try {
      await pool.query(
        'UPDATE users SET lat=$1, lng=$2, last_location_update=NOW() WHERE telegram_id=$3',
        [latitude, longitude, telegram_id]
      );
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
                                                                                                                                                                                                                                                                                                                                    max_per_motoboy INT DEFAULT 2
                                                                                                                                                                                                                                                                                                                                        );
                                                                                                                                                                                                                                                                                                                                            INSERT INTO settings (id) VALUES (1) ON CONFLICT DO NOTHING;
                                                                                                                                                                                                                                                                                                                                                INSERT INTO users (username,password,role,name) VALUES ('admin','admin123','admin','Administrador') ON CONFLICT DO NOTHING;
                                                                                                                                                                                                                                                                                                                                                  `);
    try { await pool.query("ALTER TABLE orders ADD COLUMN IF NOT EXISTS tipo_pagamento VARCHAR(20) DEFAULT 'dinheiro'"); } catch(e) {}
    try { await pool.query("ALTER TABLE orders ALTER COLUMN motoboy_id TYPE INTEGER USING motoboy_id::INTEGER"); } catch(e) {}
    console.log('DB initialized');
}

app.get('/health', (req, res) => res.json({ ok: true }));

const loginHandler = async (req, res) => {
    const { username, password } = req.body;
    const r = await pool.query('SELECT * FROM users WHERE username=$1 AND password=$2', [username, password]);
    if (r.rows.length === 0) return res.status(401).json({ error: 'Invalido' });
    if (r.rows[0].blocked) return res.status(403).json({ error: 'Bloqueado' });
    res.json(r.rows[0]);
};
app.post('/users/login', loginHandler);
app.post('/api/login', loginHandler);
app.post('/api/admin/login', loginHandler);

app.get('/users', async (req, res) => {
    const r = await pool.query('SELECT * FROM users');
    res.json(r.rows);
});

app.get('/users/:id', async (req, res) => {
    try {
          const r = await pool.query('SELECT * FROM users WHERE id=$1', [req.params.id]);
          if (r.rows.length === 0) return res.status(404).json({ error: 'Nao encontrado' });
          res.json(r.rows[0]);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/users', async (req, res) => {
    const { username, password, role, name, address, phone, vehicle } = req.body;
    const r = await pool.query(
          'INSERT INTO users (username,password,role,name,address,phone,vehicle) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *',
          [username, password, role, name, address, phone, vehicle]
        );
    res.json(r.rows[0]);
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

app.post('/orders', async (req, res) => {
    const d = req.body;
    const r = await pool.query(
          `INSERT INTO orders (loja_user,loja_name,plataforma,endereco_coleta,endereco_entrega,bairro_destino,nome_cliente,telefone_cliente,cod_pedido,cobrar_cliente,tipo_pagamento,valor_pedido,valor_total,valor_motoboy,comissao,distancia,previsao,obs,status,pending_until)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,'pendente',$19) RETURNING *`,
          [d.loja_user,d.loja_name,d.plataforma,d.endereco_coleta,d.endereco_entrega,d.bairro_destino,d.nome_cliente,d.telefone_cliente,d.cod_pedido,d.cobrar_cliente||'nao',d.tipo_pagamento||'dinheiro',d.valor_pedido||0,d.valor_total,d.valor_motoboy,d.comissao,d.distancia,d.previsao,d.obs,Date.now()+15000]
        );
      
  // Notificar motoboys online
  if (bot) {
    const motoboys = await pool.query(
      "SELECT telegram_id, name FROM users WHERE role='motoboy' AND online=true AND telegram_id IS NOT NULL"
    );
    const pedido = r.rows[0];
    motoboys.rows.forEach(mb => {
      bot.sendMessage(mb.telegram_id, 
        `🆕 *Novo Pedido Disponível!*\n\n` +
        `📦 Pedido #${pedido.id}\n` +
        `📍 ${pedido.endereco_entrega}\n` +
        `💰 Você ganha: R$ ${parseFloat(pedido.valor_motoboy).toFixed(2)}\n` +
        `📏 Distância: ${pedido.distancia} km`,
        {parse_mode: 'Markdown'}
      );
    });
  }

    res.json(r.rows[0]);
});

app.put('/orders/:id', async (req, res) => {
    try {
          const fields = req.body;
          const sets = Object.keys(fields).map((k,i) => `${k}=$${i+2}`).join(',');
          const vals = Object.values(fields);
          const r = await pool.query(`UPDATE orders SET ${sets} WHERE id=$1 RETURNING *`, [req.params.id, ...vals]);
          const order = r.rows[0];

      if (fields.status === 'entregue' && order.motoboy_id) {
              const valorMotoboy = parseFloat(order.valor_motoboy) || 0;
              if (valorMotoboy > 0) {
                        await pool.query('UPDATE users SET balance = balance + $1 WHERE id=$2', [valorMotoboy, order.motoboy_id]);
              }
              if (order.cobrar_cliente === 'sim' && order.tipo_pagamento === 'dinheiro') {
                        const valorPedido = parseFloat(order.valor_pedido) || 0;
                        if (valorPedido > 0) {
                                    await pool.query("UPDATE users SET credit = credit + $1 WHERE username=$2", [valorPedido, order.loja_user]);
                        }
              }
      }
          res.json(order);
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/orders/:id', async (req, res) => {
    try {
          await pool.query('DELETE FROM orders WHERE id=$1', [req.params.id]);
          res.json({ ok: true });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/settings', async (req, res) => {
    const r = await pool.query('SELECT * FROM settings WHERE id=1');
    res.json(r.rows[0]);
});

app.put('/settings', async (req, res) => {
    const { min_fee, price_per_km, arrancada, commission, max_per_motoboy } = req.body;
    const r = await pool.query(
          'UPDATE settings SET min_fee=$1,price_per_km=$2,arrancada=$3,commission=$4,max_per_motoboy=$5 WHERE id=1 RETURNING *',
          [min_fee, price_per_km, arrancada, commission, max_per_motoboy]
        );
    res.json(r.rows[0]);
});

app.post('/distance', async (req, res) => {
    const { origin, destination } = req.body;
    try {
          const r = await axios.get('https://maps.googleapis.com/maps/api/distancematrix/json', {
                  params: { origins: origin, destinations: destination, key: process.env.GOOGLE_MAPS_KEY, mode: 'driving' }
          });
          const el = r.data.rows[0].elements[0];
          if (el.status !== 'OK') return res.status(400).json({ error: 'Endereco nao encontrado' });
          res.json({ distance_km: (el.distance.value/1000).toFixed(1), duration_min: Math.ceil(el.duration.value/60) });
    } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/webhook', (req, res) => { res.json({ ok: true }); });

const PORT = process.env.PORT || 3000;
initDB().then(() => app.listen(PORT, () => console.log(`FlashDrop backend porta ${PORT}`)));
