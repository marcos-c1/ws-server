import express from 'express';
import http from 'http';
import WebSocket from 'ws';
import { Server } from 'socket.io';
import pRetry from 'p-retry';
import { config } from "dotenv";
config()


const API_KEY = process.env.TWELVEDATA_KEY || 'SUA_APIKEY_AQUI';

// ===== CONFIGURE AQUI CONFORME A DOC DA TWELVE DATA =====
const WSS_URL = `wss://ws.twelvedata.com/v1/quotes/price?apikey=${API_KEY}`;
const WS_BINANCE = `wss://stream.binance.com:9443/ws`
// Exemplos genéricos (ajuste os campos para o payload que sua instância exige)
const SUB_MSG   = (symbol) => JSON.stringify({ action: 'subscribe',   params: { symbols: [symbol] } });
const UNSUB_MSG = (symbol) => JSON.stringify({ action: 'unsubscribe', params: { symbols: [symbol] } });
// ========================================================

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

/** Estado do tdSocket (Twelve Data) */
let tdSocket, binanceSocket;                  // WebSocket único
let upstreamReady = false;
const pendingQueue = [];       // mensagens a enviar enquanto o socket não abre
let heartbeatTimer;

/** Tabelas de roteamento */
const symbolSubscribers = new Map(); // symbol -> Set(socket.id)
const clientSymbols = new Map();     // socket.id -> Set(symbol)

/** Util: envia (ou fila) mensagens ao tdSocket */
function upstreamSend(msg) {
  if (upstreamReady && tdSocket?.readyState === WebSocket.OPEN) {
    tdSocket.send(msg);
    binanceSocket.send(msg);
  } else {
    pendingQueue.push(msg);
  }
}

/** Conecta (ou reconecta) ao tdSocket com retry */
async function connectUpstream() {
  upstreamReady = false;

  tdSocket = new WebSocket(WSS_URL);
  binanceSocket = new WebSocket(WS_BINANCE);

  // Eventos do tdSocket
  binanceSocket.on('open', () => {
    console.log('Conectado ao Binance WS');
  });

  binanceSocket.on('message', (data) => {
    // Encaminha dados do Binance para os clientes
    let msg;
    console.log(data);

    try { msg = JSON.parse(data.toString()); } catch { return; }

    // Exemplo de mensagem do Binance:
    // { e: 'trade', E: 123456789, s: 'BNBBTC', t: 12345, p: '0.001', q: '100', ... }
    const sym = msg.s;
    if (!sym) return;
    

    const subs = symbolSubscribers.get(sym);
    if (!subs || subs.size === 0) return;

    for (const socketId of subs) {
      io.to(socketId).emit('tick', { symbol: sym, payload: msg });
    }
  });

  binanceSocket.on('close', () => {
    console.log('Desconectado do Binance WS');
    // Tenta reconectar com backoff
    pRetry(connectUpstream, { retries: 5, factor: 2, minTimeout: 1000 }).catch(() => {
      console.error('Falha ao reconectar ao Binance WS.');
    });
  });

  binanceSocket.on('error', (err) => {
    console.error('Erro WS Binance:', err.message);
  });

  tdSocket.on('open', () => {
    upstreamReady = true;
    // Drena mensagens pendentes
    while (pendingQueue.length) upstreamSend(pendingQueue.shift());

    // Heartbeat simples (ajuste intervalo conforme doc)
    clearInterval(heartbeatTimer);
    heartbeatTimer = setInterval(() => {
      try {
        if (tdSocket?.readyState === WebSocket.OPEN) {
          tdSocket.ping?.(); // alguns servidores respondem com pong
        }
      } catch (_) {}
    }, 15000);
  });

  tdSocket.on('message', (data) => {
    // Normalmente vem JSON por símbolo; encaminhe para os assinantes
    // Adapte conforme o payload da Twelve Data
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    // Exemplos de formatos possíveis:
    // msg = { symbol: 'EUR/USD', price: 1.08123, ts: 1710001112, ... }
    // ou msg.data = [{symbol:'EUR/USD', price: ...}, ...]
    const items = Array.isArray(msg?.data) ? msg.data : [msg];

    for (const it of items) {
      const sym = it.symbol || it.s || it.ticker;
      if (!sym) continue;

      const subs = symbolSubscribers.get(sym);
      if (!subs || subs.size === 0) continue;

      // Repassa somente para quem está inscrito
      for (const socketId of subs) {
        io.to(socketId).emit('tick', { symbol: sym, payload: it });
      }
    }
  });

  tdSocket.on('close', () => {
    upstreamReady = false;
    clearInterval(heartbeatTimer);
    // Tenta reconectar com backoff
    pRetry(connectUpstream, { retries: 5, factor: 2, minTimeout: 1000 }).catch(() => {
      console.error('Falha ao reconectar ao tdSocket.');
    });
  });

  tdSocket.on('error', (err) => {
    console.error('Erro WS tdSocket:', err.message);
  });
}

connectUpstream();

/** Helpers para (des)inscrição de símbolos no tdSocket */
function subscribeSymbol(symbol) {
  // Se já existe, só garante estado
  let set = symbolSubscribers.get(symbol);
  if (!set) {
    set = new Set();
    symbolSubscribers.set(symbol, set);
    // Primeiro assinante → assina na Twelve Data
    upstreamSend(SUB_MSG(symbol));
  }
}

function unsubscribeSymbol(symbol) {
  const set = symbolSubscribers.get(symbol);
  if (set && set.size === 0) {
    symbolSubscribers.delete(symbol);
    upstreamSend(UNSUB_MSG(symbol));
  }
}

/** Socket.io (clientes) */
io.on('connection', (socket) => {
  clientSymbols.set(socket.id, new Set());

  // Cliente pede para assistir um símbolo
  socket.on('watch', (symbolRaw) => {
    const symbol = String(symbolRaw).trim().toUpperCase();
    if (!symbol) return;

    // Adiciona cliente na lista do símbolo
    const subs = symbolSubscribers.get(symbol) || new Set();
    subs.add(socket.id);
    symbolSubscribers.set(symbol, subs);

    // Se foi o primeiro do símbolo, assina no tdSocket
    if (subs.size === 1) subscribeSymbol(symbol);

    // Marca no mapa do cliente
    clientSymbols.get(socket.id)?.add(symbol);

    // (Opcional) confirmar
    socket.emit('watch:ok', { symbol });
  });

  // Cliente para de assistir
  socket.on('unwatch', (symbolRaw) => {
    const symbol = String(symbolRaw).trim().toUpperCase();
    const subs = symbolSubscribers.get(symbol);
    if (!subs) return;

    subs.delete(socket.id);
    clientSymbols.get(socket.id)?.delete(symbol);
    if (subs.size === 0) unsubscribeSymbol(symbol);

    socket.emit('unwatch:ok', { symbol });
  });

  // Cleanup ao desconectar
  socket.on('disconnect', () => {
    const symbols = clientSymbols.get(socket.id) || new Set();
    for (const s of symbols) {
      const subs = symbolSubscribers.get(s);
      if (!subs) continue;
      subs.delete(socket.id);
      if (subs.size === 0) unsubscribeSymbol(s);
    }
    clientSymbols.delete(socket.id);
  });
});

app.get('/', (_, res) => res.send('PeakBroker realtime OK'));
const PORT = process.env.PORT || 8080;
server.listen(PORT, () => console.log(`Realtime server on :${PORT}`));