const express = require("express");
const http = require("http");
const { WebSocket } = require("ws");
const { Server } = require("socket.io");
require("dotenv").config();

const API_KEY = process.env.TWELVE_DATA_KEY;
const LIMIT_RETRIES = 5;

const WSS_TW_URL = `wss://ws.twelvedata.com/v1/quotes/price?apikey=${API_KEY}`;
const WSS_BINANCE_URL = `wss://fstream.binance.com/ws`;

// Subscribe to bnbusdt@kline_1m

// Exemplos genéricos (ajuste os campos para o payload que sua instância exige)
const TD_SUB_MSG = (symbol) =>
  JSON.stringify({ action: "subscribe", params: { symbols: [symbol] } });

const B_SUB_MSG = (symbol, socketId) =>
  JSON.stringify({ method: "SUBSCRIBE", params: [symbol], id: socketId });

const TD_UNSUB_MSG = (symbol) =>
  JSON.stringify({
    action: "unsubscribe",
    params: { symbols: [symbol] },
  });

const B_UNSUB_MSG = (symbol, socketId) =>
  JSON.stringify({ method: "UNSUBSCRIBE", params: [symbol], id: socketId });

// ========================================================

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

/** Estado do twSocket (Twelve Data) */
let twSocket, bSocket; // WebSocket único
let twSocketReady,
  bSocketReady = false;
const pendingQueue = []; // mensagens a enviar enquanto o socket não abre
let heartbeatTimer;

/** Tabelas de roteamento */
const symbolSubscribers = new Map(); // symbol -> Set(socket.id)
const clientSymbols = new Map(); // socket.id -> Set(symbol)

/** Util: envia (ou fila) mensagens de subscribe ou unsubscribe ao ws da twSocket */
function socketSend(socket, msg) {
  if (socket?.readyState === WebSocket.OPEN) {
    socket.send(msg);
  } else {
    pendingQueue.push(msg);
  }
}

function createTWSocket() {
  twSocketReady = false;
  twSocket = new WebSocket(WSS_TW_URL);

  twSocket.on("open", () => {
    twSocketReady = true;
    console.log("WS Twelve Data conectado");
    // Drena mensagens pendentes
    while (pendingQueue.length) socketSend(twSocket, pendingQueue.shift());

    // Heartbeat simples (ajuste intervalo conforme doc)
    clearInterval(heartbeatTimer);
    heartbeatTimer = setInterval(() => {
      try {
        if (twSocket?.readyState === WebSocket.OPEN) {
          twSocket.ping?.(); // alguns servidores respondem com pong
        }
      } catch (_) {}
    }, 15000);
  });

  twSocket.on("message", (data) => {
    // Normalmente vem JSON por símbolo; encaminhe para os assinantes
    // Adapte conforme o payload da Twelve Data
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }

    // Exemplos de formatos possíveis:
    // msg = { symbol: 'EUR/USD', price: 1.08123, ts: 1710001112, ... }
    // ou msg.data = [{symbol:'EUR/USD', price: ...}, ...]
    const items = Array.isArray(msg?.data) ? msg.data : [msg];
    console.log("Recebi atualizacao TW");

    for (const it of items) {
      const sym = it.symbol || it.s || it.ticker;
      if (!sym) continue;

      const subs = symbolSubscribers.get(sym);
      if (!subs || subs.size === 0) continue;

      // Repassa somente para quem está inscrito
      for (const socketId of subs) {
        io.to(socketId).emit("tick", { symbol: sym, payload: it });
      }
    }
  });

  twSocket.on("close", () => {
    twSocketReady = false;
    clearInterval(heartbeatTimer);

    setTimeout(() => {}, 2000);
    let it = 0;

    // Tenta reconectar com backoff
    while (it < LIMIT_RETRIES || !twSocketReady) {
      setTimeout(() => createBSocket(), 2000);
      it++;
    }
  });

  twSocket.on("error", (err) => {
    console.error("Erro WS TD:", err.message);
  });
}

function createBSocket() {
  bSocketReady = false;
  bSocket = new WebSocket(WSS_BINANCE_URL);

  bSocket.on("open", () => {
    bSocketReady = true;
    console.log("WS Binance conectado");

    // Heartbeat simples (ajuste intervalo conforme doc)
    clearInterval(heartbeatTimer);
    heartbeatTimer = setInterval(() => {
      try {
        if (bSocket?.readyState === WebSocket.OPEN) {
          bSocket.ping?.(); // alguns servidores respondem com pong
        }
      } catch (_) {}
    }, 15000);
  });

  bSocket.on("message", (data) => {
    // Normalmente vem JSON por símbolo; encaminhe para os assinantes
    // Adapte conforme o payload da Twelve Data
    let msg;

    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }
    if (msg.e !== "kline") return;

    const k = msg.k;
    const symbol = msg.s;
    const payload = {
      symbol: symbol,
      interval: k.i,
      openTime: k.t,
      closeTime: k.T,
      open: k.o,
      high: k.h,
      low: k.l,
      close: k.c,
      volume: k.v,
      isClosed: k.x,
    };

    console.log(`${symbol}: ${payload.close}`);

    const subs = symbolSubscribers.get(symbol.toLowerCase());
    console.log(`symbolSubscribers no binance: ${subs}`);

    if (!subs) return;

    // Enviar para todos os clientes inscritos nesse stream
    for (const socketId of subs) {
      io.to(socketId).emit("tick", { symbol: data.s, payload: payload });
    }
  });

  bSocket.on("error", (err) => {
    console.error("Erro WS Binance:", err.message);
  });

  bSocket.on("close", () => {
    bSocketReady = false;
    clearInterval(heartbeatTimer);
    let it = 0;
    // Tenta reconectar com backoff
    while (it < LIMIT_RETRIES || !bSocketReady) {
      setTimeout(() => createBSocket(), 2000);
      it++;
    }
  });
}
/** Conecta (ou reconecta) ao WebSocket da Binance e TwelveData com retry */
async function connect() {
  createTWSocket();
  createBSocket();
}

connect();

function checkSymbol(symbol) {
  console.log(symbol);
  if (symbol.search("/") >= 0) return 0; // 0 for Forex
  return 1;
}

/** Helpers para (des)inscrição de símbolos no twSocket */
function subscribeSymbol(symbol, socketId) {
  // Se já existe, só garante estado
  let set = symbolSubscribers.get(symbol);
  if (!set) {
    set = new Set();
    symbolSubscribers.set(symbol, set);
    // Primeiro assinante → assina na Twelve Data
    let isCrypto = checkSymbol(symbol);
    if (!isCrypto) socketSend(twSocket, TD_SUB_MSG(symbol));
    else socketSend(bSocket, B_SUB_MSG(symbol + "@kline_1m", socketId));
  }
  set.add(socketId);
}

function unsubscribeSymbol(symbol, socketId) {
  const set = symbolSubscribers.get(symbol);
  if (set && set.size === 0) {
    symbolSubscribers.delete(symbol);
    let isCrypto = checkSymbol(symbol);
    if (!isCrypto) socketSend(twSocket, TD_UNSUB_MSG(symbol));
    else socketSend(bSocket, B_UNSUB_MSG(symbol + "@kline_1m", socketId));
  }
}

/** Socket.io (clientes) */
io.on("connection", (socket) => {
  console.log("Client id " + socket.id + " conectado.");
  clientSymbols.set(socket.id, new Set());

  // Cliente pede para assistir um símbolo
  socket.on("watch", (symbolRaw) => {
    // Normalize antes de receber o simbolo
    // Ex: btcusdt (crypto), EUR/USD (forex)
    const symbol = String(symbolRaw).trim();
    if (!symbol) return;

    // Adiciona cliente na lista do símbolo
    const subs = symbolSubscribers.get(symbol) || new Set();
    subs.add(socket.id);

    console.log(`symbolSubscribers no watch: ${symbolSubscribers}`);

    // Se foi o primeiro do símbolo, assina no twSocket
    if (subs.size === 1) subscribeSymbol(symbol, socket.id);

    // Marca no mapa do cliente
    clientSymbols.get(socket.id)?.add(symbol);

    // (Opcional) confirmar
    socket.emit("watch:ok", { symbol });
  });

  // Cliente para de assistir
  socket.on("unwatch", (symbolRaw) => {
    const symbol = String(symbolRaw).trim().toUpperCase();
    const subs = symbolSubscribers.get(symbol);
    if (!subs) return;

    subs.delete(socket.id);
    clientSymbols.get(socket.id)?.delete(symbol);

    if (subs.size === 0) unsubscribeSymbol(symbol, socket.id);

    socket.emit("unwatch:ok", { symbol });
  });

  // Cleanup ao desconectar
  socket.on("disconnect", () => {
    const symbols = clientSymbols.get(socket.id) || new Set();
    for (const s of symbols) {
      const subs = symbolSubscribers.get(s);
      if (!subs) continue;
      subs.delete(socket.id);

      if (subs.size === 0) unsubscribeSymbol(s, socket.id);
    }
    clientSymbols.delete(socket.id);
  });
});

app.get("/", (_, res) => res.send("WS Binance e TW OK"));
const PORT = process.env.PORT || 8080;
server.listen(PORT, () => console.log(`Server rodando na porta :${PORT}`));
