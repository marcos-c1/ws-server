const express = require("express");
const http = require("http");
const { WebSocket } = require("ws");
const { Server } = require("socket.io");
const { normalize } = require("path");
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
const currentDay = new Date().getDay();

/** Estado do twSocket (Twelve Data) */
let twSocket, bSocket; // WebSocket único
let twSocketReady,
  bSocketReady = false;
const pendingQueue = []; // mensagens a enviar enquanto o socket não abre
let retryTWCount,
  retryBCount = 0;

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

function reconnectTW() {
  if (retryTWCount < LIMIT_RETRIES && !twSocketReady) {
    retryTWCount++;
    setTimeout(() => {
      createTWSocket();
      reconnectTW();
    }, 2000);
  }
}

function reconnectBinance() {
  if (retryBCount < LIMIT_RETRIES && !bSocketReady) {
    retryBCount++;
    setTimeout(() => {
      createBSocket();
      reconnectBinance();
    }, 2000);
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

    // // Heartbeat simples (ajuste intervalo conforme doc)
    // clearInterval(heartbeatTimer);
    // heartbeatTimer = setInterval(() => {
    //   try {
    //     if (twSocket?.readyState === WebSocket.OPEN) {
    //       twSocket.ping?.(); // alguns servidores respondem com pong
    //     }
    //   } catch (_) {}
    // }, 15000);
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

    retryTWCount = 0;

    reconnectTW();
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
    // clearInterval(heartbeatTimer);
    // heartbeatTimer = setInterval(() => {
    //   try {
    //     if (bSocket?.readyState === WebSocket.OPEN) {
    //       bSocket.ping?.(); // alguns servidores respondem com pong
    //     }
    //   } catch (_) {}
    // }, 15000);
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
    retryBCount = 0;
    reconnectBinance();
  });
}
/** Conecta (ou reconecta) ao WebSocket da Binance e TwelveData com retry */
function connect() {
  if (currentDay != 6 && currentDay != 0)
    // Sábado e Domingo o mercado Forex n funciona
    createTWSocket();
  createBSocket();
}

connect();

function checkSymbol(symbol) {
  if (symbol.search("/") >= 0) return 0; // 0 for Forex
  return 1;
}

/** Helpers para (des)inscrição de símbolos no twSocket */
function subscribeSymbol(symbol, interval, socketId) {
  // Se já existe, só garante estado
  let set = symbolSubscribers.get(symbol);
  if (!set) {
    set = new Set();
    symbolSubscribers.set(symbol, set);
    // Primeiro assinante → assina na Twelve Data
    let isCrypto = checkSymbol(symbol);
    if (!isCrypto) socketSend(twSocket, TD_SUB_MSG(symbol));
    else
      socketSend(bSocket, B_SUB_MSG(symbol + `@kline_${interval}`, socketId));
  }
  set.add(socketId);
}

function unsubscribeSymbol(symbol, interval, socketId) {
  const set = symbolSubscribers.get(symbol);
  if (!set) return;
  set.delete(socketId);

  if (set && set.size === 0) {
    symbolSubscribers.delete(symbol);
    let isCrypto = checkSymbol(symbol);
    if (!isCrypto) socketSend(twSocket, TD_UNSUB_MSG(symbol));
    else
      socketSend(bSocket, B_UNSUB_MSG(symbol + `@kline_${interval}`, socketId));
  }
}

function normalizeInterval(interval) {
  switch (interval) {
    case "1m":
      return true;
    case "3m":
      return true;
    case "5m":
      return true;
    case "15m":
      return true;
    case "30m":
      return true;
    case "1h":
      return true;
    case "1d":
      return true;
    case "1w":
      return true;
    case "1M":
      return true;
  }

  return false;
}

/** Socket.io (clientes) */
io.on("connection", (socket) => {
  console.log("Client id " + socket.id + " conectado.");
  clientSymbols.set(socket.id, new Set());
  normalizeInterval;
  // Cliente pede para assistir um símbolo
  socket.on("watch", (symbolWithInterval) => {
    // Normalize antes de receber o simbolo
    // Ex: btcusdt (crypto), EUR/USD (forex)
    const symbol = String(symbolWithInterval.symbol).trim();
    const interval = String(symbolWithInterval.interval).trim();
    const isNormalizedInterval = normalizeInterval(interval);

    if (!symbol) return;
    if (interval && !isNormalizedInterval) return;

    // Adiciona cliente na lista do símbolo
    const subs = symbolSubscribers.get(symbol) || new Set();
    subs.add(socket.id);

    // Se foi o primeiro do símbolo, assina
    if (subs.size === 1) subscribeSymbol(symbol, interval, socket.id);

    // Marca no mapa do cliente
    clientSymbols.get(socket.id)?.add(symbol);

    // (Opcional) confirmar
    socket.emit("watch:ok", { symbol });
  });

  // Cliente para de assistir
  socket.on("unwatch", (symbolWithInterval) => {
    const symbol = String(symbolWithInterval.symbol).trim();
    const interval = String(symbolWithInterval.interval).trim();
    const isNormalizedInterval = normalizeInterval(interval);

    if (!symbol) return;
    if (interval && !isNormalizedInterval) return;

    const subs = symbolSubscribers.get(symbol);
    if (!subs) return;

    subs.delete(socket.id);
    clientSymbols.get(socket.id)?.delete(symbol);

    if (subs.size === 0) unsubscribeSymbol(symbol, interval, socket.id);

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
