const express = require("express");
const http = require("http");
const { WebSocket } = require("ws");
const { Server } = require("socket.io");
require("dotenv").config();

const API_KEY = process.env.TWELVE_DATA_KEY;
const LIMIT_RETRIES = 5;

const WSS_TW_URL = `wss://ws.twelvedata.com/v1/quotes/price?apikey=${API_KEY}`;
const WSS_BINANCE_URL = `wss://fstream.binance.com/ws`;

// Função helper para identificar símbolos Forex
function isForexSymbol(symbol) {
  const forexSymbols = ["EUR", "GBP", "AUD", "NZD", "USD", "CAD", "CHF", "JPY"];
  const parts = symbol.split("/");
  if (parts.length === 2) {
    return forexSymbols.includes(parts[0]) && forexSymbols.includes(parts[1]);
  }
  // Checa se é um par de 6 caracteres com duas moedas forex
  if (symbol.length === 6) {
    const base = symbol.substring(0, 3);
    const quote = symbol.substring(3, 6);
    return forexSymbols.includes(base) && forexSymbols.includes(quote);
  }
  return false;
}

// Subscribe to bnbusdt@kline_1m

// Exemplos genéricos (ajuste os campos para o payload que sua instância exige)
const TD_SUB_MSG = (symbol) =>
  JSON.stringify({
    action: "subscribe",
    params: { symbols: [{ symbol: symbol, exchange: "Forex" }] },
  });

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
const io = new Server(server, {
  cors: {
    origin: "*"
  }
});
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
    console.log(
      `🔄 Tentando reconectar TwelveData (tentativa ${retryTWCount + 1}/${LIMIT_RETRIES})`,
    );
    retryTWCount++;
    setTimeout(() => {
      createTWSocket();

      // Re-subscreve todos os símbolos Forex ativos
      for (const [symbol, subscribers] of symbolSubscribers.entries()) {
        if (subscribers.size > 0 && isForexSymbol(symbol)) {
          console.log(
            `🔄 Resubscrevendo símbolo Forex após reconexão:`,
            symbol,
          );
          socketSend(twSocket, TD_SUB_MSG(symbol));
        }
      }

      reconnectTW();
    }, 2000);
  }
}

function reconnectBinance() {
  const maxRetries = LIMIT_RETRIES;
  const backoffDelay = Math.min(1000 * Math.pow(2, retryBCount), 30000); // Exponential backoff, max 30s

  if (retryBCount < maxRetries && !bSocketReady) {
    console.log(
      `🔄 Reconectando Binance (tentativa ${retryBCount + 1}/${maxRetries}, delay: ${backoffDelay}ms)`,
    );
    retryBCount++;

    setTimeout(() => {
      // Se já reconectou, não tenta de novo
      if (bSocketReady) {
        console.log("✅ Binance já está conectado, ignorando retry");
        return;
      }

      console.log("🔌 Criando nova conexão Binance...");
      createBSocket();

      // Agenda resubscrições após dar tempo para conectar
      setTimeout(() => {
        if (!bSocketReady) {
          console.log("❌ Socket ainda não está pronto, retry será agendado");
          return;
        }

        // Re-subscreve todos os símbolos Binance ativos
        console.log("📝 Verificando subscrições ativas para resubscrever...");
        let resubCount = 0;

        for (const [symbol, subscribers] of symbolSubscribers.entries()) {
          if (subscribers.size > 0 && !isForexSymbol(symbol)) {
            const streamName = `${symbol.toLowerCase()}@kline_1m`;
            console.log(
              `� Resubscrevendo ${streamName} (${subscribers.size} subscribers)`,
            );
            socketSend(bSocket, B_SUB_MSG(streamName, 1));
            resubCount++;
          }
        }

        console.log(
          `✅ Processo de reconexão completo, ${resubCount} símbolos resubscritos`,
        );
      }, 1000); // Espera 1s após criar socket para resubscrever

      reconnectBinance(); // Agenda próxima tentativa se necessário
    }, backoffDelay);
  }
}

function createTWSocket() {
  twSocketReady = false;
  twSocket = new WebSocket(WSS_TW_URL);

  twSocket.on("open", () => {
    twSocketReady = true;
    console.log("🔗 WS Twelve Data conectado");
    // Drena mensagens pendentes
    while (pendingQueue.length) socketSend(twSocket, pendingQueue.shift());

    // Configurar heartbeat para manter conexão viva
    this.heartbeatInterval = setInterval(() => {
      if (twSocket.readyState === WebSocket.OPEN) {
        console.log("💓 Enviando heartbeat TwelveData");
        twSocket.send(JSON.stringify({ action: "heartbeat" }));
      }
    }, 30000); // Heartbeat a cada 30 segundos
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

  let heartbeatTimeout;
  let lastPongTime = Date.now();

  bSocket.on("open", () => {
    bSocketReady = true;
    retryBCount = 0; // Reset contador de tentativas ao conectar com sucesso
    console.log("🔗 WS Binance conectado");

    // Drena mensagens pendentes
    while (pendingQueue.length) {
      const msg = pendingQueue.shift();
      console.log("📤 Enviando mensagem pendente:", msg);
      socketSend(bSocket, msg);
    }

    // Sistema de heartbeat mais robusto para Binance
    if (this.binanceHeartbeat) clearInterval(this.binanceHeartbeat);

    this.binanceHeartbeat = setInterval(() => {
      if (bSocket?.readyState === WebSocket.OPEN) {
        // Envia ping para Binance
        bSocket.send(JSON.stringify({ method: "ping" }));
        console.log("💓 Ping enviado para Binance");

        // Verifica se recebemos pong nos últimos 30 segundos
        if (Date.now() - lastPongTime > 30000) {
          console.warn(
            "⚠️ Não recebeu pong da Binance por 30s, reconectando...",
          );
          bSocket.terminate(); // Força fechamento para reconectar
          clearInterval(this.binanceHeartbeat);
        }
      }
    }, 15000); // Ping a cada 15 segundos
  });

  bSocket.on("ping", (data) => {
    console.log(`Recebi o ping da Binance: ${data}`);
    if (bSocket && bSocket?.readyState === WebSocket.OPEN) {
      bSocket.send(
        JSON.stringify({
          method: "PONG",
        }),
      );
    }
  });

  bSocket.on("message", (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (e) {
      console.error("❌ Erro ao parsear mensagem da Binance:", e);
      return;
    }

    // Tratamento de pong
    if (msg.result === null || msg.method === "pong") {
      lastPongTime = Date.now();
      console.log("🏓 Pong recebido da Binance");
      return;
    }

    // Ignora mensagens que não são kline
    if (msg.e !== "kline") {
      if (msg.e) console.log("📨 Mensagem não-kline recebida:", msg.e);
      return;
    }

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
    // console.log(`symbolSubscribers no binance: ${subs}`);

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
  const clientId = socket.id;
  console.log("🔌 Cliente conectado:", clientId);

  // Inicializa conjunto de símbolos do cliente
  clientSymbols.set(clientId, new Set());

  // Envia estado atual do servidor para o cliente
  socket.emit("server:status", {
    twReady: twSocketReady,
    bReady: bSocketReady,
  });

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

  // Cleanup ao desconectar com delay para permitir reconexões
  socket.on("disconnect", (reason) => {
    const clientId = socket.id;
    console.log(`📴 Cliente desconectado (${clientId}):`, reason);

    // Se for uma desconexão por reload/navegação, damos um tempo antes de limpar
    if (reason === "transport close" || reason === "ping timeout") {
      console.log(`⏳ Aguardando possível reconexão para ${clientId}...`);

      setTimeout(() => {
        // Se o cliente não reconectou, aí sim limpamos
        const symbols = clientSymbols.get(clientId) || new Set();
        if (symbols.size > 0) {
          console.log(`🧹 Limpando subscrições de ${clientId} após timeout:`, [
            ...symbols,
          ]);

          for (const s of symbols) {
            const subs = symbolSubscribers.get(s);
            if (!subs) continue;

            subs.delete(clientId);
            if (subs.size === 0) {
              console.log(`❌ Removendo última subscrição de ${s}`);
              unsubscribeSymbol(s, "1m", clientId);
            }
          }
        }
        clientSymbols.delete(clientId);
      }, 5000); // 5 segundos de tolerância para reconexão
    } else {
      // Para outros tipos de desconexão, limpa imediatamente
      const symbols = clientSymbols.get(clientId) || new Set();
      for (const s of symbols) {
        const subs = symbolSubscribers.get(s);
        if (!subs) continue;
        subs.delete(clientId);
        if (subs.size === 0) unsubscribeSymbol(s, "1m", clientId);
      }
      clientSymbols.delete(clientId);
    }
  });
});

app.get("/", (_, res) => res.send("WS Binance e TW OK"));
const PORT = process.env.PORT || 8080;
server.listen(PORT, () => console.log(`Server rodando na porta :${PORT}`));
