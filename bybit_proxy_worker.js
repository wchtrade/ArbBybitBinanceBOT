/**
 * bybit-proxy — прозрачный реверс-прокси к api.bybit.com для ArbBybitBinanceBOT.
 *
 * Зачем: Bybit блокирует запросы с облачных дата-центровых IP (Railway/AWS/GCP)
 * через CloudFront ("configured to block access from your country"). Этот Worker
 * пересылает запрос от твоего бота на реальный api.bybit.com со стороны сети
 * Cloudflare edge — часто не подпадает под ту же блокировку.
 *
 * Как задеплоить (без CLI, через дашборд):
 *   1. cloudflare.com → зарегистрироваться (бесплатно) → Workers & Pages
 *   2. Create → Create Worker → дать имя, например "bybit-proxy"
 *   3. Edit code → стереть заготовку → вставить содержимое этого файла целиком
 *   4. Deploy
 *   5. Скопировать выданный адрес вида https://bybit-proxy.<твой-сабдомен>.workers.dev
 *   6. В Railway → Variables → BYBIT_PROXY_BASE = этот адрес (без слэша в конце)
 *
 * Как это работает: бот дальше стучится не в api.bybit.com напрямую, а в
 * https://bybit-proxy.xxx.workers.dev/v5/market/tickers?... — Worker сам
 * пересылает путь+параметры+заголовки+тело на настоящий api.bybit.com и
 * возвращает ответ как есть (включая подписанные приватные запросы для REAL-режима).
 */

const UPSTREAM = "https://api.bybit.com";

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const targetUrl = UPSTREAM + url.pathname + url.search;

    // Копируем заголовки запроса, убираем те, что относятся к самому Cloudflare/Worker
    const headers = new Headers(request.headers);
    headers.delete("host");
    headers.delete("cf-connecting-ip");
    headers.delete("cf-ray");
    headers.delete("cf-visitor");
    headers.delete("cf-ipcountry");
    headers.delete("x-forwarded-proto");
    headers.delete("x-forwarded-for");

    const init = {
      method: request.method,
      headers,
      body: (request.method === "GET" || request.method === "HEAD")
        ? undefined
        : await request.arrayBuffer(),
    };

    try {
      const upstreamResp = await fetch(targetUrl, init);
      const respHeaders = new Headers(upstreamResp.headers);
      respHeaders.set("Access-Control-Allow-Origin", "*");
      return new Response(upstreamResp.body, {
        status: upstreamResp.status,
        statusText: upstreamResp.statusText,
        headers: respHeaders,
      });
    } catch (e) {
      return new Response(
        JSON.stringify({ proxy_error: String(e) }),
        { status: 502, headers: { "Content-Type": "application/json" } }
      );
    }
  },
};
