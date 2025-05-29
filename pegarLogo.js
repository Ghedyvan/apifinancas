require("dotenv").config();
const { createClient } = require("@supabase/supabase-js");
const fetch = require("node-fetch");

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_ANON_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);

const TRADINGVIEW_API_URL = "https://scanner.tradingview.com/brazil/scan?label-product=screener-stock";
const LOGO_URL_TEMPLATE = "https://s3-symbol-logo.tradingview.com/{logoid}.svg";

// Payload fornecido (ajuste o range se necessário)
const payload = {
  columns: [
    "name", "description", "logoid", "update_mode", "type", "typespecs", "close", "pricescale", "minmov",
    "fractional", "minmove2", "currency", "change", "volume", "relative_volume_10d_calc", "market_cap_basic",
    "fundamental_currency_code", "price_earnings_ttm", "earnings_per_share_diluted_ttm",
    "earnings_per_share_diluted_yoy_growth_ttm", "dividends_yield_current", "sector.tr", "market", "sector",
    "recommendation_mark", "exchange"
  ],
  filter: [
    { left: "is_primary", operation: "equal", right: true }
  ],
  ignore_unknown_fields: false,
  options: { lang: "en" },
  range: [700, 800],
  sort: { sortBy: "market_cap_basic", sortOrder: "desc" },
  symbols: {},
  markets: ["brazil"],
  filter2: {
    operator: "and",
    operands: [
      {
        operation: {
          operator: "or",
          operands: [
            {
              operation: {
                operator: "and",
                operands: [
                  { expression: { left: "type", operation: "equal", right: "stock" } },
                  { expression: { left: "typespecs", operation: "has", right: ["common"] } }
                ]
              }
            },
            {
              operation: {
                operator: "and",
                operands: [
                  { expression: { left: "type", operation: "equal", right: "stock" } },
                  { expression: { left: "typespecs", operation: "has", right: ["preferred"] } }
                ]
              }
            },
            {
              operation: {
                operator: "and",
                operands: [
                  { expression: { left: "type", operation: "equal", right: "dr" } }
                ]
              }
            },
            {
              operation: {
                operator: "and",
                operands: [
                  { expression: { left: "type", operation: "equal", right: "fund" } },
                  { expression: { left: "typespecs", operation: "has_none_of", right: ["etf"] } }
                ]
              }
            }
          ]
        }
      }
    ]
  }
};

async function fetchAtivosSemLogo() {
  const { data, error } = await supabase
    .from("investing_data")
    .select("id, symbol, logo_url")
    .or("logo_url.is.null,logo_url.eq.''");

  if (error) {
    console.error("Erro ao buscar ativos:", error.message);
    return [];
  }
  return data;
}

async function fetchLogosTradingView() {
  const response = await fetch(TRADINGVIEW_API_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    console.error("Erro na API TradingView:", response.statusText);
    return [];
  }
  const data = await response.json();
  return data.data || [];
}

async function atualizarLogos() {
  const ativos = await fetchAtivosSemLogo();
  if (!ativos.length) {
    console.log("Nenhum ativo sem logo_url encontrado.");
    return;
  }

  const logos = await fetchLogosTradingView();

  // Cria um mapa para lookup rápido: prefixo (4 letras) => logoid
  const logoMap = {};
  for (const item of logos) {
    const name = item.d[0];
    const logoid = item.d[2];
    if (name && logoid) {
      logoMap[name.substring(0, 4)] = logoid;
    }
  }

  let atualizados = 0;
  for (const ativo of ativos) {
    const prefix = ativo.symbol ? ativo.symbol.substring(0, 4) : "";
    const logoid = logoMap[prefix];
    if (logoid) {
      const logo_url = LOGO_URL_TEMPLATE.replace("{logoid}", logoid);
      const { error } = await supabase
        .from("investing_data")
        .update({ logo_url })
        .eq("id", ativo.id);
      if (!error) {
        atualizados++;
        console.log(`Atualizado ${ativo.symbol} com ${logo_url}`);
      } else {
        console.error(`Erro ao atualizar ${ativo.symbol}:`, error.message);
      }
    }
  }
  console.log(`Total de logos atualizados: ${atualizados}`);
}

if (require.main === module) {
  atualizarLogos().catch((err) => {
    console.error("Erro geral:", err);
    process.exit(1);
  });
}

module.exports = { atualizarLogos };