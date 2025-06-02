require("dotenv").config();
const { createClient } = require("@supabase/supabase-js");
const fetch = require("node-fetch");

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_ANON_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);

const TRADINGVIEW_API_URL = "https://scanner.tradingview.com/america/scan?label-product=screener-etf";
const LOGO_URL_TEMPLATE = "https://s3-symbol-logo.tradingview.com/{logoid}.svg";

// Payload para ETFs americanos
const payload = {
  columns: [
    "name",
    "description",
    "logoid",
    "update_mode",
    "type",
    "typespecs",
    "close",
    "pricescale",
    "minmov",
    "fractional",
    "minmove2",
    "currency",
    "change",
    "Value.Traded",
    "relative_volume_10d_calc",
    "aum",
    "fundamental_currency_code",
    "nav_total_return.3Y",
    "expense_ratio",
    "asset_class.tr",
    "focus.tr",
    "exchange"
  ],
  ignore_unknown_fields: false,
  options: {
    lang: "en"
  },
  range: [
    0,
    700
  ],
  sort: {
    sortBy: "close",
    sortOrder: "desc"
  },
  symbols: {},
  markets: [
    "america"
  ],
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
                  {
                    expression: {
                      left: "typespecs",
                      operation: "has",
                      right: [
                        "etf"
                      ]
                    }
                  }
                ]
              }
            },
            {
              operation: {
                operator: "and",
                operands: [
                  {
                    expression: {
                      left: "type",
                      operation: "equal",
                      right: "structured"
                    }
                  }
                ]
              }
            }
          ]
        }
      }
    ]
  }
};

class TradingViewETFService {
  constructor() {
    this.intervalId = null;
    this.hasRunInitially = false;
  }

  getBrasiliaTime() {
    const now = new Date();
    return new Date(
      now.toLocaleString("en-US", { timeZone: "America/Sao_Paulo" })
    );
  }

  isBusinessHours() {
    const brasiliaTime = this.getBrasiliaTime();
    const dayOfWeek = brasiliaTime.getDay();
    const hour = brasiliaTime.getHours();
    const minute = brasiliaTime.getMinutes();
    const currentTime = hour + minute / 60;

    // Segunda a sexta (1-5) das 9:30 às 17:30
    const isWeekday = dayOfWeek >= 1 && dayOfWeek <= 5;
    const isBusinessTime = currentTime >= 9.5 && currentTime <= 17.5;

    return isWeekday && isBusinessTime;
  }

  async makeRequest() {
    console.log(`🚀 Fazendo requisição POST para: ${TRADINGVIEW_API_URL}`);

    try {
      const response = await fetch(TRADINGVIEW_API_URL, {
        method: "POST",
        headers: { 
          "Content-Type": "application/json",
          "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        },
        body: JSON.stringify(payload),
      });

      console.log(`📡 Status da resposta: ${response.status} ${response.statusText}`);

      if (!response.ok) {
        const errorText = await response.text();
        console.log(`❌ Erro HTTP ${response.status}:`, errorText.substring(0, 500));
        throw new Error(`HTTP error! status: ${response.status} - ${response.statusText}`);
      }

      const data = await response.json();
      return data;
    } catch (error) {
      console.error("❌ Erro ao fazer requisição:", error.message);
      return null;
    }
  }

  filterTradingViewData(data) {
    if (!data || !data.data) {
      return [];
    }

    return data.data.map((item) => {
      // Estrutura do retorno da TradingView: item.d[index]
      // Baseado nas colunas do payload:
      const name = item.d[0] || null;        // name
      const description = item.d[1] || null; // description
      const logoid = item.d[2] || null;      // logoid
      const close = item.d[6] || null;       // close
      const change = item.d[12] || null;     // change

      // Calcular change percent se temos close e change
      let chg_pct = null;
      if (close && change) {
        const previousClose = close - change;
        if (previousClose !== 0) {
          chg_pct = (change / previousClose) * 100;
        }
      }

      // Gerar logo_url se temos logoid
      const logo_url = logoid ? LOGO_URL_TEMPLATE.replace("{logoid}", logoid) : null;

      return {
        symbol: name,
        name: description || name,
        last: close,
        chg_pct: chg_pct ? parseFloat(chg_pct.toFixed(4)) : null,
        flag: "US",
        logo_url: logo_url
      };
    }).filter(item => item.symbol && item.last); // Filtrar apenas itens com symbol e price válidos
  }

  async saveToSupabase(etfData) {
    try {
      console.log(`💾 Salvando ${etfData.length} registros no Supabase...`);

      // Inserir ou atualizar dados em lotes
      const batchSize = 100;
      let totalUpserted = 0;
      let errorCount = 0;

      for (let i = 0; i < etfData.length; i += batchSize) {
        const batch = etfData.slice(i, i + batchSize);
        const batchNumber = Math.floor(i / batchSize) + 1;
        const totalBatches = Math.ceil(etfData.length / batchSize);
        
        console.log(`📦 Processando lote ${batchNumber}/${totalBatches} com ${batch.length} registros...`);

        try {
          // upsert pelo campo symbol
          const { data, error: upsertError } = await supabase
            .from("etf_data")
            .upsert(batch, { onConflict: "symbol" })
            .select();

          if (upsertError) {
            console.error(`❌ Erro ao inserir/atualizar lote ${batchNumber}:`, upsertError.message);
            errorCount += batch.length;
          } else {
            const upsertedCount = data ? data.length : 0;
            totalUpserted += upsertedCount;
            console.log(`✅ Lote ${batchNumber}: ${upsertedCount} registros inseridos/atualizados`);
          }
        } catch (batchError) {
          console.error(`❌ Erro de execução no lote ${batchNumber}:`, batchError.message);
          errorCount += batch.length;
        }

        // Pequena pausa entre lotes
        await new Promise((resolve) => setTimeout(resolve, 200));
      }

      console.log(`📊 Resumo: ${totalUpserted} registros inseridos/atualizados, ${errorCount} erros`);
      return totalUpserted > 0;
    } catch (error) {
      console.error("❌ Erro geral ao salvar no Supabase:", error.message);
      return false;
    }
  }

  async fetchTradingViewETFData() {
    try {
      const brasiliaTime = this.getBrasiliaTime();

      // Verificar se é primeira execução ou horário comercial
      if (!this.hasRunInitially || this.isBusinessHours()) {
        if (!this.hasRunInitially) {
          console.log(`🎬 PRIMEIRA EXECUÇÃO - ${brasiliaTime.toLocaleString("pt-BR", {
            timeZone: "America/Sao_Paulo",
          })}`);
          this.hasRunInitially = true;
        } else {
          console.log(`⏰ EXECUÇÃO PROGRAMADA - ${brasiliaTime.toLocaleString("pt-BR", {
            timeZone: "America/Sao_Paulo",
          })}`);
        }

        console.log("🚀 Iniciando coleta de dados de ETFs americanos da TradingView...");

        const response = await this.makeRequest();

        if (!response) {
          console.log("❌ Falha ao obter dados da API TradingView");
          return;
        }

        console.log(`📊 ${response.data ? response.data.length : 0} registros recebidos da API`);

        // Filtrar e preparar dados
        const filteredData = this.filterTradingViewData(response);
        console.log(`📊 ${filteredData.length} registros válidos após filtro`);

        if (filteredData.length === 0) {
          console.log("❌ Nenhum registro válido para salvar");
          return;
        }

        // Debug: mostrar exemplo dos dados
        if (filteredData.length > 0) {
          console.log("🔍 Exemplo de registro:", JSON.stringify(filteredData[0], null, 2));
        }

        // Salvar no Supabase
        const supabaseSuccess = await this.saveToSupabase(filteredData);

        if (supabaseSuccess) {
          console.log(`✨ Coleta concluída às ${brasiliaTime.toLocaleString("pt-BR", {
            timeZone: "America/Sao_Paulo",
          })}\n`);
        } else {
          console.log("⚠️ Coleta concluída mas houve erros ao salvar no Supabase\n");
        }
      } else {
        console.log(`⏸️  FORA DO HORÁRIO COMERCIAL - ${brasiliaTime.toLocaleString("pt-BR", {
          timeZone: "America/Sao_Paulo"
        })} - Aguardando próxima execução...\n`);
      }
    } catch (error) {
      console.error("❌ Erro durante a coleta:", error.message);
    }
  }

  startAutomation() {
    const brasiliaTime = this.getBrasiliaTime();
    console.log("🕒 Iniciando automação TradingView ETFs - execução a cada 15 minutos");
    console.log(`🌎 Horário atual em Brasília: ${brasiliaTime.toLocaleString("pt-BR", {
      timeZone: "America/Sao_Paulo",
    })}`);
    console.log("📅 Funcionamento: Segunda a Sexta, 9:30h às 17:30h (horário de Brasília)");

    // Executar imediatamente
    this.fetchTradingViewETFData();

    // Agendar execuções a cada 15 minutos
    this.intervalId = setInterval(() => {
      this.fetchTradingViewETFData();
    }, 15 * 60 * 1000);

    console.log("⏰ Automação ativa. Pressione Ctrl+C para parar.");
  }

  stopAutomation() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
      console.log("🛑 Automação parada");
    }
  }

  async getStats() {
    try {
      const { count, error } = await supabase
        .from("etf_data")
        .select("*", { count: "exact", head: true })
        .eq("flag", "US");

      if (error) {
        console.error("❌ Erro ao buscar estatísticas:", error.message);
        return;
      }

      const { data: latestRecord } = await supabase
        .from("etf_data")
        .select("created_at, symbol, name, last, chg_pct")
        .eq("flag", "US")
        .order("created_at", { ascending: false })
        .limit(1);

      const brasiliaTime = this.getBrasiliaTime();

      console.log("\n📊 ESTATÍSTICAS DO TRADINGVIEW ETF:");
      console.log(`📈 Total de registros (flag=US): ${count}`);
      console.log(`⏰ Horário comercial ativo: ${this.isBusinessHours() ? "✅ SIM" : "❌ NÃO"}`);

      if (latestRecord && latestRecord.length > 0) {
        const latest = latestRecord[0];
        console.log(`📅 Último registro: ${new Date(latest.created_at).toLocaleString("pt-BR")}`);
        console.log(`💰 Exemplo: ${latest.symbol} (${latest.name}) - Preço: $${latest.last}, Variação: ${latest.chg_pct}%`);
      }
    } catch (error) {
      console.error("❌ Erro ao buscar estatísticas:", error.message);
    }
  }
}

// Função para manusear encerramento gracioso
function setupGracefulShutdown(service) {
  process.on("SIGINT", () => {
    console.log("\n🛑 Recebido sinal de interrupção...");
    service.stopAutomation();
    process.exit(0);
  });

  process.on("SIGTERM", () => {
    console.log("\n🛑 Recebido sinal de terminação...");
    service.stopAutomation();
    process.exit(0);
  });
}

// Função principal
async function main() {
  const service = new TradingViewETFService();
  setupGracefulShutdown(service);

  // Verificar argumentos da linha de comando
  const args = process.argv.slice(2);

  if (args.includes("--stats")) {
    await service.getStats();
    return;
  }

  if (args.includes("--once")) {
    console.log("🎯 Executando apenas uma vez...");
    await service.fetchTradingViewETFData();
    return;
  }

  // Iniciar automação por padrão
  service.startAutomation();
}

// Executar apenas se este arquivo for executado diretamente
if (require.main === module) {
  main().catch((error) => {
    console.error("❌ Erro fatal:", error);
    process.exit(1);
  });
}

module.exports = TradingViewETFService;