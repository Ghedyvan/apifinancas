require("dotenv").config();
const { createClient } = require("@supabase/supabase-js");
const fetch = require("node-fetch");

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_ANON_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);

const TRADINGVIEW_API_URL = "https://scanner.tradingview.com/america/scan?label-product=screener-stock";
const LOGO_URL_TEMPLATE = "https://s3-symbol-logo.tradingview.com/{logoid}.svg";

class TradingViewStockService {
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

  createPayload(startRange = 0, endRange = 300) {
    return {
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
        "volume",
        "relative_volume_10d_calc",
        "market_cap_basic",
        "fundamental_currency_code",
        "price_earnings_ttm",
        "earnings_per_share_diluted_ttm",
        "earnings_per_share_diluted_yoy_growth_ttm",
        "dividends_yield_current",
        "sector.tr",
        "market",
        "sector",
        "recommendation_mark",
        "exchange"
      ],
      filter: [
        {
          left: "is_blacklisted",
          operation: "equal",
          right: false
        },
        {
          left: "is_primary",
          operation: "equal",
          right: true
        }
      ],
      ignore_unknown_fields: false,
      options: {
        lang: "en"
      },
      range: [startRange, endRange],
      sort: {
        sortBy: "market_cap_basic",
        sortOrder: "desc"
      },
      symbols: {
        symbolset: [
          "SYML:SP;SPX",
          "SYML:NASDAQ;NDX"
        ]
      },
      markets: ["america"],
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
                          left: "type",
                          operation: "equal",
                          right: "stock"
                        }
                      },
                      {
                        expression: {
                          left: "typespecs",
                          operation: "has",
                          right: ["common"]
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
                          right: "stock"
                        }
                      },
                      {
                        expression: {
                          left: "typespecs",
                          operation: "has",
                          right: ["preferred"]
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
                          right: "dr"
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
                          right: "fund"
                        }
                      },
                      {
                        expression: {
                          left: "typespecs",
                          operation: "has_none_of",
                          right: ["etf"]
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
  }

  async makeRequest(startRange, endRange, retryAttempt = 0) {
    const maxRetries = 3;
    const payload = this.createPayload(startRange, endRange);
    const retryText = retryAttempt > 0 ? ` (tentativa ${retryAttempt + 1}/${maxRetries + 1})` : "";
    
    console.log(`🚀 Fazendo requisição POST para range ${startRange}-${endRange}${retryText}`);

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
      console.error(`❌ Erro ao fazer requisição range ${startRange}-${endRange}${retryText}:`, error.message);
      
      // Tentar novamente se ainda não excedeu o limite de tentativas
      if (retryAttempt < maxRetries) {
        const waitTime = (retryAttempt + 1) * 3000; // Aumento progressivo: 3s, 6s, 9s
        console.log(`⏳ Aguardando ${waitTime/1000}s antes de tentar novamente...`);
        await new Promise(resolve => setTimeout(resolve, waitTime));
        return this.makeRequest(startRange, endRange, retryAttempt + 1);
      }
      
      console.error(`💥 Falha definitiva no range ${startRange}-${endRange} após ${maxRetries + 1} tentativas`);
      return null;
    }
  }

  async fetchAllData() {
    console.log("🔍 Buscando todos os dados em lotes de 300 registros...");
    console.log("📋 Total esperado: ~511 registros");
    
    let allData = [];
    const batchSize = 300;
    const maxRange = 511; // Atualizado para 511
    let currentStart = 0;
    
    while (currentStart < maxRange) {
      const currentEnd = Math.min(currentStart + batchSize, maxRange);
      const batchNumber = Math.floor(currentStart / batchSize) + 1;
      const totalBatches = Math.ceil(maxRange / batchSize);
      
      console.log(`📄 Processando lote ${batchNumber}/${totalBatches} (range ${currentStart}-${currentEnd})...`);
      
      // Tentar até 3 vezes para cada lote
      let response = null;
      let batchRetries = 0;
      const maxBatchRetries = 3;
      
      while (batchRetries < maxBatchRetries && !response) {
        response = await this.makeRequest(currentStart, currentEnd);
        
        if (!response || !response.data) {
          batchRetries++;
          if (batchRetries < maxBatchRetries) {
            const retryWaitTime = batchRetries * 2000; // 2s, 4s, 6s
            console.log(`❌ Falha no lote ${batchNumber} - tentativa ${batchRetries}/${maxBatchRetries}`);
            console.log(`⏳ Aguardando ${retryWaitTime/1000}s antes de tentar novamente o lote...`);
            await new Promise(resolve => setTimeout(resolve, retryWaitTime));
          } else {
            console.log(`💥 Falha definitiva no lote ${batchNumber} após ${maxBatchRetries} tentativas - continuando...`);
          }
        }
      }
      
      if (!response || !response.data) {
        console.log(`❌ Pulando lote ${batchNumber} - sem dados válidos`);
        currentStart += batchSize;
        continue;
      }

      const batchData = response.data;
      console.log(`📊 Lote ${batchNumber}: ${batchData.length} registros recebidos`);

      if (batchData.length > 0) {
        allData.push(...batchData);
      }

      const progress = ((allData.length / maxRange) * 100).toFixed(1);
      console.log(`📈 Progresso: ${allData.length}/${maxRange} registros (${progress}%)`);

      currentStart += batchSize;

      // Pausa de 3 segundos entre lotes
      if (currentStart < maxRange) {
        console.log("⏳ Aguardando 3 segundos antes do próximo lote...");
        await new Promise(resolve => setTimeout(resolve, 3000));
      }
    }

    console.log(`✅ Coleta finalizada: ${allData.length} registros coletados`);
    return allData;
  }

  filterTradingViewData(data) {
    if (!data || data.length === 0) {
      return [];
    }

    return data.map((item) => {
      // Estrutura do retorno da TradingView: item.d[index]
      // Baseado nas colunas do payload atualizado:
      const name = item.d[0] || null;        // name (índice 0)
      const description = item.d[1] || null; // description (índice 1)
      const logoid = item.d[2] || null;      // logoid (índice 2)
      const close = item.d[6] || null;       // close (índice 6)
      const change = item.d[12] || null;     // change (índice 12)

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

  async saveToSupabase(stockData) {
    try {
      console.log(`💾 Salvando ${stockData.length} registros no Supabase...`);

      // Inserir ou atualizar dados em lotes
      const batchSize = 100;
      let totalUpserted = 0;
      let errorCount = 0;

      for (let i = 0; i < stockData.length; i += batchSize) {
        const batch = stockData.slice(i, i + batchSize);
        const batchNumber = Math.floor(i / batchSize) + 1;
        const totalBatches = Math.ceil(stockData.length / batchSize);
        
        console.log(`📦 Processando lote ${batchNumber}/${totalBatches} com ${batch.length} registros...`);

        try {
          // upsert pelo campo symbol na tabela tradingview_data
          const { data, error: upsertError } = await supabase
            .from("tradingview_data")
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

  async fetchTradingViewStockData() {
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

        console.log("🚀 Iniciando coleta de dados de ações americanas (SP500/NASDAQ) da TradingView...");
        console.log("⚠️  ATENÇÃO: Coleta de ~511 ativos deve levar 2-3 minutos (com retry automático)");

        // Buscar todos os dados em lotes
        const allData = await this.fetchAllData();

        if (allData.length === 0) {
          console.log("❌ Nenhum dado válido obtido da API");
          return;
        }

        // Filtrar e preparar dados
        const filteredData = this.filterTradingViewData(allData);
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
    console.log("🕒 Iniciando automação TradingView Stocks - execução a cada 15 minutos");
    console.log(`🌎 Horário atual em Brasília: ${brasiliaTime.toLocaleString("pt-BR", {
      timeZone: "America/Sao_Paulo",
    })}`);
    console.log("📅 Funcionamento: Segunda a Sexta, 9:30h às 17:30h (horário de Brasília)");
    console.log("⚠️  ATENÇÃO: Coleta de ~511 ativos deve levar 2-3 minutos (com retry automático)");

    // Executar imediatamente
    this.fetchTradingViewStockData();

    // Agendar execuções a cada 15 minutos (reduzido devido ao menor volume)
    this.intervalId = setInterval(() => {
      this.fetchTradingViewStockData();
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
        .from("tradingview_data")
        .select("*", { count: "exact", head: true })
        .eq("flag", "US");

      if (error) {
        console.error("❌ Erro ao buscar estatísticas:", error.message);
        return;
      }

      const { data: latestRecord } = await supabase
        .from("tradingview_data")
        .select("created_at, symbol, name, last, chg_pct")
        .eq("flag", "US")
        .order("created_at", { ascending: false })
        .limit(1);

      const brasiliaTime = this.getBrasiliaTime();

      console.log("\n📊 ESTATÍSTICAS DO TRADINGVIEW STOCKS:");
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
  const service = new TradingViewStockService();
  setupGracefulShutdown(service);

  // Verificar argumentos da linha de comando
  const args = process.argv.slice(2);

  if (args.includes("--stats")) {
    await service.getStats();
    return;
  }

  if (args.includes("--once")) {
    console.log("🎯 Executando apenas uma vez...");
    await service.fetchTradingViewStockData();
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

module.exports = TradingViewStockService;