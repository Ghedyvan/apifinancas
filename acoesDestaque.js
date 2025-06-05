require("dotenv").config();
const { createClient } = require("@supabase/supabase-js");
const fetch = require("node-fetch");

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_ANON_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);

const TRADINGVIEW_API_URL = "https://scanner.tradingview.com/brazil/scan?label-product=screener-stock";
const LOGO_URL_TEMPLATE = "https://s3-symbol-logo.tradingview.com/{logoid}.svg";

class TradingViewB3DestaqueService {
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

    // Segunda a sexta (1-5) das 10:00 às 17:00 (horário B3)
    const isWeekday = dayOfWeek >= 1 && dayOfWeek <= 5;
    const isBusinessTime = currentTime >= 10.0 && currentTime <= 17.0;

    return isWeekday && isBusinessTime;
  }

  createPayload(startRange = 0, endRange = 100) {
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
          "SYML:BMFBOVESPA;IBOV"
        ]
      },
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
    
    let allData = [];
    const batchSize = 25; // Lotes menores para ações em destaque
    const maxRange = 75; // Range máximo do payload
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
      
      // Debug: mostrar total_count da resposta se disponível
      if (response.totalCount !== undefined) {
        console.log(`📊 Total count da API: ${response.totalCount}`);
      }
      
      if (batchData.length > 0) {
        allData.push(...batchData);
      }

      // Se recebeu menos que o esperado, provavelmente chegou ao fim
      if (batchData.length < batchSize) {
        console.log(`📄 Última página detectada (menos de ${batchSize} registros)`);
        break;
      }

      const progress = ((allData.length / maxRange) * 100).toFixed(1);
      console.log(`📈 Progresso: ${allData.length} registros coletados`);

      currentStart += batchSize;

      // Pausa de 2 segundos entre lotes
      if (currentStart < maxRange) {
        console.log("⏳ Aguardando 2 segundos antes do próximo lote...");
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }

    console.log(`✅ Coleta finalizada: ${allData.length} registros coletados`);
    return allData;
  }

  filterTradingViewData(data) {
    if (!data || data.length === 0) {
      return [];
    }

    let totalItems = data.length;
    let validItems = 0;
    let discardedItems = 0;
    let discardedExamples = [];

    console.log(`📊 Total de registros recebidos: ${totalItems}`);

    const processedData = data.map((item) => {
      const name = item.d[0] || null;        // name (índice 0)
      const description = item.d[1] || null; // description (índice 1)
      const logoid = item.d[2] || null;      // logoid (índice 2)
      const type = item.d[4] || null;        // type (índice 4)
      const typespecs = item.d[5] || null;   // typespecs (índice 5)
      const close = item.d[6] || null;       // close (índice 6)
      const change = item.d[12] || null;     // change (índice 12)
      const market_cap = item.d[15] || null; // market_cap_basic (índice 15)

      // Calcular change percent (já vem pronto do TradingView)
      let chg_pct = change;

      // Gerar logo_url se temos logoid
      const logo_url = logoid ? LOGO_URL_TEMPLATE.replace("{logoid}", logoid) : null;

      const processedItem = {
        symbol: name,
        name: description || name,
        last: close,
        chg_pct: chg_pct ? parseFloat(chg_pct.toFixed(2)) : null,
        flag: "BR",
        logo_url: logo_url,
        market_cap: market_cap, // Adicionar market cap para ações em destaque
        type: type,
        typespecs: typespecs
      };

      // Verificar se será descartado (apenas se name é NULL)
      const willBeDiscarded = !name;
      
      if (willBeDiscarded) {
        discardedItems++;
        
        // Coletar exemplos dos descartados
        if (discardedExamples.length < 5) {
          discardedExamples.push({
            description: description,
            type: type,
            typespecs: typespecs,
            close: close,
            market_cap: market_cap,
            reason: "name é NULL"
          });
        }
      } else {
        validItems++;
      }

      return processedItem;
    });

    console.log(`📊 Análise de filtros:`);
    console.log(`   ✅ Registros válidos (com name): ${validItems}`);
    console.log(`   ❌ Registros descartados (name NULL): ${discardedItems}`);
    console.log(`   📋 Total processado: ${totalItems}`);
    
    if (discardedExamples.length > 0) {
      console.log(`📋 Exemplos de registros descartados (name NULL):`);
      discardedExamples.forEach((item, index) => {
        console.log(`   ${index + 1}. Description: "${item.description}", Preço: ${item.close}, Tipo: ${item.type}`);
        console.log(`      Typespecs: ${JSON.stringify(item.typespecs)}, Market Cap: ${item.market_cap}`);
      });
    }

    // Filtrar apenas registros onde name não é NULL
    return processedData.filter(item => item.symbol);
  }

  async saveToSupabase(stockData) {
    try {
      console.log(`💾 Salvando ${stockData.length} registros no Supabase...`);

      // Inserir ou atualizar dados em lotes
      const batchSize = 50; // Lotes menores para ações em destaque
      let totalUpserted = 0;
      let errorCount = 0;

      for (let i = 0; i < stockData.length; i += batchSize) {
        const batch = stockData.slice(i, i + batchSize);
        const batchNumber = Math.floor(i / batchSize) + 1;
        const totalBatches = Math.ceil(stockData.length / batchSize);
        
        console.log(`📦 Processando lote ${batchNumber}/${totalBatches} com ${batch.length} registros...`);

        try {
          // upsert pelo campo symbol na tabela b3_destaque
          const { data, error: upsertError } = await supabase
            .from("b3_destaque")
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

  async fetchTradingViewB3DestaqueData() {
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

        console.log("🚀 Iniciando coleta de ações em destaque (Ibovespa) da TradingView...");
        console.log("⚠️  ATENÇÃO: Coleta de ações em destaque deve levar 1-2 minutos (com retry automático)");

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
    // Executar imediatamente
    this.fetchTradingViewB3DestaqueData();

    // Agendar execuções a cada 40 minutos (mais frequente para ações em destaque)
    this.intervalId = setInterval(() => {
      this.fetchTradingViewB3DestaqueData();
    }, 40 * 60 * 1000);

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
        .from("b3_destaque")
        .select("*", { count: "exact", head: true })
        .eq("flag", "BR");

      if (error) {
        console.error("❌ Erro ao buscar estatísticas:", error.message);
        return;
      }

      const { data: latestRecord } = await supabase
        .from("b3_destaque")
        .select("created_at, symbol, name, last, chg_pct, market_cap")
        .eq("flag", "BR")
        .order("created_at", { ascending: false })
        .limit(1);

      const brasiliaTime = this.getBrasiliaTime();

      console.log("\n📊 ESTATÍSTICAS DO TRADINGVIEW B3 DESTAQUE:");
      console.log(`📈 Total de registros (flag=BR): ${count}`);
      console.log(`⏰ Horário comercial ativo: ${this.isBusinessHours() ? "✅ SIM" : "❌ NÃO"}`);

      if (latestRecord && latestRecord.length > 0) {
        const latest = latestRecord[0];
        const marketCapFormatted = latest.market_cap ? `Market Cap: R$ ${(latest.market_cap / 1000000000).toFixed(2)}B` : 'Market Cap: N/A';
        console.log(`📅 Último registro: ${new Date(latest.created_at).toLocaleString("pt-BR")}`);
        console.log(`💰 Exemplo: ${latest.symbol} (${latest.name}) - Preço: R$${latest.last}, Variação: ${latest.chg_pct}%, ${marketCapFormatted}`);
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
  const service = new TradingViewB3DestaqueService();
  setupGracefulShutdown(service);

  // Verificar argumentos da linha de comando
  const args = process.argv.slice(2);

  if (args.includes("--stats")) {
    await service.getStats();
    return;
  }

  if (args.includes("--once")) {
    console.log("🎯 Executando apenas uma vez...");
    await service.fetchTradingViewB3DestaqueData();
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

module.exports = TradingViewB3DestaqueService;