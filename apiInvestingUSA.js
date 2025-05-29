require("dotenv").config();
const { createClient } = require("@supabase/supabase-js");

// Configurações do Supabase
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_ANON_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);

// URL da API Investing
const API_URL = "https://api.investing.com/api/financialdata/assets/equitiesByCountry/default";

class FinnhubDataService {
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

  async makeRequest(page = 0) {
    const queryParams = {
      "fields-list": "id,name,symbol,isCFD,high,low,last,lastPairDecimal,change,changePercent,volume,time,isOpen,url,flag,countryNameTranslated,exchangeId,performanceDay,performanceWeek,performanceMonth,performanceYtd,performanceYear,performance3Year,technicalHour,technicalDay,technicalWeek,technicalMonth,avgVolume,fundamentalMarketCap,fundamentalRevenue,fundamentalRatio,fundamentalBeta,pairType",
      "country-id": 5, // País ID 5 para USA
      "filter-domain": "",
      page: page,
      "page-size": 500, // Mantendo 500 como você sugeriu
      limit: 0,
      "include-additional-indices": false,
      "include-major-indices": false,
      "include-other-indices": false,
      "include-primary-sectors": false,
      "include-market-overview": false,
    };

    const url = new URL(API_URL);
    Object.keys(queryParams).forEach((key) => {
      if (queryParams[key] !== undefined && queryParams[key] !== null) {
        url.searchParams.append(key, queryParams[key]);
      }
    });

    console.log(`🚀 Fazendo requisição GET para página ${page}`);

    try {
      const response = await fetch(url.toString(), {
        method: "GET",
        headers: {
          Accept: "*/*",
          "User-Agent": "Thunder Client (https://www.thunderclient.com)",
          Cookie: "__cflb=02DiuEaBtsFfH7bEbN5e6S2b8T1ZBoeD4McSCKs9QXk2Y; __cf_bm=vh9Hh8c0WRkL4VweyMhf05i84C3YBHe5EdnicJvuFek-1748165272-1.0.1.1-wSGSIYYObeLYEwY1gpHQkVoqUV0ixr1",
        },
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
      console.error(`❌ Erro ao fazer requisição página ${page}:`, error.message);
      return null;
    }
  }

  async fetchAllPages() {
    console.log("🔍 Buscando todas as páginas de dados (500 registros por página)...");
    
    let allData = [];
    let currentPage = 1;
    let hasMoreData = true;
    const totalExpected = 10826; // Total conhecido de ativos
    
    while (hasMoreData) {
      console.log(`📄 Processando página ${currentPage + 1}...`);
      
      const response = await this.makeRequest(currentPage);
      
      if (!response || !response.data) {
        console.log(`❌ Falha ao obter dados da página ${currentPage}`);
        break;
      }

      const pageData = response.data;
      console.log(`📊 Página ${currentPage + 1}: ${pageData.length} registros recebidos`);

      if (pageData.length === 0) {
        console.log("📄 Nenhum dado na página atual, finalizando...");
        hasMoreData = false;
        break;
      }

      allData.push(...pageData);
      currentPage++;

      const progress = ((allData.length / totalExpected) * 100).toFixed(1);
      console.log(`📈 Progresso: ${allData.length}/${totalExpected} registros (${progress}%)`);

      // Pausa entre requisições para não sobrecarregar a API
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Se recebeu menos que 500, provavelmente é a última página
      if (pageData.length < 500) {
        console.log("📄 Última página detectada (menos de 500 registros)");
        hasMoreData = false;
      }

      // Limite de segurança para evitar loops infinitos
      if (currentPage > 25) { // 25 páginas * 500 = 12.500 registros (margem de segurança)
        console.log("⚠️ Limite de páginas atingido (25), parando...");
        break;
      }
    }

    console.log(`✅ Coleta finalizada: ${allData.length} registros de ${currentPage} páginas`);
    return allData;
  }

  filterFinnhubData(data) {
    return data.map((item) => ({
      symbol: item.Symbol || null,
      name: item.Name || null,
      last: item.Last || null,
      chg_pct: item.ChgPct || null,
      country_name_translated: item.CountryNameTranslated || "USA",
      flag: item.Flag || "🇺🇸",
      logo_url: null // A API do Investing não fornece logo, mantendo null
    })).filter(item => item.symbol); // Filtrar apenas itens com symbol válido
  }

  async saveToSupabase(finnhubData) {
    try {
      console.log(`💾 Salvando ${finnhubData.length} registros no Supabase...`);

      // Inserir ou atualizar dados em lotes
      const batchSize = 500; // Reduzido para corresponder ao tamanho da página
      let totalUpserted = 0;
      let errorCount = 0;

      for (let i = 0; i < finnhubData.length; i += batchSize) {
        const batch = finnhubData.slice(i, i + batchSize);
        const batchNumber = Math.floor(i / batchSize) + 1;
        const totalBatches = Math.ceil(finnhubData.length / batchSize);
        
        console.log(`📦 Processando lote ${batchNumber}/${totalBatches} com ${batch.length} registros...`);

        try {
          // upsert pelo campo symbol
          const { data, error: upsertError } = await supabase
            .from("finhub_data")
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

  async fetchFinnhubData() {
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

        console.log("🚀 Iniciando coleta de dados da API Investing (USA)...");
        console.log("📋 Estimativa: ~10.826 ativos em páginas de 500 registros");

        // Buscar todas as páginas
        const allData = await this.fetchAllPages();

        if (allData.length === 0) {
          console.log("❌ Nenhum dado válido obtido da API");
          return;
        }

        // Filtrar e preparar dados
        const filteredData = this.filterFinnhubData(allData);
        console.log(`📊 ${filteredData.length} registros válidos após filtro`);

        if (filteredData.length === 0) {
          console.log("❌ Nenhum registro válido para salvar");
          return;
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
    console.log("🕒 Iniciando automação - execução a cada 30 minutos");
    console.log(`🌎 Horário atual em Brasília: ${brasiliaTime.toLocaleString("pt-BR", {
      timeZone: "America/Sao_Paulo",
    })}`);
    console.log("📅 Funcionamento: Segunda a Sexta, 9:30h às 17:30h (horário de Brasília)");
    console.log("⚠️  ATENÇÃO: Coleta completa de ~10.826 ativos pode levar 5-10 minutos");

    // Executar imediatamente
    this.fetchFinnhubData();

    // Agendar execuções a cada 30 minutos (aumentado devido ao volume)
    this.intervalId = setInterval(() => {
      this.fetchFinnhubData();
    }, 30 * 60 * 1000);

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
        .from("finhub_data")
        .select("*", { count: "exact", head: true });

      if (error) {
        console.error("❌ Erro ao buscar estatísticas:", error.message);
        return;
      }

      const { data: latestRecord } = await supabase
        .from("finhub_data")
        .select("created_at, symbol, name, last, chg_pct")
        .order("created_at", { ascending: false })
        .limit(1);

      const brasiliaTime = this.getBrasiliaTime();

      console.log("\n📊 ESTATÍSTICAS DO FINNHUB API:");
      console.log(`📈 Total de registros: ${count}/~10.826`);
      console.log(`⏰ Horário comercial ativo: ${this.isBusinessHours() ? "✅ SIM" : "❌ NÃO"}`);

      if (latestRecord && latestRecord.length > 0) {
        const latest = latestRecord[0];
        console.log(`📅 Último registro: ${new Date(latest.created_at).toLocaleString("pt-BR")}`);
        console.log(`💰 Exemplo: ${latest.symbol} (${latest.name}) - Preço: ${latest.last}, Variação: ${latest.chg_pct}%`);
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
  const service = new FinnhubDataService();
  setupGracefulShutdown(service);

  // Verificar argumentos da linha de comando
  const args = process.argv.slice(2);

  if (args.includes("--stats")) {
    await service.getStats();
    return;
  }

  if (args.includes("--once")) {
    console.log("🎯 Executando apenas uma vez...");
    await service.fetchFinnhubData();
    return;
  }

  if (args.includes("--test-page")) {
    console.log("🧪 Testando apenas primeira página...");
    const service = new FinnhubDataService();
    const response = await service.makeRequest(0);
    if (response && response.data) {
      console.log(`✅ Teste bem-sucedido: ${response.data.length} registros na primeira página`);
    }
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

module.exports = FinnhubDataService;