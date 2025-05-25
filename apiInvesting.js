require("dotenv").config();
const { createClient } = require("@supabase/supabase-js");

// Configurações do Supabase
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_ANON_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);

// URL da API Investing
const API_URL =
  "https://api.investing.com/api/financialdata/assets/equitiesByCountry/default";

class InvestingDataService {
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
    const queryParams = {
      "fields-list":
        "id,name,symbol,isCFD,high,low,last,lastPairDecimal,change,changePercent,volume,time,isOpen,url,flag,countryNameTranslated,exchangeId,performanceDay,performanceWeek,performanceMonth,performanceYtd,performanceYear,performance3Year,technicalHour,technicalDay,technicalWeek,technicalMonth,avgVolume,fundamentalMarketCap,fundamentalRevenue,fundamentalRatio,fundamentalBeta,pairType",
      "country-id": 32,
      "filter-domain": "",
      page: 0,
      "page-size": 1393,
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

    console.log(`🚀 Fazendo requisição GET para: ${url.toString()}`);

    try {
      const response = await fetch(url.toString(), {
        method: "GET",
        headers: {
          Accept: "*/*",
          "User-Agent": "Thunder Client (https://www.thunderclient.com)",
          Cookie:
            "__cflb=02DiuEaBtsFfH7bEbN5e6S2b8T1ZBoeD4McSCKs9QXk2Y; __cf_bm=vh9Hh8c0WRkL4VweyMhf05i84C3YBHe5EdnicJvuFek-1748165272-1.0.1.1-wSGSIYYObeLYEwY1gpHQkVoqUV0ixr1",
        },
      });

      console.log(
        `📡 Status da resposta: ${response.status} ${response.statusText}`
      );

      if (!response.ok) {
        const errorText = await response.text();
        console.log(
          `❌ Erro HTTP ${response.status}:`,
          errorText.substring(0, 500)
        );
        throw new Error(
          `HTTP error! status: ${response.status} - ${response.statusText}`
        );
      }

      const data = await response.json();
      return data;
    } catch (error) {
      console.error("❌ Erro ao fazer requisição:", error.message);
      return null;
    }
  }

  filterInvestingData(data) {
    return data.map((item) => ({
      id: item.Id ? parseInt(item.Id, 10) : null,
      chg: item.Chg || null,
      chg_pct: item.ChgPct || null,
      country_name_translated: item.CountryNameTranslated || null,
      flag: item.Flag || null,
      last: item.Last || null,
      name: item.Name || null,
      symbol: item.Symbol || null,
    }));
  }

  async saveToSupabase(investingData) {
    try {
      console.log(
        `💾 Salvando ${investingData.length} registros no Supabase...`
      );

      // Inserir ou atualizar dados em lotes
      const batchSize = 1000;
      let totalUpserted = 0;
      let errorCount = 0;

      for (let i = 0; i < investingData.length; i += batchSize) {
        const batch = investingData.slice(i, i + batchSize);
        console.log(
          `📦 Processando lote ${Math.floor(i / batchSize) + 1} com ${
            batch.length
          } registros...`
        );

        // upsert pelo campo id (int4)
        const { data, error: upsertError } = await supabase
          .from("investing_data")
          .upsert(batch, { onConflict: ["id"] })
          .select();

        if (upsertError) {
          console.error(
            `❌ Erro ao inserir/atualizar lote ${
              Math.floor(i / batchSize) + 1
            }:`,
            upsertError.message
          );
          errorCount += batch.length;
        } else {
          const upsertedCount = data ? data.length : 0;
          totalUpserted += upsertedCount;
          console.log(
            `✅ Lote ${
              Math.floor(i / batchSize) + 1
            }: ${upsertedCount} registros inseridos/atualizados`
          );
        }

        // Pequena pausa entre lotes
        await new Promise((resolve) => setTimeout(resolve, 100));
      }

      console.log(
        `📊 Resumo: ${totalUpserted} registros inseridos/atualizados, ${errorCount} erros`
      );
      return totalUpserted > 0;
    } catch (error) {
      console.error("❌ Erro geral ao salvar no Supabase:", error.message);
      return false;
    }
  }

  async fetchInvestingData() {
    try {
      const brasiliaTime = this.getBrasiliaTime();

      // Verificar se é primeira execução ou horário comercial
      if (!this.hasRunInitially || this.isBusinessHours()) {
        if (!this.hasRunInitially) {
          console.log(
            `🎬 PRIMEIRA EXECUÇÃO - ${brasiliaTime.toLocaleString("pt-BR", {
              timeZone: "America/Sao_Paulo",
            })}`
          );
          this.hasRunInitially = true;
        } else {
          console.log(
            `⏰ EXECUÇÃO PROGRAMADA - ${brasiliaTime.toLocaleString("pt-BR", {
              timeZone: "America/Sao_Paulo",
            })}`
          );
        }

        console.log("🚀 Iniciando coleta de dados da API Investing...");

        const response = await this.makeRequest();

        if (!response || !response.data) {
          console.log("❌ Falha ao obter dados da API");
          return;
        }

        const data = response.data;
        console.log(`📊 ${data.length} registros recebidos da API`);

        // Filtrar e preparar dados
        const filteredData = this.filterInvestingData(data);

        // Salvar no Supabase
        const supabaseSuccess = await this.saveToSupabase(filteredData);

        if (supabaseSuccess) {
          console.log(
            `✨ Coleta concluída às ${brasiliaTime.toLocaleString("pt-BR", {
              timeZone: "America/Sao_Paulo",
            })}\n`
          );
        } else {
          console.log(
            "⚠️ Coleta concluída mas houve erros ao salvar no Supabase\n"
          );
        }
      } else {
        console.log(
          `⏸️  FORA DO HORÁRIO COMERCIAL - ${brasiliaTime.toLocaleString(
            "pt-BR",
            { timeZone: "America/Sao_Paulo" }
          )} - Aguardando próxima execução...\n`
        );
      }
    } catch (error) {
      console.error("❌ Erro durante a coleta:", error.message);
    }
  }

  startAutomation() {
    const brasiliaTime = this.getBrasiliaTime();
    console.log("🕒 Iniciando automação - execução a cada 15 minutos");
    console.log(
      `🌎 Horário atual em Brasília: ${brasiliaTime.toLocaleString("pt-BR", {
        timeZone: "America/Sao_Paulo",
      })}`
    );
    console.log(
      "📅 Funcionamento: Segunda a Sexta, 9:30h às 17:30h (horário de Brasília)"
    );

    // Executar imediatamente
    this.fetchInvestingData();

    // Agendar execuções a cada 15 minutos
    this.intervalId = setInterval(() => {
      this.fetchInvestingData();
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
        .from("investing_data")
        .select("*", { count: "exact", head: true });

      if (error) {
        console.error("❌ Erro ao buscar estatísticas:", error.message);
        return;
      }

      const { data: latestRecord } = await supabase
        .from("investing_data")
        .select("created_at, symbol, name, last, chg, chg_pct")
        .order("created_at", { ascending: false })
        .limit(1);

      const brasiliaTime = this.getBrasiliaTime();

      console.log("\n📊 ESTATÍSTICAS DO INVESTING API:");
      console.log(`📈 Total de registros: ${count}`);
      console.log(
        `⏰ Horário comercial ativo: ${
          this.isBusinessHours() ? "✅ SIM" : "❌ NÃO"
        }`
      );

      if (latestRecord && latestRecord.length > 0) {
        const latest = latestRecord[0];
        console.log(
          `📅 Último registro: ${new Date(latest.created_at).toLocaleString(
            "pt-BR"
          )}`
        );
        console.log(
          `💰 Exemplo: ${latest.symbol} (${latest.name}) - Preço: ${latest.last}, Variação: ${latest.chg} (${latest.chg_pct}%)`
        );
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
  const service = new InvestingDataService();
  setupGracefulShutdown(service);

  // Verificar argumentos da linha de comando
  const args = process.argv.slice(2);

  if (args.includes("--stats")) {
    await service.getStats();
    return;
  }

  if (args.includes("--once")) {
    console.log("🎯 Executando apenas uma vez...");
    await service.fetchInvestingData();
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

module.exports = InvestingDataService;
