require("dotenv").config();
const { createClient } = require("@supabase/supabase-js");
const fetch = require("node-fetch");

// Configurações do Supabase
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_ANON_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);

// API para cotação do dólar (AwesomeAPI - gratuita)
const DOLLAR_API_URL = "https://economia.awesomeapi.com.br/json/last/USD-BRL";

class DolarCotacaoService {
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

    // Segunda a sexta (1-5) das 9:00 às 18:00
    const isWeekday = dayOfWeek >= 1 && dayOfWeek <= 5;
    const isBusinessTime = currentTime >= 9.0 && currentTime <= 18.0;

    return isWeekday && isBusinessTime;
  }

  async fetchDollarQuote() {
    try {
      const response = await fetch(DOLLAR_API_URL, {
        method: "GET",
        headers: {
          "Accept": "application/json",
          "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        },
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.log(`❌ Erro HTTP ${response.status}:`, errorText.substring(0, 500));
        throw new Error(`HTTP error! status: ${response.status} - ${response.statusText}`);
      }

      const data = await response.json();
      return data;
    } catch (error) {
      console.error("❌ Erro ao buscar cotação do dólar:", error.message);
      return null;
    }
  }

  formatDollarData(apiData) {
    if (!apiData || !apiData.USDBRL) {
      return null;
    }

    const usdData = apiData.USDBRL;
    
    // Dados da API AwesomeAPI:
    // bid: cotação de compra (atual)
    // ask: cotação de venda
    // high: máxima do dia
    // low: mínima do dia
    // varBid: variação da cotação de compra
    // pctChange: variação percentual
    
    const precoAtual = parseFloat(usdData.bid);
    const precoAbertura = parseFloat(usdData.low); // Usando low como aproximação da abertura
    const variacaoPercentual = parseFloat(usdData.pctChange);
    
    // Se não temos pctChange, calcular baseado em high/low
    let variacaoCalculada = variacaoPercentual;
    if (!variacaoPercentual && usdData.high && usdData.low) {
      const high = parseFloat(usdData.high);
      const low = parseFloat(usdData.low);
      variacaoCalculada = ((precoAtual - low) / low) * 100;
    }
    
    return {
      id: 32, // ID fixo conforme especificado
      ticker: "USD",
      preco: precoAtual, // Cotação atual de compra
      preco_abertura: precoAbertura, // Aproximação usando low
      variacao_percentual: variacaoCalculada ? parseFloat(variacaoCalculada.toFixed(4)) : null,
      ultima_atualizacao: new Date().toISOString(),
      logo_url: "https://icons.veryicon.com/png/o/miscellaneous/alan-ui/logo-usd-3.png",
      moeda: "BRL",
      nome: "Dólar Americano"
    };
  }

  async saveToSupabase(dollarData) {
    try {
      console.log(`💰 USD/BRL: R$ ${dollarData.preco}`);
  

      // Upsert pelo ID fixo (32)
      const { data, error } = await supabase
        .from("cotacoes_cache")
        .upsert(dollarData, { onConflict: "id" })
        .select();

      if (error) {
        console.error("❌ Erro ao salvar no Supabase:", error.message);
        return false;
      }

      return true;
    } catch (error) {
      console.error("❌ Erro geral ao salvar no Supabase:", error.message);
      return false;
    }
  }

  async fetchAndSaveDollarQuote() {
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

        // Buscar cotação
        const apiData = await this.fetchDollarQuote();

        if (!apiData) {
          console.log("❌ Falha ao obter dados da API de cotação");
          return;
        }

        // Formatar dados
        const dollarData = this.formatDollarData(apiData);

        if (!dollarData) {
          console.log("❌ Falha ao formatar dados da cotação");
          return;
        }

        // Salvar no Supabase
        const success = await this.saveToSupabase(dollarData);
      } 
    } catch (error) {
      console.error("❌ Erro durante a coleta:", error.message);
    }
  }

  startAutomation() {
    const brasiliaTime = this.getBrasiliaTime();
   
    // Executar imediatamente
    this.fetchAndSaveDollarQuote();

    // Agendar execuções a cada 15 minutos
    this.intervalId = setInterval(() => {
      this.fetchAndSaveDollarQuote();
    }, 15 * 60 * 1000);

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
      const { data, error } = await supabase
        .from("cotacoes_cache")
        .select("*")
        .eq("id", 32)
        .single();

      if (error) {
        console.error("❌ Erro ao buscar estatísticas:", error.message);
        return;
      }

      const brasiliaTime = this.getBrasiliaTime();

    } catch (error) {
      console.error("❌ Erro ao buscar estatísticas:", error.message);
    }
  }

  async testConnection() {
    try {
      console.log("🧪 Testando conexão com API de cotação...");
      
      const apiData = await this.fetchDollarQuote();
      
      if (apiData && apiData.USDBRL) {
        const dollarData = this.formatDollarData(apiData);
        return true;
      } else {
        console.log("❌ Falha na resposta da API");
        return false;
      }
    } catch (error) {
      console.error("❌ Erro no teste:", error.message);
      return false;
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
  const service = new DolarCotacaoService();
  setupGracefulShutdown(service);

  // Verificar argumentos da linha de comando
  const args = process.argv.slice(2);

  if (args.includes("--stats")) {
    await service.getStats();
    return;
  }

  if (args.includes("--test")) {
    await service.testConnection();
    return;
  }

  if (args.includes("--once")) {
    console.log("🎯 Executando apenas uma vez...");
    await service.fetchAndSaveDollarQuote();
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

module.exports = DolarCotacaoService;