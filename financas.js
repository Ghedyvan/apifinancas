require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const cron = require('node-cron');

// Configuração do Supabase
const supabaseUrl = "https://ccyqfilfqakmjitzjzgh.supabase.co";
const supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNjeXFmaWxmcWFrbWppdHpqemdoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDcwOTkwNjksImV4cCI6MjA2MjY3NTA2OX0.vrPdjvpbHueqcCADrY-0TNp6wJ2zWadiE-8Ap369HOo";
const supabase = createClient(supabaseUrl, supabaseKey);

// Token da BRAPI API
const BRAPI_TOKEN = "uYp94DQ1U3a3L8uqg1Adka";

// Função para buscar cotação do dólar
async function buscarCotacaoDolar() {
  try {
    console.log('Buscando cotação do dólar...');
    const response = await fetch('https://economia.awesomeapi.com.br/json/last/USD-BRL');
    const data = await response.json();

    if (data && data.USDBRL) {
      const cotacaoDolar = parseFloat(data.USDBRL.bid);
      console.log(`Cotação do dólar atualizada: R$ ${cotacaoDolar.toFixed(2)}`);
      
      // Salvar no cache do Supabase
      const { error } = await supabase
        .from('cotacoes_cache')
        .upsert({
          ticker: 'USD',
          preco: cotacaoDolar,
          ultima_atualizacao: new Date().toISOString(),
          origem: 'awesomeapi'
        }, {
          onConflict: 'ticker',
          ignoreDuplicates: false
        });
      
      if (error) throw error;
    }
  } catch (error) {
    console.error('Erro ao atualizar cotação do dólar:', error);
  }
}

// Função para buscar todos os ativos do banco
async function buscarAtivos() {
  try {
    console.log('Buscando ativos cadastrados...');
    const { data, error } = await supabase
      .from('ativos_investidos')
      .select('nome, tipo')
      .order('tipo', { ascending: true });
    
    if (error) throw error;
    
    return data || [];
  } catch (error) {
    console.error('Erro ao buscar ativos:', error);
    return [];
  }
}

// Função para atualizar a cotação de um único ativo
async function atualizarCotacaoAtivo(ativo) {
  try {
    console.log(`Atualizando cotação de ${ativo.nome} (${ativo.tipo})...`);
    
    // Determinar se é ativo brasileiro ou americano
    const isBrazilian = ativo.tipo !== "ação_eua" && ativo.tipo !== "etf_eua";
    
    const url = `https://brapi.dev/api/quote/${ativo.nome}?range=1d&interval=1d&fundamental=false&token=${BRAPI_TOKEN}`;
    
    const response = await fetch(url);
    const data = await response.json();
    
    if (data && data.results) {
      // API pode retornar um array ou objeto único
      const resultado = Array.isArray(data.results) ? data.results[0] : data.results;
      
      if (resultado) {
        const ticker = resultado.symbol;
        const preco = resultado.regularMarketPrice;
        const precoAbertura = resultado.regularMarketOpen; // Preço de abertura
        const variacaoPercentual = resultado.regularMarketChangePercent; // Variação percentual
        const agora = new Date().toISOString();
        
        // Novos campos a serem armazenados
        const logoUrl = resultado.logourl;
        const moeda = resultado.currency;
        const nomeAbreviado = resultado.shortName;
        const variacaoAbsoluta = resultado.regularMarketChange;
        
        console.log(`${ticker}: ${preco} (${isBrazilian ? "BRL" : "USD"}) | Abertura: ${precoAbertura} | Variação: ${variacaoPercentual?.toFixed(2)}%`);
        
        // Salvar no cache
        const { error } = await supabase
          .from('cotacoes_cache')
          .upsert({
            ticker: ticker,
            preco: preco,
            preco_abertura: precoAbertura,
            variacao_percentual: variacaoPercentual,
            variacao_absoluta: variacaoAbsoluta,
            logo_url: logoUrl,
            moeda: moeda,
            nome_abreviado: nomeAbreviado,
            ultima_atualizacao: agora,
            origem: 'brapi'
          }, {
            onConflict: 'ticker',
            ignoreDuplicates: false
          });
          
        if (error) throw error;
        
        return {
          ticker,
          preco,
          preco_abertura: precoAbertura,
          variacao_percentual: variacaoPercentual,
          variacao_absoluta: variacaoAbsoluta,
          logo_url: logoUrl,
          moeda: moeda,
          nome_abreviado: nomeAbreviado,
          sucesso: true
        };
      }
    }
    
    throw new Error('Dados não encontrados na resposta da API');
  } catch (error) {
    console.error(`Erro ao atualizar cotação de ${ativo.nome}:`, error);
    return {
      ticker: ativo.nome,
      sucesso: false,
      erro: error.message
    };
  }
}

// Função para registrar quando foi a última execução
async function registrarAtualizacao(resultados) {
  try {
    const timestamp = new Date().toISOString();
    const sucessos = resultados.filter(r => r.sucesso).length;
    const falhas = resultados.filter(r => !r.sucesso).length;
    
    const { error } = await supabase
      .from('sistema_logs')
      .insert({
        tipo: 'atualizacao_cotacoes',
        mensagem: `Atualização executada: ${sucessos} sucessos, ${falhas} falhas`,
        detalhes: JSON.stringify(resultados),
        timestamp: timestamp
      });
      
    if (error) throw error;
    
    console.log(`Log de atualização registrado: ${timestamp}`);
  } catch (error) {
    console.error('Erro ao registrar log:', error);
  }
}

// Função que verifica se está dentro do horário comercial (10h às 18h) considerando o fuso de Brasília
function dentroHorarioComercial() {
  // Dallas está em UTC-5 ou UTC-6 dependendo do horário de verão
  // Brasil está em UTC-3 (não existe mais horário de verão no Brasil)
  // Precisamos ajustar a diferença que é de 2-3 horas
  
  // Obter data atual no servidor (Dallas)
  const agora = new Date();
  
  // Obter o fuso horário de Dallas em minutos
  const offsetDallasMinutos = agora.getTimezoneOffset();
  
  // Fuso de Brasília é UTC-3, ou seja, -180 minutos em relação a UTC
  const offsetBrasiliaMinutos = -180;
  
  // Diferença entre os fusos em milissegundos
  const diferencaMs = (offsetDallasMinutos - offsetBrasiliaMinutos) * 60 * 1000;
  
  // Criar um novo objeto Date com o horário de Brasília
  const horaBrasilia = new Date(agora.getTime() + diferencaMs);
  
  // Obter a hora no horário de Brasília
  const hora = horaBrasilia.getHours();
  
  console.log(`Hora local (Dallas): ${agora.getHours()}h - Hora Brasil: ${hora}h`);
  
  return hora >= 10 && hora < 18;
}

// Função principal que roda as tarefas
async function executarAtualizacao(ignorarHorarioComercial = false) {
  // Verifica se está dentro do horário comercial (a menos que seja forçado)
  if (!ignorarHorarioComercial && !dentroHorarioComercial()) {
    console.log('Fora do horário comercial brasileiro (10h-18h). Atualizações pausadas.');
    return;
  }
  
  const execucaoForcada = ignorarHorarioComercial ? " (execução forçada)" : "";
  console.log(`----- Iniciando atualização de cotações${execucaoForcada} -----`);
  console.log('Data/Hora local:', new Date().toLocaleString());
  
  try {
    // Sempre atualiza o dólar primeiro
    await buscarCotacaoDolar();
    
    // Busca os ativos
    const ativos = await buscarAtivos();
    
    if (ativos.length === 0) {
      console.log('Nenhum ativo encontrado para atualizar.');
      return;
    }
    
    console.log(`${ativos.length} ativos encontrados. Atualizando um por um...`);
    
    // Aqui usamos esperas sequenciais para evitar sobrecarga da API
    const resultados = [];
    for (const ativo of ativos) {
      // Aguarda um período entre requisições para não sobrecarregar a API
      const resultado = await atualizarCotacaoAtivo(ativo);
      resultados.push(resultado);
      
      // Espera 500ms entre requisições para ser gentil com a API
      await new Promise(resolve => setTimeout(resolve, 500));
    }
    
    // Registra os resultados no log
    await registrarAtualizacao(resultados);
    
    console.log('----- Atualização concluída com sucesso -----\n');
  } catch (error) {
    console.error('Erro durante atualização:', error);
    console.log('----- Atualização finalizada com erros -----\n');
  }
}

// Executar imediatamente na inicialização, independente do horário comercial
console.log('Iniciando serviço de atualização de cotações...');
console.log('Executando atualização inicial...');
executarAtualizacao(true); // O parâmetro true indica execução forçada

// Agendar execução a cada 15 minutos
// Dallas está 2-3 horas atrás de Brasília
// 10h-18h Brasília = 7h-15h ou 8h-16h em Dallas (dependendo do horário de verão)
// Vamos usar condição mais ampla para cobrir ambas as possibilidades
cron.schedule('*/15 7-16 * * 1-5', () => executarAtualizacao(false)); // De segunda a sexta

console.log('Serviço de atualização de cotações iniciado...');
console.log('Horário de funcionamento regular: Segunda a Sexta, 10h às 18h (horário brasileiro), a cada 15 minutos');