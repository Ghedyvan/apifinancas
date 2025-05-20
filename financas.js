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
        const agora = new Date().toISOString();
        
        console.log(`${ticker}: ${preco} (${isBrazilian ? "BRL" : "USD"})`);
        
        // Salvar no cache
        const { error } = await supabase
          .from('cotacoes_cache')
          .upsert({
            ticker: ticker,
            preco: preco,
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

// Função que verifica se está dentro do horário comercial (10h às 18h)
function dentroHorarioComercial() {
  const agora = new Date();
  const hora = agora.getHours();
  return hora >= 10 && hora < 18;
}

// Função principal que roda as tarefas
async function executarAtualizacao() {
  // Verifica se está dentro do horário comercial
  if (!dentroHorarioComercial()) {
    console.log('Fora do horário comercial (10h-18h). Atualizações pausadas.');
    return;
  }
  
  console.log('----- Iniciando atualização de cotações -----');
  console.log('Data/Hora:', new Date().toLocaleString());
  
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

// Executar imediatamente na inicialização, se estiver dentro do horário comercial
if (dentroHorarioComercial()) {
  executarAtualizacao();
} else {
  console.log('Iniciando serviço fora do horário comercial. Aguardando próximo período válido.');
}

// Agendar execução a cada 15 minutos
cron.schedule('*/15 10-17 * * 1-5', executarAtualizacao); // De segunda a sexta, das 10h às 17:45

console.log('Serviço de atualização de cotações iniciado...');
console.log('Horário de funcionamento: Segunda a Sexta, 10h às 18h, a cada 15 minutos');