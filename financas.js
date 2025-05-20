// Função que verifica se está dentro do horário comercial (10h às 18h) no horário de Brasília
function dentroHorarioComercial() {
  // Obter data atual no servidor (Dallas)
  const agora = new Date();
  
  // Dallas está em UTC-5/UTC-6, Brasília em UTC-3
  // Precisamos adicionar 2 ou 3 horas (dependendo do horário de verão) para converter para o horário de Brasília
  
  // Verificar se Dallas está em horário de verão (CDT = UTC-5) ou normal (CST = UTC-6)
  // Essa lógica é automática no objeto Date
  
  // Obter o fuso horário de Dallas em horas
  const fusoHorasDallas = -agora.getTimezoneOffset() / 60;
  
  // Fuso de Brasília é UTC-3, ou seja, -3
  const fusoBrasilia = -3;
  
  // Diferença entre Brasília e Dallas
  const diferencaHoras = fusoBrasilia - fusoHorasDallas;
  
  // Calcular a hora em Brasília
  let horaBrasilia = agora.getHours() + diferencaHoras;
  
  // Ajustar casos de virada do dia
  if (horaBrasilia >= 24) {
    horaBrasilia -= 24;
  } else if (horaBrasilia < 0) {
    horaBrasilia += 24;
  }
  
  console.log(`Hora local (Dallas): ${agora.getHours()}h - Hora Brasília: ${horaBrasilia}h`);
  
  return horaBrasilia >= 10 && horaBrasilia < 18;
}

// Função principal que roda as tarefas
async function executarAtualizacao() {
  // Verifica se está dentro do horário comercial
  if (!dentroHorarioComercial()) {
    console.log("Fora do horário comercial de Brasília (10h-18h). Atualizações pausadas.");
    return;
  }

  console.log("----- Iniciando atualização de cotações -----");
  console.log("Data/Hora local:", new Date().toLocaleString());

  try {
    // Sempre atualiza o dólar primeiro
    await buscarCotacaoDolar();

    // Busca os ativos
    const ativos = await buscarAtivos();

    if (ativos.length === 0) {
      console.log("Nenhum ativo encontrado para atualizar.");
      return;
    }

    console.log(
      `${ativos.length} ativos encontrados. Atualizando um por um...`
    );

    // Aqui usamos esperas sequenciais para evitar sobrecarga da API
    const resultados = [];
    for (const ativo of ativos) {
      // Aguarda um período entre requisições para não sobrecarregar a API
      const resultado = await atualizarCotacaoAtivo(ativo);
      resultados.push(resultado);

      // Espera 500ms entre requisições para ser gentil com a API
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    // Registra os resultados no log
    await registrarAtualizacao(resultados);

    console.log("----- Atualização concluída com sucesso -----\n");
  } catch (error) {
    console.error("Erro durante atualização:", error);
    console.log("----- Atualização finalizada com erros -----\n");
  }
}

// Executar imediatamente na inicialização, se estiver dentro do horário comercial
if (dentroHorarioComercial()) {
  executarAtualizacao();
} else {
  console.log(
    "Iniciando serviço fora do horário comercial de Brasília. Aguardando próximo período válido."
  );
}

// Agendar execução a cada 15 minutos
// Dallas está 2-3 horas atrás de Brasília
// 10h-18h Brasília = 7h-15h ou 8h-16h em Dallas (dependendo do horário de verão)
// Vamos usar condição mais ampla para cobrir ambas as possibilidades
cron.schedule("*/15 7-16 * * 1-5", executarAtualizacao); // De segunda a sexta

console.log("Serviço de atualização de cotações iniciado...");
console.log(
  "Horário de funcionamento: Segunda a Sexta, 10h às 18h (horário de Brasília), a cada 15 minutos"
);