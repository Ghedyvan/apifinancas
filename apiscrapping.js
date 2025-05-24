const fs = require('fs').promises;
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

class InfoMoneyScrapperService {
    constructor() {
        this.baseUrl = 'https://api.infomoney.com.br/ativos/top-alta-baixa-por-ativo/acao';
        this.params = {
            sector: 'Todos',
            orderAtributte: 'Volume',
            pageSize: 15,
            search: ''
        };
        this.dataPath = path.join(__dirname, 'data');
        this.fileName = 'companies_data.json';
        this.intervalId = null;
        this.hasRunInitially = false;
        
        // Configuração do Supabase
        this.supabaseUrl = process.env.SUPABASE_URL || 'YOUR_SUPABASE_URL';
        this.supabaseKey = process.env.SUPABASE_ANON_KEY || 'YOUR_SUPABASE_ANON_KEY';
        this.supabase = createClient(this.supabaseUrl, this.supabaseKey);
    }

    async init() {
        // Criar diretório data se não existir
        try {
            await fs.mkdir(this.dataPath, { recursive: true });
        } catch (error) {
            console.log('Diretório já existe ou erro ao criar:', error.message);
        }
    }

    getBrasiliaTime() {
        // Método mais preciso usando timezone nativo do JavaScript
        const now = new Date();
        return new Date(now.toLocaleString("en-US", {timeZone: "America/Sao_Paulo"}));
    }
    isBusinessHours() {
        const brasiliaTime = this.getBrasiliaTime();
        const dayOfWeek = brasiliaTime.getDay();
        const hour = brasiliaTime.getHours();
        const minute = brasiliaTime.getMinutes();
        const currentTime = hour + (minute / 60);

        // Segunda a sexta (1-5) das 9:30 às 17:30
        const isWeekday = dayOfWeek >= 1 && dayOfWeek <= 5;
        const isBusinessTime = currentTime >= 9.5 && currentTime <= 17.5;

        return isWeekday && isBusinessTime;
    }

    async makeRequest(pageIndex) {
        const url = new URL(this.baseUrl);
        url.searchParams.append('sector', this.params.sector);
        url.searchParams.append('orderAtributte', this.params.orderAtributte);
        url.searchParams.append('pageIndex', pageIndex);
        url.searchParams.append('pageSize', this.params.pageSize);
        url.searchParams.append('search', this.params.search);

        try {
            const response = await fetch(url.toString());
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const data = await response.json();
            return data;
        } catch (error) {
            console.error(`Erro ao fazer requisição para página ${pageIndex}:`, error.message);
            return null;
        }
    }

    calculateOpeningPrice(currentValue, changeDayFormatted) {
        try {
            // Verificar se temos uma variação válida
            if (!changeDayFormatted || changeDayFormatted === '0,00' || changeDayFormatted === '0.00') {
                return currentValue; // Se não há variação, o preço atual é igual ao de abertura
            }

            // Converter string para número (ex: "-1,23" para -1.23)
            const changePercent = parseFloat(changeDayFormatted.replace(',', '.'));
            
            if (isNaN(changePercent)) {
                console.warn(`Variação inválida: ${changeDayFormatted}`);
                return null;
            }

            // Fórmula: Preço Abertura = Preço Atual ÷ (1 + Variação% ÷ 100)
            const openingPrice = currentValue / (1 + (changePercent / 100));
            
            return parseFloat(openingPrice.toFixed(4));
        } catch (error) {
            console.error(`Erro ao calcular preço de abertura:`, error.message);
            return null;
        }
    }

    filterStockData(companies) {
        return companies.map(company => {
            // Calcular preço de abertura usando a fórmula
            const openingPrice = this.calculateOpeningPrice(
                company.Value, 
                company.ChangeDayFormatted
            );

            return {
                date: company.Date,
                stock_code: company.StockCode,
                stock_name: company.StockName,
                value: company.Value,
                value_formatted: company.ValueFormatted,
                change_day_formatted: company.ChangeDayFormatted,
                opening_price: openingPrice,
                image_url: null
            };
        });
    }

    async saveToSupabase(stockData) {
        try {
            console.log(`💾 Salvando ${stockData.length} registros no Supabase...`);
            
            // Inserir dados em lotes para melhor performance
            const batchSize = 100;
            let successCount = 0;
            let errorCount = 0;

            for (let i = 0; i < stockData.length; i += batchSize) {
                const batch = stockData.slice(i, i + batchSize);
                
                const { data, error } = await this.supabase
                    .from('stock_data')
                    .insert(batch);

                if (error) {
                    console.error(`❌ Erro ao inserir lote ${Math.floor(i/batchSize) + 1}:`, error.message);
                    errorCount += batch.length;
                } else {
                    successCount += batch.length;
                    console.log(`✅ Lote ${Math.floor(i/batchSize) + 1} inserido com sucesso`);
                }
            }

            console.log(`📊 Resumo: ${successCount} sucessos, ${errorCount} erros`);
            return successCount > 0;
        } catch (error) {
            console.error('❌ Erro ao salvar no Supabase:', error.message);
            return false;
        }
    }

    async getAllPages() {
        const brasiliaTime = this.getBrasiliaTime();
        console.log(`🚀 Iniciando coleta de dados - ${brasiliaTime.toLocaleString('pt-BR', {timeZone: 'America/Sao_Paulo'})}`);
        
        const allData = {
            timestamp: new Date().toISOString(),
            referenceDate: null,
            totalCount: 0,
            totalPages: 0,
            companies: []
        };

        // Primeira requisição para obter informações totais
        const firstPage = await this.makeRequest(1);
        
        if (!firstPage) {
            console.error('❌ Erro ao obter primeira página');
            return null;
        }

        allData.referenceDate = firstPage.ReferenceDate;
        allData.totalCount = firstPage.TotalCount;
        allData.totalPages = firstPage.TotalPages;

        console.log(`📊 Total de páginas: ${firstPage.TotalPages}`);
        console.log(`📈 Total de empresas: ${firstPage.TotalCount}`);

        // Adicionar dados da primeira página
        if (firstPage.Data) {
            allData.companies.push(...firstPage.Data);
        }

        // Fazer requisições para as páginas restantes
        const requests = [];
        for (let i = 2; i <= Math.min(firstPage.TotalPages, 24); i++) {
            requests.push(this.makeRequest(i));
        }

        // Executar todas as requisições em paralelo
        const results = await Promise.allSettled(requests);

        results.forEach((result, index) => {
            if (result.status === 'fulfilled' && result.value && result.value.Data) {
                allData.companies.push(...result.value.Data);
                console.log(`✅ Página ${index + 2} coletada com sucesso`);
            } else {
                console.log(`❌ Erro na página ${index + 2}`);
            }
        });

        console.log(`🎯 Total de empresas coletadas: ${allData.companies.length}`);
        return allData;
    }

    async saveToJson(data) {
        try {
            const filePath = path.join(this.dataPath, this.fileName);
            await fs.writeFile(filePath, JSON.stringify(data, null, 2), 'utf8');
            console.log(`💾 Dados salvos em: ${filePath}`);
            return true;
        } catch (error) {
            console.error('❌ Erro ao salvar arquivo:', error.message);
            return false;
        }
    }

    async loadFromJson() {
        try {
            const filePath = path.join(this.dataPath, this.fileName);
            const data = await fs.readFile(filePath, 'utf8');
            return JSON.parse(data);
        } catch (error) {
            console.log('📝 Arquivo não encontrado ou erro ao ler. Criando novo...');
            return null;
        }
    }

    async runScraping() {
        try {
            const brasiliaTime = this.getBrasiliaTime();
            
            // Verificar se é primeira execução ou horário comercial
            if (!this.hasRunInitially || this.isBusinessHours()) {
                if (!this.hasRunInitially) {
                    console.log(`🎬 PRIMEIRA EXECUÇÃO - ${brasiliaTime.toLocaleString('pt-BR', {timeZone: 'America/Sao_Paulo'})}`);
                    this.hasRunInitially = true;
                } else {
                    console.log(`⏰ EXECUÇÃO PROGRAMADA - ${brasiliaTime.toLocaleString('pt-BR', {timeZone: 'America/Sao_Paulo'})}`);
                }

                const data = await this.getAllPages();
                
                if (data && data.companies.length > 0) {
                    console.log('🔄 Calculando preços de abertura...');
                    
                    // Filtrar dados e calcular preços de abertura
                    const filteredData = this.filterStockData(data.companies);
                    
                    // Mostrar estatísticas dos preços de abertura calculados
                    const withOpeningPrice = filteredData.filter(item => item.opening_price !== null);
                    const validCalculations = filteredData.filter(item => 
                        item.opening_price !== null && item.opening_price !== item.value
                    );
                    
                    console.log(`📊 Preços de abertura calculados: ${withOpeningPrice.length}/${filteredData.length}`);
                    console.log(`🎯 Cálculos com variação: ${validCalculations.length}`);
                    
                    // Mostrar exemplo de cálculo
                    if (validCalculations.length > 0) {
                        const example = validCalculations[0];
                        console.log(`💡 Exemplo: ${example.stock_code} - Atual: R$ ${example.value}, Abertura: R$ ${example.opening_price}, Variação: ${example.change_day_formatted}%`);
                    }
                    
                    // Salvar no Supabase
                    const supabaseSuccess = await this.saveToSupabase(filteredData);
                    
                    // Também salvar no JSON como backup
                    await this.saveToJson(data);
                    
                    if (supabaseSuccess) {
                        console.log(`✨ Scraping concluído às ${brasiliaTime.toLocaleString('pt-BR', {timeZone: 'America/Sao_Paulo'})}\n`);
                    } else {
                        console.log('⚠️ Scraping concluído mas houve erros ao salvar no Supabase\n');
                    }
                } else {
                    console.log('❌ Falha no scraping\n');
                }
            } else {
                console.log(`⏸️  FORA DO HORÁRIO COMERCIAL - ${brasiliaTime.toLocaleString('pt-BR', {timeZone: 'America/Sao_Paulo'})} - Aguardando próxima execução...\n`);
            }
        } catch (error) {
            console.error('❌ Erro durante o scraping:', error.message);
        }
    }

    async getSupabaseStats() {
        try {
            const { count, error } = await this.supabase
                .from('stock_data')
                .select('*', { count: 'exact', head: true });

            if (error) {
                console.error('❌ Erro ao buscar estatísticas:', error.message);
                return;
            }

            // Contar registros com preço de abertura
            const { count: countWithOpening } = await this.supabase
                .from('stock_data')
                .select('*', { count: 'exact', head: true })
                .not('opening_price', 'is', null);

            const { data: latestRecord } = await this.supabase
                .from('stock_data')
                .select('created_at, stock_code, value, opening_price, change_day_formatted')
                .order('created_at', { ascending: false })
                .limit(1);

            console.log('\n📊 ESTATÍSTICAS DO SUPABASE:');
            console.log(`📈 Total de registros: ${count}`);
            console.log(`🎯 Registros com preço de abertura: ${countWithOpening}`);
            
            if (latestRecord && latestRecord.length > 0) {
                const latest = latestRecord[0];
                console.log(`📅 Último registro: ${new Date(latest.created_at).toLocaleString('pt-BR')}`);
                if (latest.opening_price) {
                    console.log(`💰 Exemplo: ${latest.stock_code} - Atual: R$ ${latest.value}, Abertura: R$ ${latest.opening_price}, Variação: ${latest.change_day_formatted}%`);
                }
            }
        } catch (error) {
            console.error('❌ Erro ao buscar estatísticas:', error.message);
        }
    }

    startScheduler() {
        const brasiliaTime = this.getBrasiliaTime();
        console.log(`🕒 Iniciando scheduler - execução a cada 10 minutos`);
        console.log(`🌎 Horário atual em Brasília: ${brasiliaTime.toLocaleString('pt-BR', {timeZone: 'America/Sao_Paulo'})}`);
        console.log(`📅 Funcionamento: Segunda a Sexta, 9:30h às 17:30h (horário de Brasília)`);
        
        // Executar imediatamente (primeira execução)
        this.runScraping();
        
        // Agendar execuções a cada 10 minutos (600000 ms)
        this.intervalId = setInterval(() => {
            this.runScraping();
        }, 10 * 60 * 1000);

        console.log('⏰ Scheduler ativo 24/7. Pressione Ctrl+C para parar.');
    }

    stopScheduler() {
        if (this.intervalId) {
            clearInterval(this.intervalId);
            this.intervalId = null;
            console.log('🛑 Scheduler parado');
        }
    }

    async getStats() {
        const data = await this.loadFromJson();
        const brasiliaTime = this.getBrasiliaTime();
        
        console.log(`\n📊 ESTATÍSTICAS - ${brasiliaTime.toLocaleString('pt-BR', {timeZone: 'America/Sao_Paulo'})}`);
        console.log(`⏰ Horário comercial ativo: ${this.isBusinessHours() ? '✅ SIM' : '❌ NÃO'}`);
        
        if (!data) {
            console.log('📊 Nenhum dado encontrado no arquivo local');
        } else {
            console.log('\n📁 ESTATÍSTICAS DO ARQUIVO LOCAL:');
            console.log(`📅 Última atualização: ${new Date(data.timestamp).toLocaleString('pt-BR')}`);
            console.log(`🎯 Data de referência: ${data.referenceDate}`);
            console.log(`📈 Total de empresas: ${data.companies.length}`);
            console.log(`📑 Total de páginas coletadas: ${data.totalPages}`);
        }

        // Também buscar estatísticas do Supabase
        await this.getSupabaseStats();
    }
}

// Função para manusear encerramento gracioso
function setupGracefulShutdown(scrapper) {
    process.on('SIGINT', () => {
        console.log('\n🛑 Recebido sinal de interrupção...');
        scrapper.stopScheduler();
        process.exit(0);
    });

    process.on('SIGTERM', () => {
        console.log('\n🛑 Recebido sinal de terminação...');
        scrapper.stopScheduler();
        process.exit(0);
    });
}

// Função principal
async function main() {
    const scrapper = new InfoMoneyScrapperService();
    
    await scrapper.init();
    setupGracefulShutdown(scrapper);

    // Verificar argumentos da linha de comando
    const args = process.argv.slice(2);
    
    if (args.includes('--stats')) {
        await scrapper.getStats();
        return;
    }
    
    if (args.includes('--once')) {
        console.log('🎯 Executando apenas uma vez...');
        await scrapper.runScraping();
        return;
    }

    // Iniciar scheduler por padrão
    scrapper.startScheduler();
}

// Executar apenas se este arquivo for executado diretamente
if (require.main === module) {
    main().catch(error => {
        console.error('❌ Erro fatal:', error);
        process.exit(1);
    });
}

module.exports = InfoMoneyScrapperService;