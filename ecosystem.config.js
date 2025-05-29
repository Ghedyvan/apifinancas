module.exports = {
  apps: [
    {
      name: "api-investing-brasil",
      script: "apiInvesting.js",
      cwd: "/Users/ghedyvanvinicius/Projetos/apifinancas",
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "1G",
      env: {
        NODE_ENV: "production"
      },
      error_file: "./logs/api-investing-brasil-error.log",
      out_file: "./logs/api-investing-brasil-out.log",
      log_file: "./logs/api-investing-brasil-combined.log",
      time: true
    },
    {
      name: "api-investing-usa",
      script: "apiInvestingUSA.js",
      cwd: "/Users/ghedyvanvinicius/Projetos/apifinancas",
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "1G",
      env: {
        NODE_ENV: "production"
      },
      error_file: "./logs/api-investing-usa-error.log",
      out_file: "./logs/api-investing-usa-out.log",
      log_file: "./logs/api-investing-usa-combined.log",
      time: true
    },
    {
      name: "tradingview-etfs",
      script: "apiTradingViewETF.js",
      cwd: "/Users/ghedyvanvinicius/Projetos/apifinancas",
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "512M",
      env: {
        NODE_ENV: "production"
      },
      error_file: "./logs/tradingview-etfs-error.log",
      out_file: "./logs/tradingview-etfs-out.log",
      log_file: "./logs/tradingview-etfs-combined.log",
      time: true
    },
    {
      name: "fiis-scrapping",
      script: "fiiscrapping.js",
      cwd: "/Users/ghedyvanvinicius/Projetos/apifinancas",
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "512M",
      env: {
        NODE_ENV: "production"
      },
      error_file: "./logs/fiis-scrapping-error.log",
      out_file: "./logs/fiis-scrapping-out.log",
      log_file: "./logs/fiis-scrapping-combined.log",
      time: true
    },
    {
      name: "cotacoes-dolar",
      script: "dolar.js",
      cwd: "/Users/ghedyvanvinicius/Projetos/apifinancas",
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "256M",
      env: {
        NODE_ENV: "production"
      },
      error_file: "./logs/cotacoes-dolar-error.log",
      out_file: "./logs/cotacoes-dolar-out.log",
      log_file: "./logs/cotacoes-dolar-combined.log",
      time: true
    }
  ]
};