#!/usr/bin/env node

import { program } from 'commander';
import { buildConfig, loadConfigFile } from './config.js';
import { SyncOrchestrator } from './pipeline/orchestrator.js';
import { logger, setLogLevel, LogLevel } from './logger.js';
import { startUIServer } from './ui/index.js';

async function main() {
  program
    .name('mysql-sync')
    .description('MySQL 数据传输工具 — 全量同步 + Binlog 增量同步')
    .version('1.0.0');

  program
    .command('sync')
    .description('执行数据同步')
    .option('--source-host <host>', '源数据库主机', 'localhost')
    .option('--source-port <port>', '源数据库端口', (v) => parseInt(v), 3306)
    .option('--source-user <user>', '源数据库用户', 'root')
    .option('--source-password <password>', '源数据库密码', '')
    .option('--source-database <db>', '源数据库（可选）')
    .option('--target-host <host>', '目标数据库主机', 'localhost')
    .option('--target-port <port>', '目标数据库端口', (v) => parseInt(v), 3306)
    .option('--target-user <user>', '目标数据库用户', 'root')
    .option('--target-password <password>', '目标数据库密码', '')
    .option('--mode <mode>', '同步模式: full, incremental, full+incremental', 'full+incremental')
    .option('--full-sync', '启用全量同步')
    .option('--no-full-sync', '禁用全量同步')
    .option('--incremental-sync', '启用增量同步')
    .option('--no-incremental-sync', '禁用增量同步')
    .option('--databases <databases>', '要同步的数据库，逗号分隔')
    .option('--tables <tables>', '要同步的表，逗号分隔 (格式: db.table1,db.table2)')
    .option('--exclude-tables <tables>', '排除的表，逗号分隔')
    .option('--full-sync-workers <n>', '全量同步并发数', (v) => parseInt(v), 4)
    .option('--batch-size <n>', '每批读取行数', (v) => parseInt(v), 1000)
    .option('--queue-size <n>', '批处理队列大小', (v) => parseInt(v), 10)
    .option('--incr-workers <n>', '增量同步并发数', (v) => parseInt(v), 2)
    .option('--position-file <path>', 'Binlog 位置文件路径', './binlog-position.json')
    .option('--server-id <n>', 'Binlog 从库 Server ID', (v) => parseInt(v), Math.floor(Math.random() * 100000) + 1000)
    .option('-c, --config <path>', '配置文件路径 (JSON)')
    .option('--verbose', '详细日志')
    .option('--quiet', '安静模式')
    .action(async (options) => {
      if (options.verbose) setLogLevel(LogLevel.DEBUG);
      if (options.quiet) setLogLevel(LogLevel.ERROR);

      try {
        let config;

        if (options.config) {
          config = loadConfigFile(options.config);
          // Override with CLI options if explicitly provided
          const cliOpts = program.commands[0].opts();
          if (cliOpts.sourceHost !== 'localhost') config.source.host = cliOpts.sourceHost;
          if (cliOpts.targetHost !== 'localhost') config.target.host = cliOpts.targetHost;
          if (cliOpts.sourcePort !== 3306) config.source.port = cliOpts.sourcePort;
          if (cliOpts.targetPort !== 3306) config.target.port = cliOpts.targetPort;
        } else {
          config = buildConfig(options);
        }

        const orchestrator = new SyncOrchestrator(config);
        let shuttingDown = false;
        const shutdown = async () => {
          if (shuttingDown) return;
          shuttingDown = true;
          logger.info('\nShutting down gracefully...');
          await orchestrator.stop();
          orchestrator.printStats();
          process.exit(0);
        };

        process.on('SIGINT', shutdown);
        process.on('SIGTERM', shutdown);

        await orchestrator.start();

        if (config.mode === 'full') {
          orchestrator.printStats();
          await orchestrator.stop();
          process.exit(0);
        }

        logger.info('Incremental sync running. Press Ctrl+C to stop.');
        const statsInterval = setInterval(() => orchestrator.printStats(), 60000);

        await new Promise<void>((resolve) => {
          process.on('SIGINT', () => { clearInterval(statsInterval); resolve(); });
          process.on('SIGTERM', () => { clearInterval(statsInterval); resolve(); });
        });

        await orchestrator.stop();
        orchestrator.printStats();
        process.exit(0);

      } catch (err) {
        logger.error('Fatal error:', err);
        process.exit(1);
      }
    });

  program
    .command('ui')
    .description('启动 Web UI 管理界面')
    .option('-p, --port <port>', 'HTTP 服务端口', (v) => parseInt(v), 3000)
    .action(async (options) => {
      setLogLevel(LogLevel.INFO);
      const server = startUIServer(options.port);
      logger.info(`打开浏览器访问 http://localhost:${options.port}`);

      await new Promise<void>((resolve) => {
        const shutdown = () => { server.close(); resolve(); };
        process.on('SIGINT', shutdown);
        process.on('SIGTERM', shutdown);
      });
    });

  await program.parseAsync(process.argv);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
