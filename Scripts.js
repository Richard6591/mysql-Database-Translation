const mysql = require("mysql2/promise");
const axios = require("axios");
const fs = require("fs-extra");
const path = require("path");
const crypto = require("crypto");
const ProgressBar = require("progress");

// 配置信息
const config = {
  // 有道翻译API配置
  youdao: {
    apiUrl: "https://openapi.youdao.com/api",
    appKey: "",
    appSecret: "",
  },

  // MySQL数据库配置
  mysql: {
    host: "localhost",
    user: "root",
    password: "123456",
    database: "community",
    port: 3306,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
  },

  // 要处理的表名（支持数组）
  tableNames: [], // 可以设置为空数组自动获取所有表

  // 要翻译的字段名（可留空自动获取）
  translateFields: [],

  // 翻译目标语言（默认英文）
  targetLang: "en",

  // 日志和备份目录
  dataDir: "./translation_data",

  // API调用间隔(毫秒)
  apiInterval: 1000,

  // 每批处理记录数
  batchSize: 50,

  // 中文检测正则（包含中文标点）
  chineseRegex: /[\u4e00-\u9fa5]|[\u3000-\u303f]|[\uff00-\uffef]/,

  // 翻译重试配置
  translationRetry: {
    maxAttempts: 3,
    baseDelay: 2000,
    retriableErrors: ["103", "108", "202", "411"],
    retryAllErrors: false, // true=遇到任何错误都重试 false=只重试retriableErrors中的错误
  },

  // 清理配置
  cleanupOptions: {
    removeQuotes: true, // 是否移除翻译结果中的引号
    quoteChars: ['"', "'", '""', "''", "“", "”"], // 要移除的引号字符
  },
};

// 日志管理类
class Logger {
  constructor() {
    fs.ensureDirSync(config.dataDir);
    const now = new Date();
    const timestamp = `${now.getFullYear()}${(now.getMonth() + 1)
      .toString()
      .padStart(2, "0")}${now.getDate().toString().padStart(2, "0")}_${now
      .getHours()
      .toString()
      .padStart(2, "0")}${now.getMinutes().toString().padStart(2, "0")}${now
      .getSeconds()
      .toString()
      .padStart(2, "0")}`;

    this.stream = fs.createWriteStream(
      path.join(config.dataDir, `translation_${timestamp}.log`),
      { flags: "a" }
    );
  }

  log(message, toConsole = true) {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] ${message}\n`;

    if (this.stream && !this.stream.closed) {
      this.stream.write(logMessage);
    }

    if (toConsole) {
      console.log(logMessage.trim());
    }
  }

  async close() {
    if (this.stream && !this.stream.closed) {
      return new Promise((resolve) => {
        this.stream.end(() => resolve());
      });
    }
  }
}

// 创建全局logger实例
const logger = new Logger();

// 清理文本内容函数
function cleanText(text) {
  if (typeof text !== "string") return "";
  return text
    .replace(/\s+/g, " ")
    .replace(/[”“"']/g, "")
    .trim();
}

// 清理翻译结果中的引号
function cleanTranslationResult(text) {
  if (!config.cleanupOptions.removeQuotes || typeof text !== "string") {
    return text;
  }

  // 移除指定的引号字符
  config.cleanupOptions.quoteChars.forEach((quote) => {
    text = text.replace(new RegExp(quote, "g"), "");
  });

  return text.trim();
}

// 检测字符串是否包含中文
function containsChinese(text) {
  if (typeof text !== "string") return false;
  return config.chineseRegex.test(text);
}

// 获取数据库所有表名
async function getAllTableNames(pool) {
  const connection = await pool.getConnection();
  try {
    const [tables] = await connection.query(
      `SELECT table_name FROM information_schema.tables 
       WHERE table_schema = ?`,
      [config.mysql.database]
    );
    return tables.map((t) => t.TABLE_NAME);
  } finally {
    connection.release();
  }
}

// 表结构检查函数
async function checkTableStructure(pool, tableName) {
  const connection = await pool.getConnection();
  try {
    // 获取表的所有列信息
    const [columns] = await connection.query(`SHOW COLUMNS FROM ${tableName}`);

    // 获取主键信息
    const [keys] = await connection.query(
      `SHOW KEYS FROM ${tableName} WHERE Key_name = 'PRIMARY'`
    );

    const primaryKey = keys[0]?.Column_name || "id";

    logger.log(`表结构检查结果:
表名: ${tableName}
主键: ${primaryKey}
列信息:`);

    columns.forEach((col) => {
      logger.log(
        `- ${col.Field}: ${col.Type} ${col.Null === "NO" ? "NOT NULL" : ""} ${
          col.Extra
        }`
      );
    });

    return {
      primaryKey,
      columns,
      textFields: columns
        .filter((col) => /char|text/i.test(col.Type))
        .map((col) => col.Field)
        .filter((field) => field !== primaryKey),
    };
  } finally {
    connection.release();
  }
}

// 使用有道API翻译文本
async function translateText(text, attempt = 1) {
  const cleanedText = cleanText(text);
  if (!cleanedText) return null;

  try {
    const salt = Date.now();
    const sign = crypto
      .createHash("md5")
      .update(
        config.youdao.appKey + cleanedText + salt + config.youdao.appSecret
      )
      .digest("hex");

    const response = await axios.get(config.youdao.apiUrl, {
      params: {
        q: cleanedText,
        from: "zh-CHS",
        to: config.targetLang,
        appKey: config.youdao.appKey,
        salt: salt,
        sign: sign,
      },
      timeout: 5000,
    });

    // 记录API调用
    const apiLog = {
      timestamp: new Date().toISOString(),
      originalText: cleanedText,
      response: response.data,
      status: response.data.errorCode === "0" ? "success" : "failed",
      attempt: attempt,
    };

    await fs.appendFile(
      path.join(config.dataDir, "api_calls.jsonl"),
      JSON.stringify(apiLog) + "\n"
    );

    if (response.data.errorCode === "0") {
      // 清理翻译结果中的引号
      let translated = response.data.translation[0];
      translated = cleanTranslationResult(translated);
      return translated;
    } else {
      const errorMsg = `翻译API返回错误 [代码:${response.data.errorCode}]`;
      logger.log(errorMsg);

      // 检查是否需要重试
      const shouldRetry =
        config.translationRetry.retryAllErrors ||
        config.translationRetry.retriableErrors.includes(
          response.data.errorCode
        );

      if (shouldRetry) {
        throw new Error(errorMsg);
      }
      return null;
    }
  } catch (error) {
    logger.log(`调用翻译API出错 (尝试 ${attempt}): ${error.message}`);

    if (attempt < config.translationRetry.maxAttempts) {
      const delay =
        config.translationRetry.baseDelay * Math.pow(2, attempt - 1);
      logger.log(`将在 ${delay / 1000} 秒后重试...`);

      await new Promise((resolve) => setTimeout(resolve, delay));
      return translateText(text, attempt + 1);
    }

    logger.log(
      `达到最大重试次数 (${
        config.translationRetry.maxAttempts
      })，放弃翻译: ${cleanText(text)}`
    );
    return null;
  }
}

// 处理单个表的数据
async function processTable(pool, tableName, primaryKey, translateFields) {
  let progressBar;
  let processedCount = 0;
  let translatedCount = 0;
  let totalCount = 0;

  try {
    // 获取总记录数
    let connection = await pool.getConnection();
    const [totalRows] = await connection.query(
      `SELECT COUNT(*) as total FROM ${tableName}`
    );
    totalCount = totalRows[0].total;
    connection.release();

    logger.log(`表 ${tableName} 共找到 ${totalCount} 条记录，开始分批处理...`);

    // 初始化进度条
    progressBar = new ProgressBar(`${tableName} [:bar] :percent :etas`, {
      complete: "=",
      incomplete: " ",
      width: 50,
      total: totalCount,
    });

    // 创建翻译备份文件
    const backupStream = fs.createWriteStream(
      path.join(config.dataDir, `translation_${tableName}_backup.jsonl`)
    );

    // 分批处理
    for (let offset = 0; offset < totalCount; offset += config.batchSize) {
      connection = await pool.getConnection();
      const [rows] = await connection.query(
        `SELECT * FROM ${tableName} LIMIT ? OFFSET ?`,
        [config.batchSize, offset]
      );
      connection.release();

      // 处理当前批次
      for (const row of rows) {
        let needUpdate = false;
        const updateData = {};
        const translationLog = {
          [primaryKey]: row[primaryKey],
          original: {},
          translated: {},
          timestamp: new Date().toISOString(),
        };

        try {
          // 检查每个需要翻译的字段
          for (const field of translateFields) {
            if (row.hasOwnProperty(field) && row[field] !== null) {
              const value = String(row[field]);

              if (containsChinese(value)) {
                translationLog.original[field] = value;

                logger.log(
                  `检测到中文内容 [表:${tableName} ${primaryKey}:${row[primaryKey]}] [字段:${field}]: ${value}`
                );
                const translated = await translateText(value);

                if (translated) {
                  updateData[field] = translated;
                  translationLog.translated[field] = translated;
                  needUpdate = true;
                  translatedCount++;
                  logger.log(
                    `翻译成功 [表:${tableName} ${primaryKey}:${row[primaryKey]}]: ${value} → ${translated}`
                  );
                }
              }
            }
          }

          // 如果有需要更新的字段
          if (needUpdate) {
            connection = await pool.getConnection();
            await connection.query(
              `UPDATE ${tableName} SET ? WHERE ${primaryKey} = ?`,
              [updateData, row[primaryKey]]
            );
            connection.release();

            // 记录翻译日志
            backupStream.write(JSON.stringify(translationLog) + "\n");
          }
        } catch (error) {
          logger.log(
            `处理记录时出错 [表:${tableName} ${primaryKey}:${row[primaryKey]}]: ${error.message}`
          );
        }

        processedCount++;
        progressBar.tick();

        // 避免频繁调用API
        await new Promise((resolve) => setTimeout(resolve, config.apiInterval));
      }
    }

    backupStream.end();
    logger.log(
      `表 ${tableName} 处理完成: 共处理 ${processedCount} 条记录，成功翻译 ${translatedCount} 处内容`
    );

    return { processedCount, translatedCount, totalCount };
  } catch (error) {
    logger.log(`处理表 ${tableName} 过程中出错: ${error.message}`);
    throw error;
  } finally {
    logger.log(`表 ${tableName} 最终进度: ${processedCount}/${totalCount}`);
  }
}

// 验证单个表的翻译结果
async function verifyTranslations(
  pool,
  tableName,
  primaryKey,
  translateFields
) {
  let connection;
  try {
    connection = await pool.getConnection();

    const [untranslated] = await connection.query(
      `SELECT ${primaryKey}, ${translateFields.join(", ")} 
       FROM ${tableName} 
       WHERE ${translateFields
         .map((f) => `${f} REGEXP '[\\\\u4e00-\\\\u9fa5]'`)
         .join(" OR ")}`
    );

    if (untranslated.length > 0) {
      logger.log(
        `表 ${tableName} 发现 ${untranslated.length} 条未完全翻译的记录:`
      );
      untranslated.forEach((record) => {
        logger.log(`  ${primaryKey}: ${record[primaryKey]}`);
        translateFields.forEach((field) => {
          if (containsChinese(record[field])) {
            logger.log(`    ${field}: ${record[field]}`);
          }
        });
      });

      // 保存未翻译记录到文件
      await fs.writeFile(
        path.join(config.dataDir, `untranslated_${tableName}.json`),
        JSON.stringify(untranslated, null, 2)
      );

      return untranslated;
    } else {
      logger.log(`表 ${tableName} 验证完成: 所有中文内容已翻译`);
      return [];
    }
  } catch (error) {
    logger.log(`验证表 ${tableName} 过程中出错: ${error.message}`);
    throw error;
  } finally {
    if (connection) connection.release();
  }
}

// 主执行函数
async function main() {
  let pool;
  try {
    pool = mysql.createPool(config.mysql);
    logger.log("开始翻译任务");

    // 获取要处理的表列表
    let tablesToProcess = config.tableNames;
    if (!tablesToProcess || tablesToProcess.length === 0) {
      tablesToProcess = await getAllTableNames(pool);
      logger.log(`未指定表名，将处理所有表: ${tablesToProcess.join(", ")}`);
    }

    // 处理每个表
    for (const tableName of tablesToProcess) {
      try {
        logger.log(`\n===== 开始处理表: ${tableName} =====`);

        // 检查表结构
        const tableInfo = await checkTableStructure(pool, tableName);
        const primaryKey = tableInfo.primaryKey;

        // 确定要翻译的字段
        const translateFields =
          config.translateFields.length > 0
            ? config.translateFields
            : tableInfo.textFields;

        if (translateFields.length === 0) {
          logger.log(`表 ${tableName} 没有可翻译的文本字段，跳过`);
          continue;
        }

        logger.log(`将翻译以下字段: ${translateFields.join(", ")}`);

        // 处理表数据
        const result = await processTable(
          pool,
          tableName,
          primaryKey,
          translateFields
        );

        // 验证翻译结果
        logger.log(`开始验证表 ${tableName} 的翻译结果...`);
        await verifyTranslations(pool, tableName, primaryKey, translateFields);

        logger.log(
          `表 ${tableName} 任务完成: 共处理 ${result.processedCount}/${result.totalCount} 条记录，翻译 ${result.translatedCount} 处内容`
        );
      } catch (error) {
        logger.log(`处理表 ${tableName} 时发生错误: ${error.message}`);
      }
    }

    logger.log("\n===== 所有表处理完成 =====");
  } catch (error) {
    logger.log(`主流程出错: ${error.message}`);
  } finally {
    if (pool) await pool.end();
    await logger.close();
  }
}

// 启动程序
main().catch((err) => {
  console.error("未捕获的异常:", err);
  process.exit(1);
});
