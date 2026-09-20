# gsb-task-001 · User Prompt（原文，两次跑逐字相同）

请使用 Go 语言（仅使用 Go 标准库，严禁引入任何第三方外部依赖）从零实现一个支持轻量级 MVCC（多版本并发控制）和 WAL（预写日志）的嵌入式键值存储引擎。
该存储引擎需实现完整的事务抽象，并在当前工作目录下组织代码。具体需求与设计规范如下：
1. 核心接口与事务机制：
   实现 Engine 结构体，支持 Open(dirPath string) (*Engine, error) 与 Close() error。
   实现 Transaction 上下文：支持 Begin(readOnly bool) (*Tx, error)、Tx.Get(key string) ([]byte, error)、Tx.Set(key string, value []byte) error、Tx.Delete(key string) error、Tx.Commit() error、Tx.Rollback() error。
   隔离级别要求达到 Snapshot Isolation（快照隔离）：
     事务在 Begin 时获取读取时间戳/全局递增版本号 read_tx_id，只能读取在该事务开始前已提交的最新版本数据，不可见正在进行中或在其后提交的事务数据。
     写写冲突检测（First-Committer-Wins）：若两个并发写事务修改了同一个 key，后提交的事务在 Commit 时必须检测出冲突并返回指定的 ErrWriteConflict 错误，同时事务被自动回滚。
2. 多版本存储与垃圾回收（MVCC & Vacuum）：
   内存索引采用版本链或有序多版本映射管理。每个记录包含 key, value, created_tx_id, deleted_tx_id, is_deleted 等元数据。
   实现 Active Transaction Tracker：准确追踪当前未完成的最早活跃事务版本（watermark）。
   提供 engine.Vacuum() 方法：安全清理所有版本号小于全局活跃 watermark 且已被软删除（或被更新版本完全覆盖）的过时版本，回收内存并清理无效索引。
3. WAL 持久化与崩溃一致性恢复：
   所有已提交的事务必须严格先写 WAL 并执行持久化刷盘（支持配置 Sync 策略或直接 os.File.Sync），随后才能对全局可见生效。
   WAL 采用二进制自描述日志格式：包含魔数 (Magic)、CRC32 校验码、TxID、RecordType（TxBegin, Set, Delete, TxCommit, TxRollback）、Key 长度与内容、Value 长度与内容。
   崩溃恢复验证：当 Engine.Open(dirPath) 打开一个已有 WAL 文件的目录时，必须顺序回放日志重构内存索引；对于 WAL 末尾已 Begin 但未写入 TxCommit 记录的未决（未提交或崩溃中断）事务，回放阶段必须坚决丢弃，绝不能泄露到已提交状态中；若检测到 CRC32 校验损坏的日志尾部，应截断至最后一个有效提交点并返回警告。
4. 并发安全性与质量保障：
   内部数据结构必须具备细粒度读写锁控制，确保高并发读取与事务提交时的数据竞态安全（需完全通过 go test -race 检查）。
   编写完备的自动化测试文件 engine_test.go，必须至少包含以下测试用例：
     1) TestSnapshotIsolation: 验证读事务不会受后续提交的并发写事务干扰；
     2) TestWriteConflict: 验证并发并发写事务修改同一 key 时的冲突检测与回滚；
     3) TestCrashRecovery: 模拟非正常关闭/带未提交事务的 WAL，重新 Open 验证仅已提交数据被正确恢复；
     4) TestVacuum: 验证旧版本数据在活跃事务完结后被正确清理回收。
