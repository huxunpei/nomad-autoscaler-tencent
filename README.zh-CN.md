# 腾讯云 AS Target 插件

[English](README.md) · [配置参考](docs/configuration.md) · [运行与恢复](docs/operations.md) · [开发说明](docs/development.md)

[权限样例](docs/permissions.md)区分 AS 组授权和动态 CVM 成员授权；[维护计划](docs/maintenance.md)给出版本跟进、兼容性回归及升级流程，维护负责人为 xunpeihu，修复期限一个月。

`tencent-as` 是由 Nomad Autoscaler Agent 从 `plugin_dir` 启动的独立 Target 插件。Agent 负责指标和策略，插件负责 AS 容量与 Nomad 节点排空/后处理；不需要改造 Nomad 服务端，也不要求 Kubernetes。

**v0.1.1 已发布：** 本版修复启动认证失败时的崩溃，为初始化重试日志补充云错误原因，并修订文档；插件二进制会变化。此前基线已完成带作业 1→5→1 和失败恢复等实云验证。v0.1.1 的 linux-amd64 二进制已用实例角色在真实 AS 上通过只读启动检查；扩缩容没有用 v0.1.1 二进制重跑。个人仓库为 `huxunpei/nomad-autoscaler-tencent`，采用 [MPL-2.0](LICENSE)。实际检查及限制见[发布说明](docs/release.md)和包内验证记录；启用扩缩容前，请在你的环境中验证身份、权限和作业行为。

术语：**AS** 指腾讯云弹性伸缩；**ASG** 指 AS 伸缩组；**启动配置**是 AS 用来创建组内 VM 的模板。

## 当前行为

实际 Count 来自 ASG 成员，未注册 Nomad 的 VM 也计入。绝对目标按实时云组边界裁剪；SDK 选点后固定精确节点，任务和 poststop 结束才删除对应 VM，云结果收敛后再完成必要 post。无法映射的节点不作为删除候选。

确定删除失败时，自动恢复本插件拥有的存活节点调度资格，并继续对账。AS 仍可能重试删除；恢复 eligible 不表示云成员恢复正常或重试已取消。原失败保留为失败，不伪造成功 cooldown。未知结果不盲目重发，不通过删除 journal 或强制解锁通关。

状态按已验证账号、地域、组隔离，要求单写者和持久化目录；单机文件锁不是跨主机 HA。Agent 和 SDK/恢复路径共用 Nomad 连接语义。默认使用已有 CVM 实例身份，也可显式选择 API key；不会自动创建 IAM 角色或静默切换凭据来源。

## 从发布包安装

1. 把[最新发布](https://github.com/huxunpei/nomad-autoscaler-tencent/releases/latest)的**全部**文件下载到同一目录：`linux_amd64` 和 `linux_arm64` 压缩包、源码包、`SHA256SUMS` 及其中列出的 4 个 JSON 文件。
2. 校验：`sha256sum -c SHA256SUMS`（macOS：`shasum -a 256 -c SHA256SUMS`），每行都必须以 `OK` 结尾。只下载了部分文件时，GNU 的 `sha256sum --ignore-missing -c SHA256SUMS` 只校验已有的文件。
3. 解压对应 CPU 的压缩包：`tar xzf nomad-autoscaler-tencent_<版本>_linux_amd64.tar.gz`。里面有 `nomad-autoscaler-tencent-<版本>/linux-amd64/tencent-as`（插件）和 `.../tencent-recover`（维护工具），以及许可、示例和文档。
4. 把插件安装为 `<plugin_dir>/tencent-as`，属主为运行 Agent 的用户，权限 `0755`。文件名必须与 Agent 配置里的 `driver = "tencent-as"` 一致。官方 Nomad Autoscaler Agent 需要另行安装，发布包不含它。
5. 继续看下面的配置说明。

## 构建与示例

Go 最低版本以 `go.mod` 为准，当前为 1.25.10；候选构建与 CI 使用 Go 1.26.6，实际工具链版本单独记录。插件 SDK 依赖为 v0.5.0，外部 Agent 发行包是另一版本维度。当前验证基线为未修改官方 Agent v0.6.0 和 Nomad 2.0.7，按下述要求配置重启冷却和策略目录。

```sh
make race
make cross-build
```

Linux amd64/arm64 各输出插件和恢复工具。编译成功不代表两个架构均已完成加载或真实云验收。

状态目录（默认 `/var/lib/nomad-autoscaler/tencent-as`，配置键 `tencent_state_dir`）必须是私有的：权限 `0700`，属主是运行 Agent 的用户，路径中任何一段都不能是符号链接。目录不存在时插件会以 `0700` 创建；已存在的目录若允许组或其他用户访问，插件会拒绝启动并报 `StateDirectoryNotPrivate`、`RouteDirectoryNotPrivate` 或 `UnsafeStateDirectory`。目录内文件为 `0600`，恢复备份时保持该权限。

插件文件放在 `<plugin_dir>/tencent-as`，把所选策略放进 Agent 配置的策略目录；ASG 写在策略里，不在 Agent 的 target 配置里重复。按 [Agent 示例](examples/autoscaler.hcl)配置，并为同一 ASG 只安装一份策略：[固定目标](examples/cluster-fixed.hcl)或[Prometheus CPU](examples/cluster-prometheus.hcl)。每条策略的 target 都要设置 `restart_cooldown`，值与该策略的 `cooldown` 相同；不写的话，Agent 重启后可能立刻再次伸缩。说明见 [重启冷却](docs/configuration.md#restart-cooldown)。示例 ASG 是合成占位值，策略默认禁用；替换部署信息、完成授权和验收后才启用。默认 Target 启动仍会认证腾讯云，禁用策略不是凭据或网络隔离措施。启动成功时，Agent 日志里会出现 `successfully launched and dispensed plugin` 和 `"plugin_name":"tencent-as"`；启动失败请看[故障排查](docs/operations.md#troubleshooting)。启动命令：`nomad-autoscaler agent -config=/etc/nomad-autoscaler/autoscaler.hcl`。

旧的 simulation/live-experimental 入口与历史实验使用不同参数和限制，不作为默认 Target 的隐含前提。常规恢复由后台完成；人工恢复工具不是普通失败场景验收的替代路径。不要混用不同策略或读写器、清空状态、覆盖备份来绕过未知结果。

## 部署范围与维护

首期支持使用启动配置的按量 ASG 自动创建成员。每组使用一个容量控制方，保留持久化状态目录。`restart_cooldown` 与策略 `cooldown` 一致，`policy_dir` 与 Agent 使用相同目录；策略修改后受控重启 Agent。

核心实云场景已完成；ACL/TLS、5 分钟重启冷却另有定向验证。arm64 加载采用用户态仿真。默认 30 分钟宽限期到期后的完整扩容路径未执行，首期不再追加补测，验证边界见[发布准备](docs/release.md)。

维护人为 xunpeihu（GitHub：`huxunpei`），修复期限一个月，详见[维护约定](docs/maintenance.md)。源代码采用 [MPL-2.0](LICENSE)，依赖声明见[第三方声明](THIRD_PARTY_NOTICES.md)。
