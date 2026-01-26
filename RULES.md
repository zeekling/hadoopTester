# Patch Rules and Rollback Guidelines

- 目标：建立一致、可追溯、可回滚的补丁治理流程，确保变更可重复、可审阅。
- 范围：文本文件的 Patch，尽量避免对二进制文件直接打补丁。

1. Patch 写入与应用
- 默认情况下，补丁应用应写入项目仓库，更新补丁摘要并记录在 CHANGELOG 的 Unreleased 区段，以及在 PATCH_LOG.md 记录补丁元数据。
- 补丁文件命名规范：`patch_<YYYYMMDD>_<HHMMSS>_<描述>.patch`，便于排序和追溯。
- Patch 摘要应包含：patch_id、作者、日期、简短描述、影响的文件列表。
- PATCH_LOG.md 格式示例：
  - patch_id: patch_20260126_1010_fix_feature
  - patch_file: patches/patch_20260126_1010_fix_feature.patch
  - date: 2026-01-26
  - author: <your-name>
  - changed_files: [list]
  - status: APPLIED
  - notes: 回滚指令在此处维护

- 应用工具：优先使用仓库提供的 `apply_patch` 工具进行补丁应用。
- 应用前应确保工作区干净，确保补丁不会覆盖敏感信息。

2. 回滚策略
- 未提交 Patch 的回滚：使用版本控制系统回滚到补丁应用前状态（如 `git restore --staged`/`git checkout --`），并删除补丁相关的条目。
- 已提交 Patch 的回滚：使用 `git revert <commit-hash>` 回滚提交，随后更新 CHANGELOG 的 Unreleased 区段与 PATCH_LOG。
- 回滚记录：在 PATCH_LOG.md 记录回滚的 patch_id、日期、原因、影响文件。

3. 回滚工作流
- 步骤 A：评估变更影响范围，确认回滚必要性和风险。
- 步骤 B：选择回滚方式（未提交/已提交），准备合并或撤销命令。
- 步骤 C：执行回滚，并重新运行相关测试以确保稳定性。
- 步骤 D：更新 CHANGELOG 与 PATCH_LOG，记录回滚原因与影响。
- 步骤 E：团队沟通并归档，确保透明性。

4. 自动化与脚本建议
- 建议实现 Patch Manager（如 scripts/patch_manager.py），用于：
  - 应用 patch 并自动写入 CHANGELOG 与 PATCH_LOG
  - 记录 patch_id、author、日期、影响文件
  - 生成回滚指令清单，方便审批与回滚
  - 支持回滚操作的快速执行

5. 风险与注意
- 只对文本变更进行 Patch，避免对二进制文件造成不可逆损坏。
- 在测试环境全量回归后再应用到生产分支。
- 回滚涉及外部资源时须额外小心，确保数据一致性。
- 始终确保变更可重复、可回溯，避免单点不可恢复的改动。

6. 示例工作流
- 应用补丁并记录：
  - patch_fix_feature.patch -> apply_patch patch_fix_feature.patch
  - CHANGELOG.md -> Unreleased 增记摘要
  - PATCH_LOG.md -> 记录 patch_id、日期、变更文件
- 回滚未提交补丁：
  - git restore --source=HEAD --staged --worktree <files>
  - 删除补丁记录（CHANGELOG、PATCH_LOG）中的相关条目
- 回滚已提交补丁：
  - git revert <commit_hash>
  - 更新 CHANGELOG 与 PATCH_LOG 的回滚记录

7. 角色与责任
- 开发者：编写补丁、提交 patch 描述、更新变更文件清单
- 审查者：检查补丁可回滚性、兼容性与测试影响
- CI/QA：执行构建与回归测试，验证补丁可用性

8. 变体与扩展
- 如需扩展，请在 PATCH_RULES.md 及 CHANGELOG.md 中添加对应条目，确保工具链的统一性。

9. 备注
- 本规则可作为初步 Patch 管理规范，实际执行应结合团队工作流和 CI/CD 策略进行调整。
