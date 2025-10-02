# Feishu MCP Server

集成飞书（Lark）的 MCP 服务器，支持消息、Drive 与多维表（Bitable）操作。

## 新增 Bitable 字段相关工具

已注册以下 MCP 工具：

- `bitable_query_fields(app_token, table_id)`
  - 获取指定表的所有字段信息，返回 Markdown。
  - 示例：
    ```json
    {
      "tool": "bitable_query_fields",
      "args": {"app_token": "bitable_app_token_xxx", "table_id": "tbl_xxx"}
    }
    ```

- `bitable_upsert_fields(app_token, table_id, fields)`
  - 批量新增或更新字段（Upsert）：
    - 若对象包含 `field_id` 则更新；
    - 若无 `field_id` 但提供 `field_name` 且同名字段存在，则更新；
    - 其他情况为新增。
  - 字段对象示例：
    ```json
    [
      {"field_name": "状态", "type": 3, "property": {"options": [{"name": "Open"}, {"name": "Closed"}]}},
      {"field_id": "fld_123", "field_name": "负责人", "type": 11, "description": "任务负责人"}
    ]
    ```
  - 调用示例：
    ```json
    {
      "tool": "bitable_upsert_fields",
      "args": {"app_token": "bitable_app_token_xxx", "table_id": "tbl_xxx", "fields": [/* 如上 */]}
    }
    ```

- `bitable_delete_fields(app_token, table_id, field_ids=None)`
  - 批量删除字段：仅支持通过 `field_ids` 指定。
  - 调用示例：
    ```json
    {
      "tool": "bitable_delete_fields",
      "args": {"app_token": "bitable_app_token_xxx", "table_id": "tbl_xxx", "field_ids": ["fld_1", "fld_2"]}
    }
    ```

## 新增 Bitable 表创建工具（Create-only）

- `bitable_create_table(app_token, table_name, fields=None)`
  - 仅创建数据表：
    - 若同名表已存在：返回存在提示，不进行任何字段更新。
    - 若不存在同名表：创建新表。`fields` 参数将被忽略，请使用 `bitable_upsert_fields` 管理字段。
  - 调用示例：
    ```json
    {
      "tool": "bitable_create_table",
      "args": {"app_token": "bitable_app_token_xxx", "table_name": "任务列表"}
    }
    ```

## 环境变量

在运行前确保配置：

- `FEISHU_APP_ID`
- `FEISHU_APP_SECRET`

## 注意事项

- `type` 需使用飞书多维表字段类型编码（如：3=单选、4=多选、11=人员等）。
- 批量新增/更新会拉取已有字段以做名称到 ID 匹配，字段很多时请求耗时会增长。