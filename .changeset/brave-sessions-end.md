---
"effect": patch
---

Add injectable MCP session storage and Streamable HTTP DELETE session termination. Direct `McpServer.run` calls now require `SessionStore`; layer constructors provide an in-memory default.
