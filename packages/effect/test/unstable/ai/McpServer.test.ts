import { describe, it } from "@effect/vitest"
import { deepStrictEqual, strictEqual } from "@effect/vitest/utils"
import { Effect, Layer, Option } from "effect"
import * as McpSchema from "effect/unstable/ai/McpSchema"
import * as McpServer from "effect/unstable/ai/McpServer"
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient"
import * as HttpClient from "effect/unstable/http/HttpClient"
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest"
import * as HttpRouter from "effect/unstable/http/HttpRouter"
import { RpcSerialization } from "effect/unstable/rpc"
import * as RpcClient from "effect/unstable/rpc/RpcClient"

const makeTestClient = Effect.gen(function*() {
  const responses: Array<Response> = []

  const serverLayer = McpServer.layerHttp({
    name: "TestServer",
    version: "1.0.0",
    path: "/mcp"
  })
  const { handler, dispose } = HttpRouter.toWebHandler(serverLayer, { disableLogger: true })
  yield* Effect.addFinalizer(() => Effect.promise(() => dispose()))

  let sessionId: string | null = null
  const customFetch: typeof fetch = async (input, init) => {
    const request = input instanceof Request ? input : new Request(input, init)
    if (sessionId) {
      request.headers.set("Mcp-Session-Id", sessionId)
    }
    const response = await handler(request)
    sessionId = response.headers.get("Mcp-Session-Id") ?? sessionId
    responses.push(response.clone())
    return response
  }

  const clientLayer = RpcClient.layerProtocolHttp({ url: "http://localhost/mcp" }).pipe(
    Layer.provideMerge([FetchHttpClient.layer, RpcSerialization.layerJsonRpc()]),
    Layer.provide(Layer.succeed(FetchHttpClient.Fetch, customFetch))
  )
  const client = yield* RpcClient.make(McpSchema.ClientRpcs).pipe(
    Effect.provide(clientLayer)
  )

  const httpClient = yield* HttpClient.HttpClient.pipe(
    Effect.provide(clientLayer)
  )

  return { client, responses, httpClient }
})

const makeWebHandler = Effect.fnUntraced(function*(store: Layer.Layer<McpServer.SessionStore>) {
  const { dispose, handler } = HttpRouter.toWebHandler(
    McpServer.layerHttp({
      name: "TestServer",
      version: "1.0.0",
      path: "/mcp"
    }).pipe(Layer.provide(store)),
    { disableLogger: true }
  )
  yield* Effect.addFinalizer(() => Effect.promise(() => dispose()))
  return handler
})

const webRequest = (
  handler: (request: Request) => Promise<Response>,
  method: string,
  options?: {
    readonly headers?: Record<string, string> | undefined
    readonly body?: unknown
  }
) =>
  Effect.promise(() =>
    handler(
      new Request("http://localhost/mcp", {
        method,
        headers: { "content-type": "application/json", ...options?.headers },
        ...(options?.body === undefined ? {} : { body: JSON.stringify(options.body) })
      })
    )
  )

const initializePayload = {
  protocolVersion: "2025-06-18",
  capabilities: {},
  clientInfo: {
    name: "TestClient",
    version: "1.0.0"
  }
}

const initializeBody = {
  jsonrpc: "2.0",
  id: 0,
  method: "initialize",
  params: initializePayload
}

const pingBody = {
  jsonrpc: "2.0",
  id: 1,
  method: "ping",
  params: {}
}

const getSessionId = (response: Response): string => {
  const sessionId = response.headers.get("Mcp-Session-Id")
  if (sessionId === null) {
    throw new Error("Expected initialize to return an MCP session id")
  }
  return sessionId
}

describe("McpServer", () => {
  it.effect("replays MCP session and negotiated protocol headers after initialize", () =>
    Effect.gen(function*() {
      const { client, responses } = yield* makeTestClient

      yield* client.initialize({
        protocolVersion: "9999-01-01",
        capabilities: {},
        clientInfo: {
          name: "TestClient",
          version: "1.0.0"
        }
      })

      yield* client.ping({})

      strictEqual(responses.length, 2)
      strictEqual(responses[0].headers.get("Mcp-Protocol-Version"), "2025-06-18")
    }))

  it.effect("returns 404 when a non-initialize request omits the MCP session id", () =>
    Effect.gen(function*() {
      const { httpClient } = yield* makeTestClient

      const response = yield* HttpClientRequest.post("http://locahost/mcp").pipe(
        HttpClientRequest.bodyJsonUnsafe({ jsonrpc: "2.0", method: "ping", params: {}, id: 0 }),
        httpClient.execute
      )

      strictEqual(response.status, 404)
    }))

  it.effect("terminates an initialized session exactly once", () =>
    Effect.gen(function*() {
      const { client, httpClient } = yield* makeTestClient

      const missingSessionResponse = yield* HttpClientRequest.make("DELETE")("http://localhost/mcp").pipe(
        httpClient.execute
      )
      strictEqual(missingSessionResponse.status, 400)

      yield* client.initialize(initializePayload)

      yield* client.ping({})

      const deleteResponse = yield* HttpClientRequest.make("DELETE")("http://localhost/mcp").pipe(
        HttpClientRequest.setHeader("Mcp-Protocol-Version", "2025-06-18"),
        httpClient.execute
      )
      strictEqual(deleteResponse.status, 200)
      strictEqual(yield* deleteResponse.text, "")

      const pingResponse = yield* HttpClientRequest.post("http://localhost/mcp").pipe(
        HttpClientRequest.bodyJsonUnsafe(pingBody),
        httpClient.execute
      )
      strictEqual(pingResponse.status, 404)

      const duplicateDeleteResponse = yield* HttpClientRequest.make("DELETE")("http://localhost/mcp").pipe(
        httpClient.execute
      )
      strictEqual(duplicateDeleteResponse.status, 404)
    }))

  it.effect("uses a custom SessionStore for initialize and DELETE", () =>
    Effect.gen(function*() {
      const sessions = new Map<
        string,
        Parameters<McpServer.SessionStore["Service"]["set"]>[1]
      >()
      const setSessionIds: Array<string> = []
      const removeSessionIds: Array<string> = []
      const store = McpServer.SessionStore.of({
        get: (sessionId) =>
          Effect.sync(() => {
            const payload = sessions.get(sessionId)
            return payload === undefined ? Option.none() : Option.some(payload)
          }),
        set: (sessionId, payload) =>
          Effect.sync(() => {
            setSessionIds.push(sessionId)
            sessions.set(sessionId, payload)
          }),
        remove: (sessionId) =>
          Effect.sync(() => {
            removeSessionIds.push(sessionId)
            return sessions.delete(sessionId)
          })
      })
      const webHandler = yield* makeWebHandler(Layer.succeed(McpServer.SessionStore, store))

      const initializeResponse = yield* webRequest(webHandler, "POST", {
        body: initializeBody
      })
      const sessionId = getSessionId(initializeResponse)
      deepStrictEqual(setSessionIds, [sessionId])

      const invalidDeleteResponse = yield* webRequest(webHandler, "DELETE", {
        headers: {
          "Mcp-Session-Id": sessionId,
          "Mcp-Protocol-Version": "9999-01-01"
        }
      })
      strictEqual(invalidDeleteResponse.status, 400)
      deepStrictEqual(removeSessionIds, [])

      const deleteResponse = yield* webRequest(webHandler, "DELETE", {
        headers: { "Mcp-Session-Id": sessionId }
      })
      strictEqual(deleteResponse.status, 200)
      deepStrictEqual(removeSessionIds, [sessionId])
    }))

  it.effect("shares one SessionStore across independent HTTP replicas", () =>
    Effect.gen(function*() {
      const sharedStore = yield* McpServer.SessionStore.pipe(
        Effect.provide(McpServer.layerSessionStoreInMemory)
      )
      const storeLayer = Layer.succeed(McpServer.SessionStore, sharedStore)
      const replicaA = yield* makeWebHandler(storeLayer)
      const replicaB = yield* makeWebHandler(storeLayer)

      const initializeResponse = yield* webRequest(replicaA, "POST", {
        body: initializeBody
      })
      const sessionId = getSessionId(initializeResponse)
      const headers = { "Mcp-Session-Id": sessionId }

      const pingOnB = yield* webRequest(replicaB, "POST", {
        headers,
        body: pingBody
      })
      strictEqual(pingOnB.status, 200)

      const deleteOnB = yield* webRequest(replicaB, "DELETE", { headers })
      strictEqual(deleteOnB.status, 200)

      const pingOnAAfterDelete = yield* webRequest(replicaA, "POST", {
        headers,
        body: pingBody
      })
      strictEqual(pingOnAAfterDelete.status, 404)

      const pingOnBAfterDelete = yield* webRequest(replicaB, "POST", {
        headers,
        body: pingBody
      })
      strictEqual(pingOnBAfterDelete.status, 404)
    }))
})
