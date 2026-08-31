"use client"

import { FormEvent, useEffect, useMemo, useRef, useState } from "react"
import { RunkuTimestamp, documentId } from "@runku/client"
import type { RunkuFunctionResult } from "../../runku/_generated/api"

import { authClient } from "@/lib/auth-client"
import { bootstrapProfile, runku } from "@/lib/runku"

type Room = RunkuFunctionResult<"rooms.get">
type RoomDirectory = RunkuFunctionResult<"rooms.list">
type RoomSummary = RoomDirectory[number]

type AuthMode = "sign-in" | "sign-up"

function publicError(error: unknown): string {
  if (error instanceof TypeError) {
    return "The submitted data has an invalid format."
  }
  if (error !== null && typeof error === "object" && "code" in error) {
    return `Runku rejected the operation (${String(error.code)}).`
  }
  return "The operation could not be completed. Check that Next.js and Runku are available."
}

function isTransientRealtimeError(error: unknown): boolean {
  return error !== null
    && typeof error === "object"
    && "code" in error
    && error.code === "SDK_REALTIME_DISCONNECTED"
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, milliseconds))
}

function AuthPanel() {
  const [mode, setMode] = useState<AuthMode>("sign-up")
  const [name, setName] = useState("")
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [pending, setPending] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setPending(true)
    setError(null)
    const result =
      mode === "sign-up"
        ? await authClient.signUp.email({ name: name.trim(), email: email.trim(), password })
        : await authClient.signIn.email({ email: email.trim(), password })
    if (result.error !== null) setError(result.error.message ?? "Authentication failed.")
    setPending(false)
  }

  return (
    <main className="auth-shell">
      <section className="auth-card" aria-labelledby="auth-title">
        <div className="brand-mark" aria-hidden="true">R</div>
        <p className="eyebrow">Runku · end-to-end example</p>
        <h1 id="auth-title">A real-time conversation, from end to end.</h1>
        <p className="lede">
          Better Auth establishes identity. Runku authorizes, persists, and synchronizes every room.
        </p>
        <div className="mode-switch" role="group" aria-label="Authentication mode">
          <button className={mode === "sign-up" ? "active" : ""} onClick={() => setMode("sign-up")} type="button">
            Create account
          </button>
          <button className={mode === "sign-in" ? "active" : ""} onClick={() => setMode("sign-in")} type="button">
            Sign in
          </button>
        </div>
        <form onSubmit={submit} className="stack">
          {mode === "sign-up" && (
            <label>
              Display name
              <input data-testid="auth-name" required minLength={1} maxLength={48} value={name} onChange={(event) => setName(event.target.value)} autoComplete="name" />
            </label>
          )}
          <label>
            Email
            <input data-testid="auth-email" required type="email" value={email} onChange={(event) => setEmail(event.target.value)} autoComplete="email" />
          </label>
          <label>
            Password
            <input data-testid="auth-password" required minLength={10} maxLength={128} type="password" value={password} onChange={(event) => setPassword(event.target.value)} autoComplete={mode === "sign-up" ? "new-password" : "current-password"} />
          </label>
          {error !== null && <p className="error" data-testid="app-error" role="alert">{error}</p>}
          <button data-testid="auth-submit" className="primary" disabled={pending} type="submit">
            {pending ? "Working…" : mode === "sign-up" ? "Create account" : "Sign in"}
          </button>
        </form>
      </section>
    </main>
  )
}

function Chat({ displayName, email }: Readonly<{ displayName: string; email: string }>) {
  const [profileReady, setProfileReady] = useState(false)
  const [roomId, setRoomId] = useState("")
  const [roomName, setRoomName] = useState("")
  const [joinId, setJoinId] = useState("")
  const [room, setRoom] = useState<Room | null>(null)
  const [rooms, setRooms] = useState<RoomDirectory>([])
  const [message, setMessage] = useState("")
  const [pending, setPending] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const messagesEnd = useRef<HTMLDivElement>(null)

  useEffect(() => {
    let active = true
    void (async () => {
      let lastFailure: unknown
      for (let attempt = 0; attempt < 6 && active; attempt += 1) {
        try {
          await bootstrapProfile(displayName)
          if (active) {
            setError(null)
            setProfileReady(true)
          }
          return
        } catch (cause) {
          lastFailure = cause
          if (attempt < 5) await delay(Math.min(250 * 2 ** attempt, 2_000))
        }
      }
      if (active) setError(publicError(lastFailure))
    })()
    return () => { active = false }
  }, [displayName])

  useEffect(() => {
    if (!profileReady) return
    const realtime = runku.realtime()
    const subscription = realtime.subscribe("rooms.list", null, {
      onValue: ({ value }) => {
        setError(null)
        setRooms(value)
      },
      onError: (cause) => {
        if (!isTransientRealtimeError(cause)) setError(publicError(cause))
      },
    })
    void subscription.ready.catch((cause: unknown) => setError(publicError(cause)))
    return () => {
      void subscription.unsubscribe()
      realtime.close()
    }
  }, [profileReady])

  useEffect(() => {
    if (!profileReady || roomId === "") return
    const realtime = runku.realtime()
    const subscription = realtime.subscribe("rooms.get", { roomId: documentId("rooms", roomId) }, {
      onValue: ({ value }) => {
        setError(null)
        setRoom(value)
      },
      onError: (cause) => {
        if (!isTransientRealtimeError(cause)) setError(publicError(cause))
      },
    })
    void subscription.ready.catch((cause: unknown) => setError(publicError(cause)))
    return () => {
      void subscription.unsubscribe()
      realtime.close()
    }
  }, [profileReady, roomId])

  useEffect(() => {
    messagesEnd.current?.scrollIntoView({ behavior: "smooth" })
  }, [room?.messages.length])

  const status = useMemo(() => {
    if (!profileReady) return "Verifying identity…"
    if (room === null) return "No active room"
    return `${room.members.length} participant${room.members.length === 1 ? "" : "s"} · realtime active`
  }, [profileReady, room])

  async function createRoom(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setPending(true)
    setError(null)
    try {
      const result = await runku.mutation("rooms.create", {
        name: roomName.trim(),
      })
      setRoom(result.value.room)
      setRoomId(result.value.roomId.value)
      setJoinId(result.value.roomId.value)
    } catch (cause) {
      setError(publicError(cause))
    } finally {
      setPending(false)
    }
  }

  async function joinRoom(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    await openRoom(joinId)
  }

  async function openListedRoom(summary: RoomSummary) {
    await openRoom(summary.roomId.value)
  }

  async function openRoom(nextRoomId: string) {
    setPending(true)
    setError(null)
    try {
      const canonical = documentId("rooms", nextRoomId.trim())
      const result = await runku.mutation("rooms.join", { roomId: canonical })
      setRoom(result.value)
      setRoomId(canonical.value)
      setJoinId(canonical.value)
    } catch (cause) {
      setError(publicError(cause))
    } finally {
      setPending(false)
    }
  }

  async function sendMessage(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const body = message.trim()
    if (body === "" || roomId === "") return
    setMessage("")
    setError(null)
    try {
      await runku.mutation("messages.send", {
        roomId: documentId("rooms", roomId),
        messageId: crypto.randomUUID(),
        body,
        clientSentAt: new RunkuTimestamp(BigInt(Date.now()) * 1000n),
      })
    } catch (cause) {
      setMessage(body)
      setError(publicError(cause))
    }
  }

  return (
    <main className="chat-shell">
      <aside className="sidebar">
        <div>
          <p className="eyebrow">Runku Chat</p>
          <h1>Honest conversations over real infrastructure.</h1>
        </div>
        <div className="identity-card">
          <strong>{displayName}</strong>
          <span>{email}</span>
          <button type="button" onClick={() => void authClient.signOut()}>Sign out</button>
        </div>
        <form className="compact-form" onSubmit={createRoom}>
          <label>
            New room
            <input data-testid="room-name" required maxLength={80} value={roomName} onChange={(event) => setRoomName(event.target.value)} placeholder="Platform team" />
          </label>
          <button data-testid="create-room" className="primary" disabled={!profileReady || pending} type="submit">Create room</button>
        </form>
        <section className="room-directory" data-testid="room-directory" aria-labelledby="room-directory-title">
          <div className="room-directory-heading">
            <h2 id="room-directory-title">Available rooms</h2>
            <span>{rooms.length}</span>
          </div>
          {rooms.length === 0 ? (
            <p className="room-directory-empty">No rooms yet. Create the first one.</p>
          ) : (
            <ul>
              {rooms.map((summary) => (
                <li data-testid="room-directory-item" key={summary.roomId.value}>
                  <div>
                    <strong>{summary.name}</strong>
                    <span>{Number(summary.memberCount)} participant{summary.memberCount === 1n ? "" : "s"}</span>
                  </div>
                  <button
                    aria-label={`${summary.joined ? "Open" : "Join"} ${summary.name}`}
                    className={summary.roomId.value === roomId ? "active" : ""}
                    data-room-id={summary.roomId.value}
                    disabled={!profileReady || pending}
                    onClick={() => void openListedRoom(summary)}
                    type="button"
                  >
                    {summary.roomId.value === roomId ? "Active" : summary.joined ? "Open" : "Join"}
                  </button>
                </li>
              ))}
            </ul>
          )}
        </section>
        <form className="compact-form" onSubmit={joinRoom}>
          <label>
            Join by ID
            <input data-testid="join-room-id" required value={joinId} onChange={(event) => setJoinId(event.target.value)} placeholder="doc_…" />
          </label>
          <button data-testid="join-room" disabled={!profileReady || pending} type="submit">Join</button>
        </form>
      </aside>
      <section className="conversation" aria-label="Active conversation">
        <header className="conversation-header">
          <div>
            <h2 data-testid="active-room-name">{room?.name ?? "Choose a room"}</h2>
            <p>{status}</p>
          </div>
          {roomId !== "" && <code data-testid="active-room-id">{roomId}</code>}
        </header>
        {error !== null && <p className="error banner" data-testid="app-error" role="alert">{error}</p>}
        <div className="messages" aria-live="polite" aria-relevant="additions">
          {room === null ? (
            <div className="empty-state">
              <span aria-hidden="true">↗</span>
              <h3>Create a room or choose an available one.</h3>
              <p>Two independent sessions receive the same state as soon as Runku commits the change.</p>
            </div>
          ) : room.messages.length === 0 ? (
            <div className="empty-state"><h3>No messages yet.</h3><p>Write the first one.</p></div>
          ) : (
            room.messages.map((item) => (
              <article className="message" data-testid="message" key={item.id}>
                <div><strong>{item.senderName}</strong><time>{new Date(Number(item.clientSentAt.micros / 1000n)).toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" })}</time></div>
                <p>{item.body}</p>
              </article>
            ))
          )}
          <div ref={messagesEnd} />
        </div>
        <form className="composer" onSubmit={sendMessage}>
          <label className="sr-only" htmlFor="message-body">Message</label>
          <textarea id="message-body" data-testid="message-body" disabled={room === null} required maxLength={1000} value={message} onChange={(event) => setMessage(event.target.value)} placeholder={room === null ? "Join a room to start chatting" : "Write a message…"} rows={2} />
          <button data-testid="send-message" className="primary" disabled={room === null || message.trim() === ""} type="submit">Send</button>
        </form>
      </section>
    </main>
  )
}

export default function Home() {
  const session = authClient.useSession()
  if (session.isPending) return <main className="loading">Loading session…</main>
  if (session.data === null) return <AuthPanel />
  return <Chat displayName={session.data.user.name} email={session.data.user.email} />
}
