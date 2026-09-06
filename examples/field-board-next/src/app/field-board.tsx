"use client";

import { fileDownloadGrant, fileUploadGrant, type DocumentId } from "@runku/client";
import { useAction, useDownloadFile, useMutation, useQuery, useUploadFile } from "@runku/react";
import { useState, type FormEvent } from "react";
import { api } from "../../runku/_generated/api.js";
import {
  createTaskOnServer,
  exportBoardOnServer,
  toggleTaskOnServer,
  uploadAttachmentOnServer,
} from "@/lib/server-actions";

export function FieldBoard() {
  const tasks = useQuery(api.tasks.list, null);
  const createTask = useMutation(api.tasks.create);
  const toggleTask = useMutation(api.tasks.toggle);
  const attachTask = useMutation(api.tasks.attach);
  const beginUpload = useAction(api.files.beginUpload);
  const beginDownload = useAction(api.files.beginDownload);
  const exportBoard = useAction(api.files.exportBoard);
  const uploadFile = useUploadFile();
  const downloadFile = useDownloadFile();
  const [title, setTitle] = useState("");
  const [mode, setMode] = useState<"client" | "server">("client");
  const [serverExport, setServerExport] = useState("");
  const [lastTransfer, setLastTransfer] = useState("");
  const [error, setError] = useState("");

  async function addTask(event: FormEvent) {
    event.preventDefault();
    setError("");
    try {
      if (mode === "client") await createTask.mutate({ title });
      else await createTaskOnServer(title);
      setTitle("");
    } catch (caught) {
      setError(message(caught));
    }
  }

  async function toggle(taskId: DocumentId<"tasks">) {
    setError("");
    try {
      if (mode === "client") await toggleTask.mutate({ taskId });
      else await toggleTaskOnServer(String(taskId));
    } catch (caught) {
      setError(message(caught));
    }
  }

  async function upload(taskId: Parameters<typeof toggle>[0], file: File) {
    setError("");
    try {
      if (mode === "server") {
        const form = new FormData();
        form.set("file", file);
        await uploadAttachmentOnServer(String(taskId), form);
        return;
      }
      const contentType = file.type || "application/octet-stream";
      const grant = fileUploadGrant(
        (await beginUpload.execute({ sizeBytes: BigInt(file.size), contentType })).value,
      );
      const metadata = await uploadFile(grant, file, { contentType });
      await attachTask.mutate({ taskId, fileId: metadata.fileId });
    } catch (caught) {
      setError(message(caught));
    }
  }

  async function download(fileId: string) {
    try {
      const grant = fileDownloadGrant((await beginDownload.execute({ fileId })).value);
      const response = await downloadFile(grant);
      saveBlob(await response.blob(), `attachment-${fileId}`);
      setLastTransfer(`Downloaded attachment ${fileId}`);
    } catch (caught) {
      setError(message(caught));
    }
  }

  async function exportTasks() {
    setError("");
    try {
      if (mode === "server") {
        setServerExport(await exportBoardOnServer());
        return;
      }
      const grant = fileDownloadGrant((await exportBoard.execute(null)).value);
      const response = await downloadFile(grant);
      saveBlob(await response.blob(), "field-board.txt");
      setLastTransfer("Client export downloaded");
    } catch (caught) {
      setError(message(caught));
    }
  }

  return (
    <main>
      <header>
        <p className="eyebrow">Runku React + Next.js integration lab</p>
        <h1>Field board</h1>
        <p>SSR preload becomes a live Query; every write can run in the browser or a Server Action.</p>
      </header>

      <section className="toolbar">
        <span>Execution path</span>
        <button className={mode === "client" ? "active" : ""} onClick={() => setMode("client")}>Browser SDK</button>
        <button className={mode === "server" ? "active" : ""} onClick={() => setMode("server")}>Next.js server</button>
        <span className={`status ${tasks.isStale ? "stale" : "live"}`}>
          {tasks.isStale ? "hydrated · connecting" : "realtime · live"}
        </span>
      </section>

      <form className="composer" onSubmit={addTask}>
        <input value={title} onChange={(event) => setTitle(event.target.value)} placeholder="Add a field task" required />
        <button disabled={createTask.status === "pending"}>Add</button>
      </form>

      {error === "" ? null : <p className="error">{error}</p>}
      {lastTransfer === "" ? null : <p className="success">{lastTransfer}</p>}
      {tasks.status === "pending" ? <p>Loading board…</p> : null}
      <ul className="tasks">
        {tasks.data?.map(({ taskId, task }) => (
          <li key={String(taskId)}>
            <button className={`check ${task.done ? "done" : ""}`} onClick={() => void toggle(taskId)}>{task.done ? "✓" : ""}</button>
            <span className={task.done ? "done-title" : ""}>{task.title}</span>
            <label className="file-button">
              Attach
              <input type="file" onChange={(event) => {
                const file = event.target.files?.[0];
                if (file !== undefined) void upload(taskId, file);
              }} />
            </label>
            {task.attachmentFileId === undefined ? null : (
              <button className="link" onClick={() => void download(task.attachmentFileId as string)}>Download</button>
            )}
          </li>
        ))}
      </ul>

      <section className="export">
        <button onClick={() => void exportTasks()}>Export board through {mode}</button>
        {serverExport === "" ? null : <pre>{serverExport}</pre>}
      </section>
    </main>
  );
}

function saveBlob(blob: Blob, name: string) {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = name;
  anchor.click();
  URL.revokeObjectURL(url);
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : "Unexpected failure";
}
