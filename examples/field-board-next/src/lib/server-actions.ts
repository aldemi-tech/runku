"use server";

import { documentId, fileDownloadGrant, fileUploadGrant } from "@runku/client";
import { serverApi } from "../../runku/_generated/server.js";
import { createServerRunku } from "./runku-server";

export async function createTaskOnServer(title: string) {
  await createServerRunku().mutation(serverApi.tasks.create, { title });
}

export async function toggleTaskOnServer(taskIdValue: string) {
  await createServerRunku().mutation(serverApi.tasks.toggle, {
    taskId: documentId("tasks", taskIdValue),
  });
}

export async function exportBoardOnServer(): Promise<string> {
  const runku = createServerRunku();
  const grant = fileDownloadGrant((await runku.action(serverApi.files.exportBoard, null)).value);
  const response = await runku.downloadFile(grant);
  return response.text();
}

export async function uploadAttachmentOnServer(taskIdValue: string, formData: FormData) {
  const file = formData.get("file");
  if (!(file instanceof File) || file.size === 0) throw new Error("file required");
  const runku = createServerRunku();
  const grant = fileUploadGrant(
    (await runku.action(serverApi.files.beginUpload, {
      sizeBytes: BigInt(file.size),
      contentType: file.type || "application/octet-stream",
    })).value,
  );
  const metadata = await runku.uploadFile(grant, file, { contentType: file.type || "application/octet-stream" });
  await runku.mutation(serverApi.tasks.attach, {
    taskId: documentId("tasks", taskIdValue),
    fileId: metadata.fileId,
  });
}
