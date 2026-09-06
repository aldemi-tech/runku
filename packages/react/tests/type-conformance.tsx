import { functionReference, type FileDownloadGrant, type FileUploadGrant, type RunkuClient } from "@runku/client";
import {
  RunkuHydrationBoundary,
  RunkuProvider,
  useAction,
  useDownloadFile,
  useMutation,
  useQuery,
  useUploadFile,
  type RunkuDehydratedState,
} from "../src/index.js";
import { createRunkuReactServer } from "../src/server.js";

const list = functionReference<"query", { readonly board: string }, readonly string[], "user">(
  "tasks.list",
  "query",
  "user",
);
const create = functionReference<"mutation", { readonly title: string }, string, "user">(
  "tasks.create",
  "mutation",
  "user",
);
const exportBoard = functionReference<"action", null, {
  readonly download: {
    readonly path: string;
    readonly token: string;
    readonly expiresAtMicros: string;
    readonly metadata: {
      readonly fileId: `fil_${string}`;
      readonly sizeBytes: string;
      readonly sha256: string;
      readonly contentType: string;
      readonly createdAtMicros: string;
    };
  };
}, "user">(
  "tasks.export",
  "action",
  "user",
);
const serviceAction = functionReference<"action", null, string, "service">(
  "tasks.serviceExport",
  "action",
  "service",
);

declare const client: RunkuClient;
declare const hydration: RunkuDehydratedState;
declare const uploadGrant: FileUploadGrant;

function Probe() {
  const tasks = useQuery(list, { board: "today" });
  const creation = useMutation(create);
  const exporting = useAction(exportBoard);
  const upload = useUploadFile();
  const download = useDownloadFile();
  void creation.mutate({ title: "Ship" });
  void exporting.execute(null);
  void upload(uploadGrant, new Uint8Array([1]));
  if (exporting.data !== undefined) void download(exporting.data.download as FileDownloadGrant);
  tasks.data?.[0] satisfies string | undefined;
  return null;
}

const tree = (
  <RunkuHydrationBoundary state={hydration}>
    <RunkuProvider client={client} identityKey="user_123">
      <Probe />
    </RunkuProvider>
  </RunkuHydrationBoundary>
);

const server = createRunkuReactServer(client, "user_123");
void server.query(list, { board: "today" });
void server.preloadQuery(list, { board: "today" });
void server.mutation(create, { title: "Ship" });
void server.action(exportBoard, null);
void server.action(serviceAction, null);
void server.uploadFile(uploadGrant, new Uint8Array([1]));
server.realtime().close();

// @ts-expect-error mutation references cannot be passed to useQuery
void useQuery(create, { title: "wrong" });
// @ts-expect-error service-authenticated Functions are server-only
void useAction(serviceAction);
// @ts-expect-error query arguments are inferred from the reference
void server.query(list, { title: "wrong" });

void tree;
